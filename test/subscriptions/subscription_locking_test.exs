defmodule EventStore.Subscriptions.SubscriptionLockingTest do
  use EventStore.StorageCase

  alias EventStore.{Config, EventFactory, ProcessHelper}
  alias EventStore.Subscriptions.Subscription

  @conn EventStore.Postgrex

  setup do
    subscription_name = UUID.uuid4()

    {:ok, %{subscription_name: subscription_name}}
  end

  describe "subscription lock lost" do
    setup [:create_subscription]

    test "should resend in-flight events", %{subscription: subscription} do
      assert_receive {:subscribed, ^subscription}

      append_events_to_stream(3)

      assert_receive_events([1, 2, 3])

      :ok = Subscription.disconnect(subscription)

      # Acknowledgements should be ignored while subscription is disconnected
      :ok = Subscription.ack(subscription, 1)
      :ok = Subscription.ack(subscription, 2)
      :ok = Subscription.ack(subscription, 3)

      :ok = Subscription.reconnect(subscription, 0)

      # Should receive already sent, but not successfully ack'd events
      assert_receive_events([1, 2, 3])
    end

    test "should not send ack'd events before disconnect", %{subscription: subscription} do
      assert_receive {:subscribed, ^subscription}

      append_events_to_stream(3)

      assert_receive_events([1, 2, 3])

      # Acknowledgement sent before disconnect should be persisted
      :ok = Subscription.ack(subscription, 1)

      refute_receive {:events, _received_events}

      :ok = Subscription.disconnect(subscription)

      # Acknowledgements sent after subscription disconnect should be ignored
      :ok = Subscription.ack(subscription, 2)
      :ok = Subscription.ack(subscription, 3)

      :ok = Subscription.reconnect(subscription, 0)

      # Should receive already sent, but not successfully ack'd events
      assert_receive_events([2, 3])
    end

    test "should not send events ack'd by another subscription during disconnect", %{
      subscription: subscription,
      subscription_name: subscription_name
    } do
      assert_receive {:subscribed, ^subscription}

      append_events_to_stream(3)

      assert_receive_events([1, 2, 3])
      refute_receive {:events, _received_events}

      :ok = Subscription.disconnect(subscription)

      :ok =
        EventStore.Storage.Subscription.ack_last_seen_event(
          @conn,
          "$all",
          subscription_name,
          2,
          pool: DBConnection.Poolboy
        )

      :ok = Subscription.reconnect(subscription, 0)

      # Should only receive events not yet ack'd
      assert_receive_events([3])
    end
  end

  describe "duplicate subscriptions" do
    setup [:lock_subscription, :create_subscription]

    test "should not be subscribed", %{subscription: subscription} do
      refute_receive {:subscribed, ^subscription}
    end

    test "should only allow single active subscription", %{
      conn2: conn2,
      subscription: subscription
    } do
      stream1_uuid = append_events_to_stream(1)

      # Subscriber should not receive events until subscribed
      refute_receive {:events, _received_events}

      # Release lock, allowing subscriber to subscribe
      ProcessHelper.shutdown(conn2)

      # Subscription should now be subscribed
      assert_receive {:subscribed, ^subscription}

      stream2_uuid = append_events_to_stream(2)

      # Subscriber should now start receiving events
      assert_receive {:events, received_events}
      assert length(received_events) == 1

      for event <- received_events do
        assert event.stream_uuid == stream1_uuid
      end

      :ok = Subscription.ack(subscription, received_events)

      assert_receive {:events, received_events}, 5_000
      assert length(received_events) == 2

      Enum.each(received_events, fn event ->
        assert event.stream_uuid == stream2_uuid
      end)

      :ok = Subscription.ack(subscription, received_events)

      refute_receive {:events, _received_events}
    end
  end

  defp lock_subscription(_context) do
    config = Config.parsed() |> Config.sync_connect_postgrex_opts()

    {:ok, conn} = Postgrex.start_link(config)

    EventStore.Storage.Lock.try_acquire_exclusive_lock(conn, 1)

    on_exit(fn ->
      ProcessHelper.shutdown(conn)
    end)

    [conn2: conn]
  end

  defp create_subscription(%{subscription_name: subscription_name}) do
    {:ok, subscription} =
      EventStore.Subscriptions.Subscription.start_link(
        EventStore.Postgrex,
        "$all",
        subscription_name,
        buffer_size: 3,
        start_from: 0
      )

    {:ok, ^subscription} =
      EventStore.Subscriptions.Subscription.connect(subscription, self(), buffer_size: 3)

    [subscription: subscription]
  end

  defp append_events_to_stream(count) do
    stream_uuid = UUID.uuid4()
    stream_events = EventFactory.create_events(count)

    :ok = EventStore.append_to_stream(stream_uuid, 0, stream_events)

    stream_uuid
  end

  defp assert_receive_events(expected_event_numbers) do
    assert_receive {:events, received_events}
    assert pluck(received_events, :event_number) == expected_event_numbers
  end

  defp pluck(enumerable, field) do
    Enum.map(enumerable, &Map.get(&1, field))
  end
end
