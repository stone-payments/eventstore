defmodule EventStore.Subscriptions.ConcurrentSubscriptionTest do
  use EventStore.StorageCase

  alias EventStore.EventFactory
  alias EventStore.Subscriptions.Subscription

  describe "concurrent subscription" do
    test "should allow multiple subscribers" do
      subscription_name = UUID.uuid4()
      subscriber = self()

      {:ok, subscription} =
        EventStore.subscribe_to_all_streams(subscription_name, subscriber, concurrency: 2)

      {:ok, ^subscription} =
        EventStore.subscribe_to_all_streams(subscription_name, subscriber, concurrency: 2)

      assert_receive {:subscribed, ^subscription}
      refute_receive {:subscribed, ^subscription}
    end

    test "should send events to all subscribers" do
      subscription_name = UUID.uuid4()
      stream_uuid = UUID.uuid4()

      subscriber1 = start_subscriber(:subscriber1)
      subscriber2 = start_subscriber(:subscriber2)
      subscriber3 = start_subscriber(:subscriber3)

      {:ok, subscription} =
        EventStore.subscribe_to_all_streams(subscription_name, subscriber1, concurrency: 3)

      {:ok, ^subscription} =
        EventStore.subscribe_to_all_streams(subscription_name, subscriber2, concurrency: 3)

      {:ok, ^subscription} =
        EventStore.subscribe_to_all_streams(subscription_name, subscriber3, concurrency: 3)

      append_to_stream(stream_uuid, 3)

      assert_receive_events([1], :subscriber1)
      assert_receive_events([2], :subscriber2)
      assert_receive_events([3], :subscriber3)

      refute_receive {:events, _received_events, _subscriber}
    end

    test "should send event to next available subscriber after ack" do
      subscription_name = UUID.uuid4()
      stream_uuid = UUID.uuid4()

      subscriber1 = start_subscriber(:subscriber1)
      subscriber2 = start_subscriber(:subscriber2)

      {:ok, subscription} =
        EventStore.subscribe_to_all_streams(subscription_name, subscriber1, concurrency: 2)

      {:ok, ^subscription} =
        EventStore.subscribe_to_all_streams(subscription_name, subscriber2, concurrency: 2)

      append_to_stream(stream_uuid, 6)

      assert_receive_events([1], :subscriber1)
      assert_receive_events([2], :subscriber2)

      Subscription.ack(subscription, 2, subscriber2)
      assert_receive_events([3], :subscriber2)

      Subscription.ack(subscription, 3, subscriber2)
      assert_receive_events([4], :subscriber2)

      Subscription.ack(subscription, 4, subscriber2)
      assert_receive_events([5], :subscriber2)

      Subscription.ack(subscription, 1, subscriber1)
      assert_receive_events([6], :subscriber1)

      refute_receive {:events, _received_events, _subscriber}
    end

    test "should ack events in order" do
      subscription_name = UUID.uuid4()
      stream_uuid = UUID.uuid4()

      subscriber1 = start_subscriber(:subscriber1)
      subscriber2 = start_subscriber(:subscriber2)
      subscriber3 = start_subscriber(:subscriber3)

      {:ok, subscription} =
        EventStore.subscribe_to_all_streams(subscription_name, subscriber1, concurrency: 3)

      {:ok, ^subscription} =
        EventStore.subscribe_to_all_streams(subscription_name, subscriber2, concurrency: 3)

      {:ok, ^subscription} =
        EventStore.subscribe_to_all_streams(subscription_name, subscriber3, concurrency: 3)

      append_to_stream(stream_uuid, 8)

      assert_receive_events([1], :subscriber1)
      assert_receive_events([2], :subscriber2)
      assert_receive_events([3], :subscriber3)

      Subscription.ack(subscription, 1, subscriber1)
      assert_receive_events([4], :subscriber1)
      assert_last_ack(subscription, 1)

      Subscription.ack(subscription, 2, subscriber2)
      assert_receive_events([5], :subscriber2)
      assert_last_ack(subscription, 2)

      Subscription.ack(subscription, 3, subscriber3)
      assert_receive_events([6], :subscriber3)
      assert_last_ack(subscription, 3)

      # Ack for event number 6 received, but next ack to store is event number 4
      Subscription.ack(subscription, 6, subscriber3)
      assert_receive_events([7], :subscriber3)
      assert_last_ack(subscription, 3)

      Subscription.ack(subscription, 5, subscriber2)
      assert_receive_events([8], :subscriber2)
      assert_last_ack(subscription, 3)

      Subscription.ack(subscription, 4, subscriber1)
      assert_last_ack(subscription, 6)
      refute_receive {:events, _received_events, _subscriber}

      Subscription.ack(subscription, 7, subscriber3)
      assert_last_ack(subscription, 7)
      refute_receive {:events, _received_events, _subscriber}

      Subscription.ack(subscription, 8, subscriber2)
      assert_last_ack(subscription, 8)
      refute_receive {:events, _received_events, _subscriber}
    end
  end

  defp assert_receive_events(expected_event_numbers, expected_subscriber) do
    assert_receive {:events, received_events, ^expected_subscriber}

    actual_event_numbers = Enum.map(received_events, & &1.event_number)
    assert expected_event_numbers == actual_event_numbers
  end

  defp assert_last_ack(subscription, expected_ack) do
    last_seen = Subscription.last_seen(subscription)

    assert last_seen == expected_ack
  end

  defp start_subscriber(name) do
    reply_to = self()

    spawn_link(fn ->
      receive_events = fn loop ->
        receive do
          {:events, events} ->
            send(reply_to, {:events, events, name})

            loop.(loop)
        end
      end

      receive_events.(receive_events)
    end)
  end

  def receive_and_ack(subscription, expected_stream_uuid) do
    assert_receive {:events, received_events}
    assert Enum.all?(received_events, fn event -> event.stream_uuid == expected_stream_uuid end)

    Subscription.ack(subscription, received_events)
  end

  defp append_to_stream(stream_uuid, event_count) do
    events = EventFactory.create_events(event_count)

    :ok = EventStore.append_to_stream(stream_uuid, 0, events)
  end
end
