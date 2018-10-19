defmodule EventStore.Subscriptions.SubscriptionCatchUpTest do
  use EventStore.StorageCase

  alias EventStore.{EventFactory, RecordedEvent}
  alias EventStore.Subscriptions.Subscription

  describe "catch-up subscription" do
    test "should receive all existing events" do
      subscription_name = UUID.uuid4()

      stream1_uuid = UUID.uuid4()
      stream2_uuid = UUID.uuid4()
      stream3_uuid = UUID.uuid4()
      stream4_uuid = UUID.uuid4()

      append_to_stream(stream1_uuid, 10)
      append_to_stream(stream2_uuid, 10)
      append_to_stream(stream3_uuid, 10)
      append_to_stream(stream4_uuid, 10)

      {:ok, subscription} = subscribe_to_all_streams(subscription_name, self(), buffer_size: 10)

      append_to_stream(stream1_uuid, 10, 10)

      receive_and_ack(subscription, stream1_uuid, 1)
      receive_and_ack(subscription, stream2_uuid, 11)
      receive_and_ack(subscription, stream3_uuid, 21)
      receive_and_ack(subscription, stream4_uuid, 31)

      append_to_stream(stream1_uuid, 10, 20)

      receive_and_ack(subscription, stream1_uuid, 41)
      receive_and_ack(subscription, stream1_uuid, 51)

      refute_receive {:events, _events}

      append_to_stream(stream1_uuid, 10, 30)
      receive_and_ack(subscription, stream1_uuid, 61)

      refute_receive {:events, _events}
    end
  end

  defp append_to_stream(stream_uuid, event_count, expected_version \\ 0) do
    events = EventFactory.create_events(event_count)

    :ok = EventStore.append_to_stream(stream_uuid, expected_version, events)
  end

  # Subscribe to all streams and wait for the subscription to be subscribed.
  defp subscribe_to_all_streams(subscription_name, subscriber, opts) do
    {:ok, subscription} = EventStore.subscribe_to_all_streams(subscription_name, subscriber, opts)

    assert_receive {:subscribed, ^subscription}

    {:ok, subscription}
  end

  def receive_and_ack(subscription, expected_stream_uuid, expected_intial_event_number) do
    assert_receive {:events, received_events}
    assert length(received_events) == 10

    received_events
    |> Enum.with_index(expected_intial_event_number)
    |> Enum.each(fn {event, expected_event_number} ->
      %RecordedEvent{event_number: event_number} = event

      assert event_number == expected_event_number
      assert event.stream_uuid == expected_stream_uuid
    end)

    Subscription.ack(subscription, received_events)
  end
end
