defmodule EventStore.Subscriptions.SingleSubscriptionFsmTest do
  use EventStore.Subscriptions.StreamSubscriptionTestCase, stream_uuid: UUID.uuid4()

  alias EventStore.EventFactory

  setup [:append_events_to_another_stream]

  defp create_recorded_events(%{stream_uuid: stream_uuid}, count, initial_event_number \\ 1) do
    EventFactory.create_recorded_events(count, stream_uuid, initial_event_number)
  end

  defp append_events_to_stream(%{conn: conn, stream_uuid: stream_uuid}) do
    recorded_events = EventFactory.create_recorded_events(3, stream_uuid)

    {:ok, stream_id} = CreateStream.execute(conn, stream_uuid)

    :ok = Appender.append(conn, stream_id, recorded_events)

    [
      recorded_events: recorded_events
    ]
  end

  # Append events to another stream so that for single stream subscription tests
  # the stream version in the `$all` stream is not identical to the stream
  # version of events appended to the test subject stream.
  defp append_events_to_another_stream(_context) do
    stream_uuid = UUID.uuid4()
    events = EventFactory.create_events(3)

    :ok = EventStore.append_to_stream(stream_uuid, 0, events)
  end
end
