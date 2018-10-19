defmodule EventStore.Streams.AllStreamTest do
  use EventStore.StorageCase

  alias EventStore.EventFactory
  alias EventStore.Streams.Stream
  alias EventStore.Subscriptions.Subscription

  @all_stream "$all"
  @subscription_name "test_subscription"

  describe "read stream forward" do
    setup [:append_events_to_streams]

    test "should fetch events from all streams", %{conn: conn} do
      {:ok, read_events} = Stream.read_stream_forward(conn, @all_stream, 0, 1_000)

      assert length(read_events) == 6
    end
  end

  describe "stream forward" do
    setup [:append_events_to_streams]

    test "should stream events from all streams using single event batch size", %{conn: conn} do
      read_events = Stream.stream_forward(conn, @all_stream, 0, 1) |> Enum.to_list()

      assert length(read_events) == 6
    end

    test "should stream events from all streams using two event batch size", %{conn: conn} do
      read_events = Stream.stream_forward(conn, @all_stream, 0, 2) |> Enum.to_list()

      assert length(read_events) == 6
    end

    test "should stream events from all streams uisng large batch size", %{conn: conn} do
      read_events = Stream.stream_forward(conn, @all_stream, 0, 1_000) |> Enum.to_list()

      assert length(read_events) == 6
    end
  end

  describe "subscribe to all streams" do
    setup [:append_events_to_streams]

    test "from origin should receive all events" do
      {:ok, subscription} =
        EventStore.subscribe_to_stream(
          @all_stream,
          @subscription_name,
          self(),
          start_from: :origin,
          buffer_size: 3
        )

      assert_receive {:events, received_events1}
      Subscription.ack(subscription, received_events1)

      assert_receive {:events, received_events2}
      Subscription.ack(subscription, received_events2)

      assert length(received_events1 ++ received_events2) == 6

      refute_receive {:events, _events}
    end

    test "from current should receive only new events", context do
      %{conn: conn, stream1_uuid: stream1_uuid} = context

      {:ok, _subscription} =
        EventStore.subscribe_to_stream(
          @all_stream,
          @subscription_name,
          self(),
          start_from: :current,
          buffer_size: 2
        )

      refute_receive {:events, _received_events}

      events = EventFactory.create_events(1, 4)
      :ok = Stream.append_to_stream(conn, stream1_uuid, 3, events)

      assert_receive {:events, received_events}
      assert length(received_events) == 1
    end

    test "from given event id should receive only later events" do
      {:ok, subscription} =
        EventStore.subscribe_to_stream(
          @all_stream,
          @subscription_name,
          self(),
          start_from: 2,
          buffer_size: 2
        )

      assert_receive {:events, received_events1}
      Subscription.ack(subscription, received_events1)

      assert_receive {:events, received_events2}
      Subscription.ack(subscription, received_events2)

      assert length(received_events1 ++ received_events2) == 4

      refute_receive {:events, _events}
    end
  end

  defp append_events_to_streams(%{conn: conn}) do
    {stream1_uuid, stream1_events} = append_events_to_stream(conn)
    {stream2_uuid, stream2_events} = append_events_to_stream(conn)

    [
      stream1_uuid: stream1_uuid,
      stream1_events: stream1_events,
      stream2_uuid: stream2_uuid,
      stream2_events: stream2_events
    ]
  end

  defp append_events_to_stream(conn) do
    stream_uuid = UUID.uuid4()
    events = EventFactory.create_events(3)

    :ok = Stream.append_to_stream(conn, stream_uuid, 0, events)

    {stream_uuid, events}
  end
end
