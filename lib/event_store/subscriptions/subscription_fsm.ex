defmodule EventStore.Subscriptions.SubscriptionFsm do
  @moduledoc false

  alias EventStore.{AdvisoryLocks, RecordedEvent, Storage}
  alias EventStore.Subscriptions.{SubscriptionState, Subscription, Subscriber}

  use Fsm, initial_state: :initial, initial_data: %SubscriptionState{}

  require Logger

  @max_buffer_size 1_000

  # The main flow between states in this finite state machine is:
  #
  #   initial -> subscribe_to_events -> request_catch_up -> catching_up -> subscribed
  #

  defstate initial do
    defevent subscribe(conn, stream_uuid, subscription_name, subscriber, opts) do
      data =
        %SubscriptionState{
          conn: conn,
          stream_uuid: stream_uuid,
          subscription_name: subscription_name,
          start_from: opts[:start_from],
          mapper: opts[:mapper],
          selector: opts[:selector],
          partition_by: opts[:partition_by],
          max_size: opts[:max_size] || @max_buffer_size
        }
        |> monitor_subscriber(subscriber, opts)

      with {:ok, subscription} <- create_subscription(data),
           :ok <- try_acquire_exclusive_lock(subscription) do
        %Storage.Subscription{subscription_id: subscription_id, last_seen: last_seen} =
          subscription

        last_seen = last_seen || 0

        data = %SubscriptionState{
          data
          | subscription_id: subscription_id,
            last_received: last_seen,
            last_sent: last_seen,
            last_ack: last_seen
        }

        next_state(:subscribe_to_events, data)
      else
        _ ->
          # Failed to subscribe to stream, retry after delay
          next_state(:initial, data)
      end
    end

    # ignore ack's before subscribed
    defevent ack(_ack, _subscriber), data: %SubscriptionState{} = data do
      next_state(:initial, data)
    end
  end

  defstate subscribe_to_events do
    defevent subscribed, data: %SubscriptionState{} = data do
      next_state(:request_catch_up, data)
    end

    defevent ack(ack, subscriber), data: %SubscriptionState{} = data do
      data =
        data
        |> ack_events(ack, subscriber)
        |> notify_subscribers()

      next_state(:subscribe_to_events, data)
    end
  end

  defstate request_catch_up do
    defevent catch_up, data: %SubscriptionState{} = data do
      catch_up_from_stream(data)
    end

    defevent ack(ack, subscriber), data: %SubscriptionState{} = data do
      data
      |> ack_events(ack, subscriber)
      |> notify_subscribers()
      |> catch_up_from_stream()
    end
  end

  defstate catching_up do
    defevent ack(ack, subscriber), data: %SubscriptionState{} = data do
      data
      |> ack_events(ack, subscriber)
      |> notify_subscribers()
      |> catch_up_from_stream()
    end
  end

  defstate subscribed do
    # Notify events when subscribed
    defevent notify_events(events), data: %SubscriptionState{last_sent: last_sent} = data do
      expected_event = last_sent + 1

      case first_event_number(events) do
        past when past < expected_event ->
          Logger.debug(fn -> describe(data) <> " received past event(s), ignoring" end)

          # Ignore already seen events
          next_state(:subscribed, data)

        future when future > expected_event ->
          Logger.debug(fn ->
            describe(data) <> " received unexpected event(s), requesting catch up"
          end)

          # Missed events, go back and catch-up with unseen
          next_state(:request_catch_up, data)

        _ ->
          Logger.debug(fn ->
            describe(data) <> " is enqueueing #{length(events)} event(s)"
          end)

          # Subscriber is up-to-date, so enqueue events to send
          data = data |> enqueue_events(events) |> notify_subscribers()

          if over_capacity?(data) do
            # Too many pending events, must wait for these to be processed.
            next_state(:max_capacity, data)
          else
            # Remain subscribed, waiting for subscriber to ack already sent events.
            next_state(:subscribed, data)
          end
      end
    end

    defevent ack(ack, subscriber), data: %SubscriptionState{} = data do
      data =
        data
        |> ack_events(ack, subscriber)
        |> notify_subscribers()

      next_state(:subscribed, data)
    end

    defevent catch_up, data: %SubscriptionState{} = data do
      next_state(:request_catch_up, data)
    end
  end

  defstate max_capacity do
    defevent ack(ack, subscriber), data: %SubscriptionState{} = data do
      data =
        data
        |> ack_events(ack, subscriber)
        |> notify_subscribers()

      if empty_queue?(data) do
        # No further pending events so catch up with any unseen.
        next_state(:request_catch_up, data)
      else
        # Pending events remain, wait until subscriber ack's.
        next_state(:max_capacity, data)
      end
    end
  end

  defstate disconnected do
    # reconnect to subscription after lock reacquired
    defevent reconnect, data: %SubscriptionState{} = data do
      with {:ok, subscription} <- create_subscription(data) do
        %Storage.Subscription{
          subscription_id: subscription_id,
          last_seen: last_seen
        } = subscription

        last_ack = last_seen || 0

        data = %SubscriptionState{
          data
          | subscription_id: subscription_id,
            last_sent: last_ack,
            last_ack: last_ack
        }

        next_state(:request_catch_up, data)
      else
        _ ->
          next_state(:disconnected, data)
      end
    end
  end

  defstate unsubscribed do
    defevent ack(_ack, _subscriber), data: %SubscriptionState{} = data do
      next_state(:unsubscribed, data)
    end

    defevent unsubscribe(_subscriber), data: %SubscriptionState{} = data do
      next_state(:unsubscribed, data)
    end
  end

  # Catch-all event handlers

  defevent connect_subscriber(subscriber, opts),
    data: %SubscriptionState{} = data,
    state: state do
    data = data |> monitor_subscriber(subscriber, opts) |> notify_subscribers()

    next_state(state, data)
  end

  defevent subscribe(_conn, _stream_uuid, _subscription_name, _subscriber, _opts),
    data: %SubscriptionState{} = data,
    state: state do
    next_state(state, data)
  end

  defevent subscribed, data: %SubscriptionState{} = data, state: state do
    next_state(state, data)
  end

  # Ignore notify events unless subscribed
  defevent notify_events(events), data: %SubscriptionState{} = data, state: state do
    next_state(state, track_last_received(events, data))
  end

  defevent catch_up, data: %SubscriptionState{} = data, state: state do
    next_state(state, data)
  end

  defevent disconnect, data: %SubscriptionState{} = data do
    next_state(:disconnected, data)
  end

  defevent unsubscribe(pid), data: %SubscriptionState{} = data, state: state do
    data = data |> remove_subscriber(pid) |> notify_subscribers()

    case has_subscribers?(data) do
      true ->
        next_state(state, data)

      false ->
        next_state(:unsubscribed, data)
    end
  end

  defp create_subscription(%SubscriptionState{} = data) do
    %SubscriptionState{
      conn: conn,
      start_from: start_from,
      stream_uuid: stream_uuid,
      subscription_name: subscription_name
    } = data

    Storage.Subscription.subscribe_to_stream(
      conn,
      stream_uuid,
      subscription_name,
      start_from,
      pool: DBConnection.Poolboy
    )
  end

  defp try_acquire_exclusive_lock(%Storage.Subscription{subscription_id: subscription_id}) do
    subscription = self()

    AdvisoryLocks.try_advisory_lock(
      subscription_id,
      lock_released: fn ->
        # Disconnect subscription when lock is released (e.g. database connection down).
        Subscription.disconnect(subscription)
      end,
      lock_reacquired: fn ->
        # Reconnect subscription when lock reacquired.
        Subscription.reconnect(subscription)
      end
    )
  end

  defp monitor_subscriber(%SubscriptionState{subscribers: subscribers} = data, pid, opts)
       when is_pid(pid) do
    subscriber = %Subscriber{
      pid: pid,
      ref: Process.monitor(pid),
      buffer_size: Keyword.get(opts, :buffer_size, 1)
    }

    %SubscriptionState{data | subscribers: Map.put(subscribers, pid, subscriber)}
  end

  defp remove_subscriber(%SubscriptionState{subscribers: subscribers} = data, pid)
       when is_pid(pid) do
    case Map.get(subscribers, pid) do
      nil ->
        data

      %Subscriber{in_flight: in_flight} ->
        # Prepend in-flight events for the down subscriber to the pending
        # event queue so they will be resent to another available subscriber.
        data =
          in_flight
          |> Enum.sort_by(fn %RecordedEvent{event_number: event_number} -> -event_number end)
          |> Enum.reduce(data, fn event, data ->
            enqueue_event(data, event, &:queue.in_r/2)
          end)

        %SubscriptionState{data | subscribers: Map.delete(subscribers, pid)}
    end
  end

  defp has_subscribers?(%SubscriptionState{subscribers: subscribers}), do: subscribers != %{}

  defp track_last_received(events, %SubscriptionState{} = data) do
    %SubscriptionState{data | last_received: last_event_number(events)}
  end

  defp first_event_number([%RecordedEvent{event_number: event_number} | _]), do: event_number

  defp last_event_number([%RecordedEvent{event_number: event_number}]), do: event_number
  defp last_event_number([_event | events]), do: last_event_number(events)

  def catch_up_from_stream(%SubscriptionState{queue_size: 0} = data) do
    %SubscriptionState{last_sent: last_sent, last_received: last_received} = data

    case read_stream_forward(data) do
      {:ok, []} ->
        if last_sent == last_received do
          # Subscriber is up-to-date with latest published events
          next_state(:subscribed, data)
        else
          # Need to catch-up with events published while catching up
          next_state(:request_catch_up, data)
        end

      {:ok, events} ->
        data = data |> enqueue_events(events) |> notify_subscribers()

        if empty_queue?(data) do
          # Request next batch of events
          next_state(:request_catch_up, data)
        else
          # Wait until subscribers have ack'd in-flight events
          next_state(:catching_up, data)
        end

      {:error, :stream_not_found} ->
        next_state(:subscribed, data)
    end
  end

  def catch_up_from_stream(%SubscriptionState{} = data) do
    next_state(:catching_up, data)
  end

  defp read_stream_forward(%SubscriptionState{} = data) do
    %SubscriptionState{
      conn: conn,
      stream_uuid: stream_uuid,
      last_sent: last_sent
    } = data

    EventStore.Streams.Stream.read_stream_forward(
      conn,
      stream_uuid,
      last_sent + 1,
      @max_buffer_size,
      pool: DBConnection.Poolboy
    )
  end

  defp enqueue_events(%SubscriptionState{} = data, []), do: data

  defp enqueue_events(%SubscriptionState{} = data, [event | events]) do
    %SubscriptionState{
      processed_event_ids: processed_event_ids,
      last_sent: last_sent,
      last_received: last_received
    } = data

    %RecordedEvent{event_number: event_number} = event

    data =
      case selected?(event, data) do
        true ->
          # Unfiltered event, enqueue to send to a subscriber
          enqueue_event(data, event)

        false ->
          # Filtered event, don't send to subscriber, but track it as processed.
          %SubscriptionState{
            data
            | processed_event_ids: MapSet.put(processed_event_ids, event_number),
              last_sent: max(last_sent, event_number)
          }
      end

    %SubscriptionState{data | last_received: max(last_received, event_number)}
    |> enqueue_events(events)
  end

  defp enqueue_event(%SubscriptionState{} = data, event, enqueue \\ &:queue.in/2) do
    %SubscriptionState{partitions: partitions, queue_size: queue_size} = data

    partition_key = partition_key(data, event)

    partitions =
      partitions
      |> Map.put_new(partition_key, :queue.new())
      |> Map.update!(partition_key, fn pending_events -> enqueue.(event, pending_events) end)

    %SubscriptionState{data | partitions: partitions, queue_size: queue_size + 1}
  end

  def partition_key(%SubscriptionState{partition_by: nil}, %RecordedEvent{}), do: nil

  def partition_key(%SubscriptionState{partition_by: partition_by}, %RecordedEvent{} = event)
      when is_function(partition_by, 1),
      do: partition_by.(event)

  # Attempt to notify subscribers with any pending events. Partitions are
  # selected by peeking at the event number of their queue to ensure earlier
  # events are sent first.
  defp notify_subscribers(%SubscriptionState{partitions: partitions} = data) do
    partitions
    |> Enum.sort_by(fn {_partition_key, pending_events} -> peek_event_number(pending_events) end)
    |> Enum.reduce(data, fn {partition_key, _pending_events}, data ->
      notify_partition_subscriber(data, partition_key)
    end)
    |> checkpoint_last_seen()
  end

  defp peek_event_number(pending_events) do
    case :queue.peek(pending_events) do
      {:value, %RecordedEvent{event_number: event_number}} -> event_number
      _ -> nil
    end
  end

  defp notify_partition_subscriber(data, partition_key, events_to_send \\ []) do
    %SubscriptionState{
      partitions: partitions,
      subscribers: subscribers,
      last_sent: last_sent,
      queue_size: queue_size
    } = data

    with pending_events when not is_nil(pending_events) <- Map.get(partitions, partition_key),
         {{:value, event}, pending_events} <- :queue.out(pending_events),
         {:ok, subscriber} <- next_available_subscriber(data, partition_key) do
      %RecordedEvent{event_number: event_number} = event
      %Subscriber{pid: subscriber_pid} = subscriber

      subscriber = Subscriber.track_in_flight(subscriber, event, partition_key)

      partitions =
        case :queue.is_empty(pending_events) do
          true -> Map.delete(partitions, partition_key)
          false -> Map.put(partitions, partition_key, pending_events)
        end

      %SubscriptionState{
        data
        | partitions: partitions,
          last_sent: max(last_sent, event_number),
          subscribers: Map.put(subscribers, subscriber_pid, subscriber),
          queue_size: max(queue_size - 1, 0)
      }
      |> notify_partition_subscriber(partition_key, [{subscriber_pid, event} | events_to_send])
    else
      _ ->
        # No further queued event or available subscriber, send ready events to
        # subscribers then stop notifying.
        send_queued_events(events_to_send, data)
    end
  end

  # Send events to the subscriber
  defp send_queued_events([], data), do: data

  defp send_queued_events(events_to_send, data) do
    events_to_send
    |> Enum.group_by(fn {pid, _event} -> pid end, fn {_pid, event} -> event end)
    |> Enum.each(fn {pid, events} ->
      mapped_events = events |> Enum.reverse() |> map(data)

      send(pid, {:events, mapped_events})
    end)

    data
  end

  # Select the next available subscriber based upon their partition key, buffer
  # size and number of currently in-flight events.
  #
  # Uses a round robin strategy for balancing events between subscribers.
  #
  # Events will be distributed to subscribers based upon their partition key
  # when a `partition_by/1` function is provided. This is used to guarantee
  # ordering of events for each partition (e.g. stream/aggregate identity).
  #
  defp next_available_subscriber(%SubscriptionState{} = data, partition_key) do
    %SubscriptionState{subscribers: subscribers} = data

    partition_subscriber =
      Enum.find(subscribers, fn {_pid, subscriber} ->
        Subscriber.in_partition?(subscriber, partition_key)
      end)

    subscribers =
      case partition_subscriber do
        nil -> subscribers
        subscriber -> [subscriber]
      end

    subscribers
    |> Enum.sort_by(fn {_pid, %Subscriber{last_sent: last_sent}} -> last_sent end)
    |> Enum.find(fn {_pid, subscriber} -> Subscriber.available?(subscriber) end)
    |> case do
      nil -> {:error, :no_available_subscriber}
      {_pid, subscriber} -> {:ok, subscriber}
    end
  end

  defp selected?(event, %SubscriptionState{selector: selector}) when is_function(selector, 1),
    do: selector.(event)

  defp selected?(_event, %SubscriptionState{}), do: true

  defp map(events, %SubscriptionState{mapper: mapper}) when is_function(mapper, 1),
    do: Enum.map(events, mapper)

  defp map(events, _mapper), do: events

  defp ack_events(%SubscriptionState{} = data, ack, subscriber_pid) do
    %SubscriptionState{subscribers: subscribers, processed_event_ids: processed_event_ids} = data

    case subscribers |> Map.get(subscriber_pid) |> Subscriber.acknowledge(ack) do
      {_subscriber, []} ->
        # Not an in-flight event, ignore ack.
        data

      {subscriber, acknowledged_events} ->
        processed_event_ids =
          acknowledged_events
          |> Enum.map(& &1.event_number)
          |> Enum.reduce(processed_event_ids, &MapSet.put(&2, &1))

        %SubscriptionState{
          data
          | subscribers: Map.put(subscribers, subscriber_pid, subscriber),
            processed_event_ids: processed_event_ids
        }
        |> checkpoint_last_seen()
    end
  end

  defp checkpoint_last_seen(%SubscriptionState{} = data, persist \\ false) do
    %SubscriptionState{
      conn: conn,
      stream_uuid: stream_uuid,
      subscription_name: subscription_name,
      processed_event_ids: processed_event_ids,
      last_ack: last_ack
    } = data

    ack = last_ack + 1

    cond do
      MapSet.member?(processed_event_ids, ack) ->
        %SubscriptionState{
          data
          | processed_event_ids: MapSet.delete(processed_event_ids, ack),
            last_ack: ack
        }
        |> checkpoint_last_seen(true)

      persist ->
        Storage.Subscription.ack_last_seen_event(
          conn,
          stream_uuid,
          subscription_name,
          last_ack,
          pool: DBConnection.Poolboy
        )

        data

      true ->
        data
    end
  end

  defp empty_queue?(%SubscriptionState{queue_size: 0}), do: true
  defp empty_queue?(%SubscriptionState{}), do: false

  defp over_capacity?(%SubscriptionState{queue_size: queue_size, max_size: max_size}),
    do: queue_size >= max_size

  defp describe(%SubscriptionState{stream_uuid: stream_uuid, subscription_name: name}),
    do: "Subscription #{inspect(name)}@#{inspect(stream_uuid)}"
end
