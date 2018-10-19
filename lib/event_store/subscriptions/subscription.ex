defmodule EventStore.Subscriptions.Subscription do
  @moduledoc false

  # Subscription to a single, or all, event streams.
  #
  # A subscription is persistent so that resuming the subscription will continue
  # from the last acknowledged event. This guarantees at least once delivery of
  # every event appended to storage.

  use GenServer
  require Logger

  alias EventStore.RecordedEvent
  alias EventStore.Subscriptions.{SubscriptionFsm, Subscription, SubscriptionState}

  defstruct [
    :stream_uuid,
    :subscription_name,
    :subscription,
    :retry_interval
  ]

  def start_link(conn, stream_uuid, subscription_name, subscription_opts, opts \\ []) do
    state = %Subscription{
      stream_uuid: stream_uuid,
      subscription_name: subscription_name,
      subscription: SubscriptionFsm.new(conn, stream_uuid, subscription_name, subscription_opts),
      retry_interval: subscription_retry_interval()
    }

    GenServer.start_link(__MODULE__, state, opts)
  end

  @doc """
  Connect a subscriber to a started subscription.
  """
  def connect(subscription, subscriber, subscription_opts) do
    GenServer.call(subscription, {:connect, subscriber, subscription_opts})
  end

  @doc """
  Confirm receipt of an event by its event number for a given subscriber.
  """
  def ack(subscription, ack, subscriber) when is_integer(ack) and is_pid(subscriber) do
    GenServer.cast(subscription, {:ack, ack, subscriber})
  end

  @doc """
  Confirm receipt of an event by its event number.
  """
  def ack(subscription, ack) when is_integer(ack) do
    GenServer.cast(subscription, {:ack, ack, self()})
  end

  @doc """
  Confirm receipt of the given list of events.
  """
  def ack(subscription, events) when is_list(events) do
    Subscription.ack(subscription, List.last(events))
  end

  @doc """
  Confirm receipt of the given `EventStore.RecordedEvent` struct.
  """
  def ack(subscription, %RecordedEvent{event_number: event_number}) do
    Subscription.ack(subscription, event_number)
  end

  @doc """
  Unsubscribe a subscriber from the subscription.
  """
  def unsubscribe(subscription) do
    GenServer.call(subscription, {:unsubscribe, self()})
  end

  @doc false
  def last_seen(subscription) do
    GenServer.call(subscription, :last_seen)
  end

  @doc false
  def init(%Subscription{} = state) do
    # Schedule an event pooling mechanism that will catch up a subscription to recent events.
    # In this way a subscription does not rely entirely on the PostgreSQL listener
    # capability of notifying events.
    schedule_event_pooling()

    {:ok, state}
  end

  def handle_info(:subscribe_to_stream, %Subscription{subscription: subscription} = state) do
    _ = Logger.debug(fn -> describe(state) <> " subscribe to stream" end)

    state =
      subscription
      |> SubscriptionFsm.subscribe()
      |> apply_subscription_to_state(state)

    {:noreply, state}
  end

  def handle_info({:events, events}, %Subscription{subscription: subscription} = state) do
    _ = Logger.debug(fn -> describe(state) <> " received #{length(events)} event(s)" end)

    state =
      subscription
      |> SubscriptionFsm.notify_events(events)
      |> apply_subscription_to_state(state)

    {:noreply, state}
  end

  def handle_info(
        {EventStore.AdvisoryLocks, :lock_released, lock_ref, reason},
        %Subscription{} = state
      ) do
    %Subscription{subscription: subscription} = state

    _ =
      Logger.debug(fn -> describe(state) <> " advisory lock lost due to: " <> inspect(reason) end)

    state =
      subscription
      |> SubscriptionFsm.disconnect(lock_ref)
      |> apply_subscription_to_state(state)

    {:noreply, state}
  end

  def handle_info(
        :event_pooling,
        %Subscription{subscription: %SubscriptionFsm{state: fsm_state}} = state
      ) do
    if fsm_state == :subscribed do
      GenServer.cast(self(), :catch_up)
    end

    schedule_event_pooling()

    {:noreply, state}
  end

  def handle_info({:DOWN, _ref, :process, pid, reason}, %Subscription{} = state) do
    %Subscription{subscription: subscription} = state

    _ =
      Logger.debug(fn ->
        describe(state) <> " subscriber #{inspect(pid)} down due to: #{inspect(reason)}"
      end)

    state =
      subscription
      |> SubscriptionFsm.unsubscribe(pid)
      |> apply_subscription_to_state(state)

    if unsubscribed?(state) do
      {:stop, reason, state}
    else
      {:noreply, state}
    end
  end

  def handle_cast(:catch_up, %Subscription{subscription: subscription} = state) do
    state =
      subscription
      |> SubscriptionFsm.catch_up()
      |> apply_subscription_to_state(state)

    {:noreply, state}
  end

  def handle_cast({:ack, ack, subscriber}, %Subscription{subscription: subscription} = state) do
    state =
      subscription
      |> SubscriptionFsm.ack(ack, subscriber)
      |> apply_subscription_to_state(state)

    {:noreply, state}
  end

  def handle_call({:connect, subscriber, opts}, _from, %Subscription{} = state) do
    %Subscription{
      subscription:
        %SubscriptionFsm{data: %SubscriptionState{subscribers: subscribers}} = subscription
    } = state

    _ =
      Logger.debug(fn ->
        describe(state) <> " attempting to connect subscriber " <> inspect(subscriber)
      end)

    with :ok <- ensure_not_already_subscribed(subscribers, subscriber),
         :ok <- ensure_within_concurrency_limit(subscribers, opts) do
      state =
        subscription
        |> SubscriptionFsm.connect_subscriber(subscriber, opts)
        |> SubscriptionFsm.subscribe()
        |> apply_subscription_to_state(state)

      {:reply, {:ok, self()}, state}
    else
      {:error, _error} = reply ->
        {:reply, reply, state}
    end
  end

  def handle_call({:unsubscribe, pid}, _from, %Subscription{} = state) do
    %Subscription{subscription: subscription} = state

    state =
      subscription
      |> SubscriptionFsm.unsubscribe(pid)
      |> apply_subscription_to_state(state)

    if unsubscribed?(state) do
      {:stop, :shutdown, :ok, state}
    else
      {:reply, :ok, state}
    end
  end

  def handle_call(:last_seen, _from, %Subscription{subscription: subscription} = state) do
    %SubscriptionFsm{data: %SubscriptionState{last_ack: last_seen}} = subscription

    {:reply, last_seen, state}
  end

  defp apply_subscription_to_state(%SubscriptionFsm{} = subscription, %Subscription{} = state) do
    handle_subscription_state(%Subscription{state | subscription: subscription})
  end

  # Attempt to subscribe to an initial or disconnected subscription after a
  # retry interval.
  defp handle_subscription_state(
         %Subscription{subscription: %SubscriptionFsm{state: fsm}} = state
       )
       when fsm in [:initial, :disconnected] do
    %Subscription{retry_interval: retry_interval} = state

    _ref = Process.send_after(self(), :subscribe_to_stream, retry_interval)

    state
  end

  defp handle_subscription_state(
         %Subscription{subscription: %SubscriptionFsm{state: :request_catch_up}} = state
       ) do
    _ = Logger.debug(fn -> describe(state) <> " catching-up" end)

    :ok = GenServer.cast(self(), :catch_up)

    state
  end

  defp handle_subscription_state(
         %Subscription{subscription: %SubscriptionFsm{state: :max_capacity}} = state
       ) do
    _ =
      Logger.warn(fn ->
        describe(state) <>
          " has reached max capacity, events will be ignored until it has caught up"
      end)

    state
  end

  defp handle_subscription_state(
         %Subscription{subscription: %SubscriptionFsm{state: :unsubscribed}} = state
       ) do
    _ = Logger.debug(fn -> describe(state) <> " has no subscribers, shutting down" end)

    state
  end

  # No-op for all other subscription states.
  defp handle_subscription_state(%Subscription{} = state), do: state

  defp schedule_event_pooling do
    Process.send_after(self(), :event_pooling, event_pooling_interval())
  end

  # Get the delay between subscription attempts, in milliseconds, from app
  # config. The default value is one minute and minimum allowed value is one
  # second.
  defp subscription_retry_interval do
    get_interval(:subscription_retry_interval, 60_000, 1_000)
  end

  # Get the delay between event pooling requests, in milliseconds, from app
  # config. The default value is 5 seconds and minimum allowed value is one
  # second.
  defp event_pooling_interval do
    get_interval(:event_pooling_interval, 5_000, 1_000)
  end

  defp get_interval(interval_config_name, default, min) do
    case Application.get_env(:eventstore, interval_config_name) do
      interval when is_integer(interval) and interval > 0 ->
        max(interval, min)

      _ ->
        default
    end
  end

  # Prevent duplicate subscriptions from same process.
  defp ensure_not_already_subscribed(subscribers, pid) do
    unless Map.has_key?(subscribers, pid) do
      :ok
    else
      {:error, :already_subscribed}
    end
  end

  # Prevent more subscribers than requested concurrency limit.
  defp ensure_within_concurrency_limit(subscribers, opts) do
    concurrency_limit = Keyword.get(opts, :concurrency_limit, 1)

    if Map.size(subscribers) < concurrency_limit do
      :ok
    else
      {:error, :too_many_subscribers}
    end
  end

  def unsubscribed?(%Subscription{subscription: %SubscriptionFsm{state: :unsubscribed}}), do: true
  def unsubscribed?(%Subscription{}), do: false

  defp describe(%Subscription{stream_uuid: stream_uuid, subscription_name: name}),
    do: "Subscription #{inspect(name)}@#{inspect(stream_uuid)}"
end
