defmodule EventStore.Subscriptions.Subscription do
  @moduledoc """
  Subscription to a single, or all, event streams.

  A subscription is persistent so that resuming the subscription will continue
  from the last acknowledged event. This guarantees at least once delivery of
  every event appended to storage.
  """

  use GenServer
  require Logger

  alias EventStore.RecordedEvent
  alias EventStore.Subscriptions.{StreamSubscription,Subscription}

  defstruct [
    conn: nil,
    stream_uuid: nil,
    subscription_name: nil,
    subscriber: nil,
    subscription: nil,
    subscription_opts: [],
    postgrex_config: nil
  ]

  def start_link(postgrex_config, stream_uuid, subscription_name, subscriber, subscription_opts, opts \\ []) do
    GenServer.start_link(__MODULE__, %Subscription{
      stream_uuid: stream_uuid,
      subscription_name: subscription_name,
      subscriber: subscriber,
      subscription: StreamSubscription.new(),
      subscription_opts: subscription_opts,
      postgrex_config: postgrex_config
    }, opts)
  end

  def notify_events(subscription, events) when is_list(events) do
    GenServer.cast(subscription, {:notify_events, events})
  end

  @doc """
  Confirm receipt of the given event by its `event_number` or `stream_version`
  """
  def ack(subscription, ack) when is_integer(ack) do
    GenServer.cast(subscription, {:ack, ack})
  end

  @doc """
  Confirm receipt of the given events
  """
  def ack(subscription, events) when is_list(events) do
    Subscription.ack(subscription, List.last(events))
  end

  @doc """
  Confirm receipt of the given event
  """
  def ack(subscription, %RecordedEvent{event_number: event_number, stream_version: stream_version}) do
    GenServer.cast(subscription, {:ack, {event_number, stream_version}})
  end

  @doc false
  def caught_up(subscription, last_seen) do
    GenServer.cast(subscription, {:caught_up, last_seen})
  end

  @doc false
  def unsubscribe(subscription) do
    GenServer.call(subscription, :unsubscribe)
  end

  @doc false
  def subscribed?(subscription) do
    GenServer.call(subscription, :subscribed?)
  end

  @doc false
  def init(%Subscription{subscriber: subscriber, postgrex_config: postgrex_config} = state) do
    Process.link(subscriber)
    
    # A subscription has its own connection to the database to enforce locking
    {:ok, conn} = Postgrex.start_link(postgrex_config)

    send(self(), :subscribe_to_stream)

    {:ok, %Subscription{state | conn: conn}}
  end

  def handle_info(:subscribe_to_stream, %Subscription{} = state) do
    %Subscription{
      conn: conn,
      stream_uuid: stream_uuid,
      subscription_name: subscription_name,
      subscriber: subscriber,
      subscription: subscription,
      subscription_opts: opts
    } = state

    subscription = StreamSubscription.subscribe(subscription, conn, stream_uuid, subscription_name, subscriber, opts)

    state = %Subscription{state | subscription: subscription}

    :ok = handle_subscription_state(state)

    subscribe_to_events(stream_uuid)

    {:noreply, state}
  end

  def handle_cast({:notify_events, events}, %Subscription{subscription: subscription} = state) do
    subscription = StreamSubscription.notify_events(subscription, events)

    state = %Subscription{state | subscription: subscription}

    :ok = handle_subscription_state(state)

    {:noreply, state}
  end

  def handle_cast(:catch_up, %Subscription{subscription: subscription} = state) do
    subscription = StreamSubscription.catch_up(subscription)

    state = %Subscription{state | subscription: subscription}

    :ok = handle_subscription_state(state)

    {:noreply, state}
  end

  def handle_cast({:caught_up, last_seen}, %Subscription{subscription: subscription} = state) do
    subscription = StreamSubscription.caught_up(subscription, last_seen)

    state = %Subscription{state | subscription: subscription}

    :ok = handle_subscription_state(state)

    {:noreply, state}
  end

  def handle_cast({:ack, ack}, %Subscription{subscription: subscription} = state) do
    subscription = StreamSubscription.ack(subscription, ack)

    state = %Subscription{state | subscription: subscription}

    :ok = handle_subscription_state(state)

    {:noreply, state}
  end

  def handle_call(:unsubscribe, _from, %Subscription{subscriber: subscriber, subscription: subscription} = state) do
    Process.unlink(subscriber)

    subscription = StreamSubscription.unsubscribe(subscription)

    state = %Subscription{state | subscription: subscription}

    :ok = handle_subscription_state(state)

    {:reply, :ok, state}
  end

  def handle_call(:subscribed?, _from, %Subscription{subscription: %{state: subscription_state}} = state) do
    reply = case subscription_state do
      :subscribed -> true
      _ -> false
    end

    {:reply, reply, state}
  end

  defp subscribe_to_events(stream_uuid) do
    {:ok, _} = Registry.register(EventStore.Subscriptions.PubSub, stream_uuid, {Subscription, :notify_events})
  end

  defp handle_subscription_state(%Subscription{subscription: %{state: :initial}, subscription_name: subscription_name}) do
    retry_interval = subscription_retry_interval()
    Logger.debug(fn -> "Failed to subscribe to #{subscription_name}, will retry in #{retry_interval}ms" end)
    Process.send_after(self(), :subscribe_to_stream, retry_interval)
  end

  defp handle_subscription_state(%Subscription{subscription: %{state: :request_catch_up}}) do
    GenServer.cast(self(), :catch_up)
  end

  defp handle_subscription_state(%Subscription{subscription: %{state: :max_capacity}, subscription_name: subscription_name}) do
    _ = Logger.warn(fn -> "Subscription #{subscription_name} has reached max capacity, events will be ignored until it has caught up" end)
    :ok
  end

  # no-op for all other subscription states
  defp handle_subscription_state(_state), do: :ok

  # get the delay between subscription attempts, in milliseconds, from app
  # config. Default value is one minute.
  defp subscription_retry_interval do
    case Application.get_env(:eventstore, :subscription_retry_interval) do
      interval when is_integer(interval) -> interval
      _ -> 60_000
    end
  end
end
