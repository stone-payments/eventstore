defmodule EventStore.Notifications.Heartbeat do
  @moduledoc """
  This module contains a timeout mechanism to restart the Notifications database connection
  and catch up the subscriptions to recent events periodically.
  """

  use GenServer

  require Logger

  @timeout 10_000

  def start_link(_args) do
    GenServer.start_link(__MODULE__, [], name: __MODULE__)
  end

  def init(state) do
    schedule_work()
    {:ok, state}
  end

  def handle_info(:heartbeat, state) do
    Logger.debug("Killing Postgrex Notifications due to listener heartbeat")
    restart_notifications()

    Logger.debug("Catching up subscriptions due to listener heartbeat")
    catch_up_subscriptions()

    schedule_work()
    {:noreply, state}
  end

  defp schedule_work(), do: Process.send_after(self(), :heartbeat, @timeout)

  defp restart_notifications() do
    EventStore.Notifications.Listener.Postgrex
    |> :sys.get_state()
    |> Map.get(:pid)
    |> Process.exit(:kill)
  end

  defp catch_up_subscriptions() do
    EventStore.Subscriptions.Supervisor
    |> Supervisor.which_children()
    |> Enum.each(fn {_, subscription, _, _} ->
      EventStore.Subscriptions.Subscription.catch_up(subscription)
    end)
  end
end
