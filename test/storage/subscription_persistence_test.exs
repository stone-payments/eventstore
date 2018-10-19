defmodule EventStore.Storage.SubscriptionPersistenceTest do
  use EventStore.StorageCase

  alias EventStore.Storage

  @all_stream "$all"
  @subscription_name "test_subscription"

  test "create subscription", %{conn: conn} do
    {:ok, subscription} = Storage.subscribe_to_stream(conn, @all_stream, @subscription_name)

    verify_subscription(subscription)
  end

  test "create subscription when already exists", %{conn: conn} do
    {:ok, subscription1} = Storage.subscribe_to_stream(conn, @all_stream, @subscription_name)
    {:ok, subscription2} = Storage.subscribe_to_stream(conn, @all_stream, @subscription_name)

    verify_subscription(subscription1)
    verify_subscription(subscription2)

    assert subscription1.subscription_id == subscription2.subscription_id
  end

  test "list subscriptions", %{conn: conn} do
    {:ok, subscription} = Storage.subscribe_to_stream(conn, @all_stream, @subscription_name)
    {:ok, subscriptions} = Storage.subscriptions(conn)

    assert length(subscriptions) > 0
    assert Enum.member?(subscriptions, subscription)
  end

  test "remove subscription when exists", %{conn: conn} do
    {:ok, subscriptions} = Storage.subscriptions(conn)
    initial_length = length(subscriptions)

    {:ok, _subscription} = Storage.subscribe_to_stream(conn, @all_stream, @subscription_name)
    :ok = Storage.delete_subscription(conn, @all_stream, @subscription_name)

    {:ok, subscriptions} = Storage.subscriptions(conn)
    assert length(subscriptions) == initial_length
  end

  test "remove subscription when not found should not fail", %{conn: conn} do
    :ok = Storage.delete_subscription(conn, @all_stream, @subscription_name)
  end

  test "ack last seen event by id", %{conn: conn} do
    {:ok, _subscription} = Storage.subscribe_to_stream(conn, @all_stream, @subscription_name)

    :ok = Storage.ack_last_seen_event(conn, @all_stream, @subscription_name, 1)

    {:ok, subscriptions} = Storage.subscriptions(conn)

    subscription = subscriptions |> Enum.reverse() |> hd

    verify_subscription(subscription, 1)
  end

  test "ack last seen event by stream version", %{conn: conn} do
    {:ok, _subscription} = Storage.subscribe_to_stream(conn, @all_stream, @subscription_name)

    :ok = Storage.ack_last_seen_event(conn, @all_stream, @subscription_name, 1)

    {:ok, subscriptions} = Storage.subscriptions(conn)

    subscription = subscriptions |> Enum.reverse() |> hd

    verify_subscription(subscription, 1)
  end

  defp verify_subscription(subscription, last_seen \\ nil)

  defp verify_subscription(subscription, last_seen) do
    assert subscription.subscription_id > 0
    assert subscription.stream_uuid == @all_stream
    assert subscription.subscription_name == @subscription_name
    assert subscription.last_seen == last_seen
    assert subscription.created_at != nil
  end
end
