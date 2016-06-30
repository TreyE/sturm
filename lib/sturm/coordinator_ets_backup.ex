defmodule Sturm.CoordinatorEtsBackup do
  use GenServer

  def init(_args) do
    {:ok, nil}
  end

  def handle_info(_info, state) do
    {:noreply, state}
  end

  def handle_call({:create_table_for, table, pid}, _from, state) do
    table_info = :ets.info(table)
    case table_info do
      :undefined -> :ets.new(table, [:named_table, :ordered_set, {:keypos, 1}])
      _ -> :ok
    end
    :ets.setopts(table, [{:heir, self(), {}}])
    :ets.give_away(table, pid, {})
    {:reply, :ok, state}
  end

  def create_table_for(table, pid) do
    GenServer.call(:sturm_coordinators_ets_backup, {:create_table_for, table, pid})
  end

  def start_link do
    GenServer.start_link(__MODULE__, [], name: __MODULE__)
  end
end
