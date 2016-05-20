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
      :undefined -> 
        :ets.new(table, [:named_table, {:keypos, 1}])
        :ets.insert(table, {:requests, :queue.new()})
        :ets.insert(table, {:workers, :queue.new()})
      _ -> :do_nothing
    end
    :ets.setopts(table, [{:heir, self(), {}}])
    :ets.give_away(table, pid, {})
    {:noreply, state}
  end

  def create_table_for(table, pid) do
    GenServer.call({:sturm_coordinators_ets_backup, :erlang.node()}, {:create_table_for, table, pid})
  end

  def start_link do
    GenServer.start_link(__MODULE__, [], name: {:local, :sturm_coordinators_ets_backup})
  end
end
