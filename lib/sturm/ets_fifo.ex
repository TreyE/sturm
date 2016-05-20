defmodule Sturm.EtsFifo do
  def new_request_key(tab) do
    ts = :os.timestamp()
    ms = [{{{ts,String.to_atom("_")},String.to_atom("_")},[],[true]}]
    highest_number = :ets.select_count(tab, ms)
    {ts, highest_number + 1}
  end

  def size(tab) do
    Keyword.get(:ets.info(tab), :size, 0)
  end

  def push(tab, item) do
    :ets.insert(tab, {new_request_key(tab), item})
  end

  def pop(tab) do
    [{_, obj}] = :ets.take(tab, :ets.last(tab))
    obj
  end

  def pop_all(tab) do
    ms = [{{String.to_atom("_"),String.to_atom("$1")},[],[String.to_atom("$1")]}]
    vals = :ets.select(tab, ms)
    :ets.delete_all_objets(tab)
    vals
  end
end
