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

  def insert(tab, rec) do
    :ets.insert(tab, rec)
  end

  def delete(tab, {key, _}) do
    :ets.delete(tab, key)
  end

  def pop_request(tab, outstanding_tab) do
    [obj] = :ets.take(tab, :ets.last(tab))
    insert(outstanding_tab, obj)
    obj
  end

  def new_request(tab, req) do
    {new_request_key(tab), req} 
  end

  def select_all(tab) do
    ms = [{String.to_atom("$1"),[],[String.to_atom("$1")]}]
    :ets.select(tab, ms)
  end

  def pop_all(tab) do
    ms = [{String.to_atom("$1"),[],[String.to_atom("$1")]}]
    vals = :ets.select(tab, ms)
    :ets.delete_all_objects(tab)
    vals
  end

  def pop_all_requests(tab, outstanding_tab) do
    vals = pop_all(tab)
    :ets.insert(outstanding_tab, vals)
    vals
  end
end
