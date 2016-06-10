defmodule Sturm.EtsFifo do
  def new_request_key(tab) do
    ts = :os.timestamp()
    ms = [{{{ts,String.to_atom("_")},:"_",:"_"},[],[true]}]
    highest_number = :ets.select_count(tab, ms)
    {ts, highest_number + 1}
  end

  def size(tab) do
    Keyword.get(:ets.info(tab), :size, 0)
  end

  def push(tab, item, retry_count \\ 0) do
    :ets.insert(tab, {new_request_key(tab), item, retry_count})
  end

  def insert(tab, rec) do
    :ets.insert(tab, rec)
  end

  def delete(tab, {key, _, _}) do
    :ets.delete(tab, key)
  end

  def pop_request(tab, outstanding_tab) do
    [obj] = :ets.take(tab, :ets.last(tab))
    insert(outstanding_tab, obj)
    obj
  end

  def new_request(tab, req, retry_count \\ 0) do
    {new_request_key(tab), req, retry_count} 
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
