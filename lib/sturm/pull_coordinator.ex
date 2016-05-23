defmodule Sturm.PullCoordinator do
  use GenServer

  @type request :: any

  @spec start_global_link(term) :: GenServer.on_start
  def start_global_link(name) do
    case GenServer.start(Sturm.PullCoordinator, {:global, name}, name: {:via, Sturm.PullCoordinatorGlobalNaming, name}) do
      {:ok, pid} -> 
        Process.link(pid)
        {:ok, pid}
      {:error, {:already_started, pid}} -> 
        Process.link(pid)
        {:ok, pid}
      other -> other
    end
  end

  @spec worker_available(GenServer.name, %Sturm.PullWorkerDefinition{}) :: :ok
  def worker_available(namespec, worker_spec) do
    GenServer.cast(namespec, {:worker_available, worker_spec})
  end

  @spec request(GenServer.name, request) :: :ok
  def request(namespec, req), do: GenServer.cast(namespec, {:request, req})

  def request_handled(namespec, req) do
    GenServer.cast(namespec, {:request_handled, req})
  end

  def init(namespec) do
    init_state =  Sturm.CoordinatorState.from_namespec(namespec)
    Sturm.CoordinatorEtsBackup.create_table_for(init_state.tablespec, self())
    Sturm.CoordinatorEtsBackup.create_table_for(init_state.outstanding_tablespec, self())
    existing_requests = Sturm.EtsFifo.pop_all(init_state.outstanding_tablespec)
    Sturm.EtsFifo.insert(init_state.tablespec, existing_requests)
    {:ok, init_state}
  end


  def merge_coordinators(_name, pid_1, pid_2) do
    other_state = GenServer.call(pid_2, {:pop_state})
    merge_state(pid_1, other_state)
    GenServer.stop(pid_2)
    pid_1
  end

  def merge_state(pid, other_state) do
    GenServer.call(pid, {:merge_state, other_state})
  end


  def handle_call({:pop_state}, _from, state) do
    requests = Sturm.EtsFifo.pop_all(state.tablespec)
    outstanding_requests = Sturm.EtsFifo.pop_all(state.outstanding_tablespec)
    {:reply, {requests, outstanding_requests, state.workers}, state}
  end

  def handle_call({:get_state}, _from, state) do
    requests = Sturm.EtsFifo.select_all(state.tablespec)
    outstanding_requests = Sturm.EtsFifo.select_all(state.outstanding_tablespec)
    {:reply, {requests, outstanding_requests, state.workers}, state}
  end

  def handle_call({:merge_state, {other_requests, outstanding_requests, other_workers}}, _from, state) do
    Sturm.EtsFifo.insert(state.tablespec, other_requests)
    Sturm.EtsFifo.insert(state.outstanding_tablespec, outstanding_requests)
    updated_state = %{state | workers: :queue.join(state.workers, other_workers)}
    {:reply, nil, updated_state}
  end

  def handle_cast({:request_handled, req}, state) do
    Sturm.EtsFifo.delete(state.outstanding_tablespec, req)
    {:noreply, state}
  end

  def handle_cast({:worker_available, workerspec}, state) do
    case Sturm.EtsFifo.size(state.tablespec) do
       0 ->
         updated_state = %{state | workers: :queue.in(workerspec, state.workers)}
         {:noreply, updated_state}
       _ -> call_single_request(workerspec, state)
    end
  end

  def handle_cast({:request, req}, state) do
    case :queue.is_empty(state.workers) do
      true ->
         Sturm.EtsFifo.push(state.tablespec, req)
         {:noreply, state}
      _ -> send_request_to_worker(req, state)
    end
  end

  defp send_request_to_worker(req, state) do
    case Sturm.EtsFifo.size(state.tablespec) do
       0 -> call_single_worker(req, state)
       _ -> call_workers(req, state)
    end
  end

  defp call_single_request(workerspec, state) do
    req = Sturm.EtsFifo.pop_request(state.tablespec, state.outstanding_tablespec)
    Sturm.PullWorkerDefinition.cast_worker(workerspec, state.namespec, req)
    {:noreply, state}
  end

  defp call_single_worker(req, state) do
    {{:value, workerspec}, remaining_workers} = :queue.out(state.workers)
    request_record = Sturm.EtsFifo.new_request(state.tablespec, req)
    Sturm.EtsFifo.insert(state.outstanding_tablespec, request_record)
    Sturm.PullWorkerDefinition.cast_worker(workerspec, state.namespec, request_record)
    updated_state = %{state | workers: remaining_workers}
    {:noreply, updated_state}
  end

  defp call_workers(req, state) do
    workers_available = :queue.length(state.workers)
    requests_available = Sturm.EtsFifo.size(state.tablespec)
    case (workers_available > requests_available) do
      true -> invoke_workers_from_requests(req, requests_available + 1, state)
      _ -> invoke_requests_using_workers(req, workers_available, state)
    end
  end

  defp invoke_worker_request_pairings(pairings, namespec) do
     Enum.map(pairings,
       fn(p) ->
         {workerspec, r} = p
         Sturm.PullWorkerDefinition.cast_worker(workerspec, namespec, r)
       end
     )
  end

  defp invoke_requests_using_workers(req, workers_available, state) do
     processing_requests = Enum.map((1..(workers_available - 1)), fn(_) ->
       Sturm.EtsFifo.pop_request(state.tablespec, state.outstanding_tablespec)
     end)
     request_record = Sturm.EtsFifo.new_request(state.tablespec, req)
     Sturm.EtsFifo.insert(state.outstanding_tablespec, request_record)
     pairings = Enum.zip(:queue.to_list(state.workers), processing_requests ++ [req])
     invoke_worker_request_pairings(pairings, state.namespec)
     updated_state = %{state | workers: :queue.new()}
     {:noreply, updated_state}
  end

  defp invoke_workers_from_requests(req, requests_to_process, state) do
     {working_workers, remaining_workers} = :queue.split(requests_to_process, state.workers)
     requests = Sturm.EtsFifo.pop_all_requests(state.tablespec, state.outstanding_tablespec)
     request_record = Sturm.EtsFifo.new_request(state.tablespec, req)
     Sturm.EtsFifo.insert(state.outstanding_tablespec, request_record)
     pairings = Enum.zip(:queue.to_list(working_workers), requests ++ [request_record])
     invoke_worker_request_pairings(pairings, state.namespec)
     updated_state = %{state | workers: remaining_workers}
     {:noreply, updated_state}
  end

end
