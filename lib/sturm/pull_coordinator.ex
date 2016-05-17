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

  def init(namespec), do: {:ok, {namespec, :queue.new(), :queue.new()}}

  def merge_coordinators(_name, pid_1, pid_2) do
    other_state = GenServer.call(pid_2, {:get_state})
    merge_state(pid_1, other_state)
    GenServer.stop(pid_2)
    pid_1
  end

  def merge_state(pid, other_state) do
    GenServer.call(pid, {:merge_state, other_state})
  end

  def handle_call({:get_state}, _from, my_state) do
    {:reply, my_state, my_state}
  end

  def handle_call({:merge_state, other_state}, _from, {namespec, workers, requests}) do
    {_other_namespec, other_workers, other_requests} = other_state
    {:reply, nil, {namespec, :queue.join(workers, other_workers), :queue.join(requests, other_requests)}}
  end

  def handle_cast({:worker_available, workerspec}, {namespec, workers, requests}) do
    case :queue.is_empty(requests) do
       true -> {:noreply, {namespec, :queue.in(workerspec, workers), requests}}
       _ -> call_single_request(namespec, workerspec, workers, requests)
    end
  end

  def handle_cast({:request, req}, {namespec, workers, requests}) do
    case :queue.is_empty(workers) do
      true -> {:noreply, {namespec, workers, :queue.in(req,requests)}}
      _ -> send_request_to_worker(namespec, req, workers, requests)
    end
  end

  defp send_request_to_worker(namespec, req, workers, requests) do
    case :queue.is_empty(requests) do
       true -> call_single_worker(namespec, req, workers, requests)
       _ -> call_workers(namespec, req, workers, requests)
    end
  end

  defp call_single_request(namespec, workerspec, workers, requests) do
    {{:value, req}, remaining_requests} = :queue.out(requests)
    Sturm.PullWorkerDefinition.cast_worker(workerspec, namespec, req)
    {:noreply, {namespec, workers, remaining_requests}}
  end

  defp call_single_worker(namespec, req, workers, requests) do
    {{:value, workerspec}, remaining_workers} = :queue.out(workers)
    Sturm.PullWorkerDefinition.cast_worker(workerspec, namespec, req)
    {:noreply, {namespec, remaining_workers, requests}}
  end

  defp call_workers(namespec, req, workers, requests) do
    workers_available = :queue.length(workers)
    requests_available = :queue.length(requests)
    case (workers_available > requests_available) do
      true -> invoke_workers_from_requests(namespec, req, workers, requests, requests_available + 1)
      _ -> invoke_requests_using_workers(namespec, req, workers, requests, workers_available)
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

  defp invoke_requests_using_workers(namespec, req, workers, requests, workers_available) do
     {processing_requests, remaining_requests} = :queue.split(workers_available - 1, requests)
     pairings = Enum.zip(:queue.to_list(workers), :queue.to_list(processing_requests) ++ [req])
     invoke_worker_request_pairings(pairings, namespec)
     {:noreply, {namespec, :queue.new(), remaining_requests}}
  end

  defp invoke_workers_from_requests(namespec, req, workers, requests, requests_to_process) do
     {working_workers, remaining_workers} = :queue.split(requests_to_process, workers)
     pairings = Enum.zip(:queue.to_list(working_workers), :queue.to_list(requests) ++ [req])
     invoke_worker_request_pairings(pairings, namespec)
     {:noreply, {namespec, remaining_workers, :queue.new()}}
  end

end
