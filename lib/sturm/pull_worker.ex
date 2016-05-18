defmodule Sturm.PullWorker do
  @type worker_state :: any
  @type worker_request :: any

  @callback do_work(worker_request, worker_state) :: {:ok, worker_state} |
                                                     {:emit, worker_state, list(term)} |
                                                     {:error, worker_state}

  @callback init_worker(any) :: {:ok, worker_state} |
                                {:stop, reason :: any}

  defmacro __using__(_) do 
    quote do
      use GenServer

      @behaviour Sturm.PullWorker

      def init(args) do
        IO.puts("#{inspect args}")
        case init_worker(args.options) do
          {:ok, start_state} -> {:ok, %Sturm.WorkState{outs: args.out_coordinators, state: start_state}}
          {:stop, reason} -> {:stop, reason}
        end
      end

      def handle_cast({:request, {my_ns, coordinator_ns, req}}, my_state) do
        state_result = case do_work(req, my_state.state) do
          {:emit, new_state, records} -> emit_records(my_state.outs, records, new_state)
          {:ok, new_state} -> new_state
          {:error, new_state} -> requeue_record(coordinator_ns, req, new_state)
        end
        Sturm.PullCoordinator.worker_available(coordinator_ns, %Sturm.PullWorkerDefinition{module: __MODULE__, namespec: my_ns})
        {:noreply, Map.merge(my_state, %{state: state_result})}
      end

      def requeue_record(coordinator_ns, req, new_state) do
        Sturm.PullCoordinator.request(coordinator_ns, req)
        new_state
      end

      def emit_records(outs, records, new_state) do
        Enum.map(outs, fn(o) ->
          Enum.map(records, fn(r) -> Sturm.PullCoordinator.request(o, r) end)
        end)
        new_state
      end

      def cast_request(my_ns, req) do
        GenServer.cast(my_ns, {:request, {my_ns, req.in_source, req.body}})
      end

      def start_link(args) do
        case GenServer.start_link(__MODULE__, args, name: args.worker_id) do
          {:ok, pid} -> 
                      Sturm.PullCoordinator.worker_available(args.in_source, %Sturm.PullWorkerDefinition{module: __MODULE__, namespec: pid})
                      {:ok, pid}
          other -> other
        end
      end
    end
  end
end
