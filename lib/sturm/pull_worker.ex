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
        case init_worker(args.options) do
          {:ok, start_state} -> {:ok, %Sturm.WorkState{outs: args.out_coordinators, state: start_state}}
          {:stop, reason} -> {:stop, reason}
        end
      end

      defp check_retries(request, retry_count, my_state) do
        try do
          case (retry_count > my_state.retries) do
            true -> {:too_many_retries, my_state.state}
            _ -> do_work(request, my_state.state)
          end
        rescue
          _ -> {:error, my_state.state}
        catch
          :exit, _ -> {:error, my_state.state}
        end
      end

      def handle_cast({:request, {my_ns, coordinator_ns, req}}, my_state) do
        {_, request, retry_count} = req
        state_result = case check_retries(request, retry_count, my_state) do
          {:too_many_retries, new_state} -> new_state
          {:emit, new_state, records} -> emit_records(my_state.outs, records, new_state)
          {:ok, new_state} -> new_state
          {:error, new_state} -> requeue_record(coordinator_ns, request, new_state, retry_count + 1)
        end
        Sturm.PullCoordinator.request_handled(coordinator_ns, req)
        Sturm.PullCoordinator.worker_available(coordinator_ns, %Sturm.PullWorkerDefinition{module: __MODULE__, namespec: my_ns})
        {:noreply, Map.merge(my_state, %{state: state_result})}
      end

      def requeue_record(coordinator_ns, req, new_state, retry_count) do
        Sturm.PullCoordinator.request(coordinator_ns, req, retry_count)
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
