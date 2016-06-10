defmodule Sturm.PullSink do
  @type sink_state :: any
  @type sink_request :: any

  @callback do_work(sink_request, sink_state) :: {:ok, sink_state} |
                                                 {:error, sink_state}

  @callback init_sink(any) :: {:ok, sink_state} |
                              {:stop, reason :: any}

  defmacro __using__(_) do 
    quote do
      use GenServer

      @behaviour Sturm.PullSink

      def init(args) do
        case init_sink(args.options) do
          {:ok, start_state} -> {:ok, %Sturm.WorkState{state: start_state}}
          {:stop, reason} -> {:stop, reason}
        end
      end

      defp check_retries(request, retry_count, my_state) do
        case (retry_count > my_state.retries) do
          true -> {:too_many_retries, my_state}
          _ -> do_work(request, my_state.state)
        end
      end

      def handle_cast({:request, {my_ns, coordinator_ns, req}}, my_state) do
        {_, request, retry_count} = req
        state_result = case check_retries(request, retry_count, my_state) do
          {:too_many_retries, new_state} -> new_state
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
