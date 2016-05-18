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

      def handle_cast({:request, {my_ns, coordinator_ns, req}}, my_state) do
        state_result = case do_work(req, my_state.state) do
          {:ok, new_state} -> new_state
          {:error, new_state} -> requeue_record(coordinator_ns, req, new_state)
        end
        Sturm.PullCoordinator.worker_available(coordinator_ns, %Sturm.PullWorkerDefinition{module: __MODULE__, namespec: my_ns})
        {:noreply, state_result}
      end

      def requeue_record(coordinator_ns, req, new_state) do
        Sturm.PullCoordinator.request(coordinator_ns, req)
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
