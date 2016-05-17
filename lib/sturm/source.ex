defmodule Sturm.Source do
  @callback start_link(Sturm.WorkerConfig) :: any

  defmacro __using__(_) do 
    quote do
      def emit_result(outs, result) do
        Enum.map(outs, fn(o) ->
          Sturm.PullCoordinator.request(o, result)
        end)
      end
    end
  end
end
