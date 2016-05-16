defmodule Sturm.TopologyDsl do
  @doc "Expand topology to a supervisor tree"
  def topology(defs) do

  end

  def source(name, mod, opts) do
  end

  def sink(name, mod, opts) do
  end

  def worker(name, mod, opts) do
    count = Keyword.get(opts, :workers, :single)
    case count do
      :single -> [Supervisor.Spec.worker(mod, [], id: name, shutdown: :brutal_kill)]
      _ -> Enum.map(Range.new(1, count), fn(x) -> 
            Supervisor.spec.worker(mod, [], id: worker_with_count(name, x), shutdown: :brutal_kill) 
           end)
    end
  end

  defp worker_with_count(name, n) when is_atom(name) do
    String.to_atom(Atom.to_string(name) <> "_" <> Integer.to_string(n))
  end

  defp worker_with_count(name, n) when is_binary(name) do
    String.to_atom(name <> "_" <> Integer.to_string(n))
  end
end
