defmodule Sturm.TopologyDsl do
  @doc "Expand topology to a supervisor tree"
  def topology(defs) do
    ins = Enum.flat_map(defs, &map_ins/1) |> Enum.uniq |> Enum.sort
    outs = Enum.flat_map(defs, &map_outs/1) |> Enum.uniq |> Enum.sort
    case ins == outs do
      true -> generate_specs(ins, defs)
      _ -> {:error, {:routing_mismatch, %{ins: ins, outs: outs}}}
    end
  end

  def generate_specs(ins, defs) do
    listeners = Enum.map(ins, &create_queue_spec/1)
    component_specs = Enum.map(defs, &create_component_spec/1)
    Supervisor.Spec.supervise(component_specs ++ listeners, strategy: :one_for_all)
  end

  defp create_queue_spec(in_name) do
    Supervisor.Spec.worker(Sturm.PullCoordinator, [in_name], function: :start_global_link, id: append_to_id(in_name, "_supervisor"))
  end

  defp create_component_spec(component) do
    case component do
      %Sturm.SourceDef{outs: outs, spec: {name, mod, opts}} -> source_def(name,mod,opts,outs)
      %Sturm.SinkDef{in_source: in_source, spec: {name, mod, opts}} -> sink_def(name,mod,opts, in_source)
      %Sturm.WorkerDef{in_source: in_source, outs: outs, spec: {name, mod, opts}} -> worker_def(name, mod, opts, in_source, outs)
    end
  end

  defp map_ins(component) do
    case component do
      %Sturm.SourceDef{} -> []
      %Sturm.SinkDef{in_source: in_source} -> [in_source]
      %Sturm.WorkerDef{in_source: in_source} -> [in_source]
    end
  end

  defp map_outs(component) do
    case component do
      %Sturm.SourceDef{outs: outs} -> outs
      %Sturm.SinkDef{} -> []
      %Sturm.WorkerDef{outs: outs} -> outs
    end
  end

  def source(name, mod, opts) do
    outs = Enum.map(Keyword.get(opts, :outs, []), fn(x) -> topo_queue_name(x) end)
    %Sturm.SourceDef{outs: outs, spec: {name, mod, opts}}
  end

  def sink(name, mod, opts) do
    in_source = topo_queue_name(name)
    %Sturm.SinkDef{in_source: in_source, spec: {name, mod, opts}}
  end

  def worker(name, mod, opts) do
    outs = Enum.map(Keyword.get(opts, :outs, []), fn(x) -> topo_queue_name(x) end)
    in_source = topo_queue_name(name)
    %Sturm.WorkerDef{in_source: in_source, outs: outs, spec: {name, mod, opts}}
  end

  defp worker_def(name, mod, opts, in_source, emit_destination) do
    child_specs = worker_specs(name, mod, opts, in_source, emit_destination)
    id = worker_supervisor_name(name)
    Supervisor.Spec.supervisor(Sturm.WorkerSupervisor, [id, child_specs], id: id)
  end

  defp sink_def(name, mod, opts, in_source) do
    child_specs = sink_specs(name, mod, opts, in_source)
    id = worker_supervisor_name(name)
    Supervisor.Spec.supervisor(Sturm.WorkerSupervisor, [id, child_specs], id: id)
  end

  defp source_def(name, mod, opts, emit_destination) do
    child_specs = source_specs(name, mod, opts, emit_destination)
    id = worker_supervisor_name(name)
    Supervisor.Spec.supervisor(Sturm.WorkerSupervisor, [id, child_specs], id: id)
  end

  defp sink_specs(name, mod, opts, in_source) do
    count = Keyword.get(opts, :workers, :single)
    in_name = {:global, in_source}
    args = %Sturm.WorkerConfig{:options => opts, :in_source => in_name}
    monitor_kind = Keyword.get(opts, :monitor_as, :worker)
    generate_specs_for_count(count, name, mod, args, monitor_kind)
  end

  defp source_specs(name, mod, opts, out_destination) do
    count = Keyword.get(opts, :workers, :single)
    out_names = Enum.map(out_destination, fn(x) -> {:global, x} end)
    args = %Sturm.WorkerConfig{:out_coordinators => out_names, :options => opts}
    monitor_kind = Keyword.get(opts, :monitor_as, :worker)
    generate_specs_for_count(count, name, mod, args, monitor_kind)
  end

  defp worker_specs(name, mod, opts, in_source, out_destination) do
    count = Keyword.get(opts, :workers, :single)
    in_name = {:global, in_source}
    out_names = Enum.map(out_destination, fn(x) -> {:global, x} end)
    args = %Sturm.WorkerConfig{:out_coordinators => out_names, :options => opts, :in_source => in_name}
    monitor_kind = Keyword.get(opts, :monitor_as, :worker)
    generate_specs_for_count(count, name, mod, args, monitor_kind)
  end

  defp generate_specs_for_count(count, name, mod, args, monitor_kind) do
    case count do
      :single -> [generate_spec_for(mod, Map.merge(args, %{:worker_id => name}), name, monitor_kind)]
      _ -> Enum.map(Range.new(1, count), fn(x) -> 
            worker_id = worker_with_count(name, x)
            generate_spec_for(mod, Map.merge(args, %{:worker_id => worker_id}), name, monitor_kind)
           end)
    end
  end

  defp generate_spec_for(mod, args, name, monitor_kind) do
    case monitor_kind do
     :supervisor -> Supervisor.Spec.supervisor(mod, [args], id: name, shutdown: :brutal_kill)
     _ -> Supervisor.Spec.worker(mod, [args], id: name, shutdown: :brutal_kill)
    end
  end 

  defp topo_queue_name(name) do
    append_to_id(name, "_topoqueue")
  end

  defp worker_supervisor_name(name) do
    append_to_id(name, "_supervisor")
  end

  defp worker_with_count(name, n) do
    append_to_id(name, "_" <> Integer.to_string(n))
  end

  defp append_to_id(name, n) when is_atom(name) do
    String.to_atom(Atom.to_string(name) <> n)
  end

  defp append_to_id(name, n) when is_binary(name) do
    String.to_atom(name <> n)
  end

end
