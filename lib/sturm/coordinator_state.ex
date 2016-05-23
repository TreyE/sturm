defmodule Sturm.CoordinatorState do
  defstruct [:namespec, :tablespec, :outstanding_tablespec, workers: :queue.new()]

  def from_namespec(namespec) do
    cleaned_ns = clean_name(namespec)
    out_ts = outstanding_tablespec_name(cleaned_ns)
    %Sturm.CoordinatorState{namespec: namespec, tablespec: cleaned_ns, outstanding_tablespec: out_ts}
  end

  defp clean_name({:global, name}), do: name

  defp outstanding_tablespec_name(name) when is_atom(name) do
    String.to_atom(Atom.to_string(name) <> "_outstanding")
  end

  defp outstanding_tablespec_name(name) when is_binary(name) do
    String.to_atom(name <> "_outstanding")
  end
end
