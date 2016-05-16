defmodule Sturm.PullWorkerDefinition do
  @type t :: %Sturm.PullWorkerDefinition{module: module, namespec: GenServer.name}
  defstruct [:module, :namespec]

  @spec cast_worker(t, GenServer.name, any) :: any
  def cast_worker(workerspec, coordinator_ns, request) do
    cb_mod = workerspec.module
    ns = workerspec.namespec 
    cb_mod.cast_request(ns, coordinator_ns, request)
  end
end
