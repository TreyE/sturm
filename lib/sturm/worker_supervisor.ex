defmodule Sturm.WorkerSupervisor do
  use Supervisor

  def start_link(arg) do
    Supervisor.start_link(__MODULE__, arg)
  end

  def init(arg) do
    children = Keyword.get(arg, :worker_specs, []) 
    id = Keyword.get(arg, :id, __MODULE__) 
    supervise(children, id: id, strategy: :one_for_one)
  end
end
