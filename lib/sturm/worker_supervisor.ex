defmodule Sturm.WorkerSupervisor do
  use Supervisor

  def start_link(s_id, arg) do
    Supervisor.start_link(__MODULE__, arg, id: s_id)
  end

  def init(arg) do
    supervise(arg, strategy: :one_for_one)
  end
end
