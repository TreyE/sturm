defmodule Sturm do
  use Application

  def start(_,_) do
    child_def = Supervisor.Spec.worker(Sturm.CoordinatorEtsBackup, [], [])
    Supervisor.start_link([child_def], strategy: :one_for_one)
  end  
end
