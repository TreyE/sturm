defmodule Sturm.PullCoordinatorGlobalNaming do
  def register_name(name, pid) do
    :global.register_name(name, pid, &Sturm.PullCoordinator.merge_coordinators/3)
  end

  def unregister_name(name) do
    :global.unregister_name(name)
  end

  def whereis_name(name) do
    :global.whereis_name(name)
  end

  def send(name, msg) do
    :global.send(name, msg)
  end
end
