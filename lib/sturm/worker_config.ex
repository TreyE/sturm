defmodule Sturm.WorkerConfig do
  defstruct [{:in_source, nil}, {:out_coordinators ,[]}, {:options, []}, {:worker_id, nil}]
end
