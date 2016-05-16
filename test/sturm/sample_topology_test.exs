defmodule Sturm.SampleTopologyTest do
  use ExUnit.Case

  import Sturm.TopologyDsl

  test "a sample topology dsl" do
    my_topo = topology([
      source(:read_db_record, TestDbSource, workers: :single),
      worker(:enrich_db_record, TestDbEnricher, workers: :single),
      sink(:store_result_record, TestDbSink, workers: 6)
    ])
  end
end
