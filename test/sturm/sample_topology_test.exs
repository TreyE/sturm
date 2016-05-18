defmodule Sturm.SampleTopologyTest do
  use ExUnit.Case

  import Sturm.TopologyDsl

  test "a sample topology dsl" do
    {:ok, _} = topology([
      source(:read_db_record, TestDbSource, workers: :single, outs: [:enrich_db_record], monitor_as: :supervisor),
      worker(:enrich_db_record, TestDbEnricher, workers: :single, outs: [:store_result_record]),
      sink(:store_result_record, TestDbSink, workers: 6)
    ])
  end

  test "a topology with mismatching ins and outs" do
    {:error, _} = topology([
      source(:read_db_record, TestDbSource, workers: :single, outs: [:enrich_db]),
      worker(:enrich_db_record, TestDbEnricher, workers: :single, outs: [:store_result_record]),
      sink(:store_result_record, TestDbSink, workers: 6)
    ])
  end
end
