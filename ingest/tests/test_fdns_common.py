from dabi_ingest.pipelines import _fdns_common as fc


def test_ddl_records_targets_scratch_and_partitions_monthly():
    ddl = fc.DDL_RECORDS
    assert "CREATE TABLE IF NOT EXISTS dabi.dns_records" in ddl
    assert "storage_policy = 'scratch'" in ddl
    assert "PARTITION BY toYYYYMM(observed_date)" in ddl
    assert "ORDER BY (apex, query_type, response)" in ddl
    assert "TTL observed_date + INTERVAL" in ddl


def test_ddl_projection_orders_by_response_for_reverse_pivots():
    assert "proj_by_response" in fc.DDL_PROJECTION
    assert "ORDER BY (response, query_type, apex)" in fc.DDL_PROJECTION


def test_ddl_current_is_replacing_mergetree():
    ddl = fc.DDL_CURRENT
    assert "CREATE TABLE IF NOT EXISTS dabi.dns_current" in ddl
    assert "ReplacingMergeTree(last_seen)" in ddl
    assert "storage_policy = 'scratch'" in ddl
