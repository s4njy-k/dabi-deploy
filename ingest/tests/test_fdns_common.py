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


def test_parse_sources_from_listing_html():
    html = '<a href="/download/forward-dns/basis=toplist/source=tranco">x</a>' \
           '<a href="/download/forward-dns/basis=toplist/source=umbrella">y</a>' \
           '<a href="/download/forward-dns/basis=toplist">parent</a>'
    assert fc.parse_sources(html) == ["tranco", "umbrella"]


def test_parse_parts_from_leaf_html_keeps_only_object_parquet():
    html = (
        'junk <a href="https://object.openintel.nl/openintel-public/fdns/'
        'basis=zonefile/source=li/year=2026/month=05/day=29/part-00002-abc.c000.gz.parquet">f</a> '
        '<a href="/download/forward-dns/basis=zonefile/source=li">nav</a>'
    )
    parts = fc.parse_parts(html)
    assert parts == [
        "https://object.openintel.nl/openintel-public/fdns/basis=zonefile/"
        "source=li/year=2026/month=05/day=29/part-00002-abc.c000.gz.parquet"
    ]


def test_listing_url_for_builds_encoded_path():
    url = fc.listing_url_for("zonefile", "li", 2026, 5, 29)
    assert url == (
        "https://openintel.nl/download/forward-dns/basis=zonefile/source=li/"
        "year%3D2026/month%3D05/day%3D29/"
    )
