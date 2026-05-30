from dabi_ingest.pipelines import _fdns_common as fc


def test_ddl_records_targets_scratch_and_partitions_monthly():
    ddl = fc.DDL_RECORDS
    assert "CREATE TABLE IF NOT EXISTS dabi.dns_records" in ddl
    assert "storage_policy = 'scratch'" in ddl
    assert "PARTITION BY toYYYYMM(observed_date)" in ddl
    assert "ORDER BY (apex, query_type, response)" in ddl
    assert "TTL observed_date + INTERVAL" in ddl
    # idempotent DELETE must be allowed despite the reverse-pivot projection
    assert "lightweight_mutation_projection_mode = 'rebuild'" in ddl


def test_delete_source_day_sql_scopes_to_source_and_date():
    sql = fc.build_delete_source_day_sql("li", "2026-05-29")
    assert sql == (
        "DELETE FROM dabi.dns_records WHERE source = 'li' AND observed_date = toDate('2026-05-29')"
    )


def test_ddl_projection_orders_by_response_for_reverse_pivots():
    assert "proj_by_response" in fc.DDL_PROJECTION
    assert "ORDER BY (response, query_type, apex)" in fc.DDL_PROJECTION


def test_ddl_current_is_replacing_mergetree():
    ddl = fc.DDL_CURRENT
    assert "CREATE TABLE IF NOT EXISTS dabi.dns_current" in ddl
    assert "ReplacingMergeTree(last_seen)" in ddl
    assert "storage_policy = 'scratch'" in ddl


def test_parse_sources_from_listing_html():
    html = (
        '<a href="/download/forward-dns/basis=toplist/source=tranco">x</a>'
        '<a href="/download/forward-dns/basis=toplist/source=umbrella">y</a>'
        '<a href="/download/forward-dns/basis=toplist">parent</a>'
    )
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
        "https://openintel.nl/download/forward-dns/basis=zonefile/source=li/year%3D2026/month%3D05/day%3D29/"
    )


def test_build_insert_sql_maps_value_columns_and_filters():
    sql = fc.build_insert_sql(
        part_url="https://object.openintel.nl/openintel-public/fdns/basis=zonefile/"
        "source=li/year=2026/month=05/day=29/part-x.gz.parquet",
        basis="zonefile",
        source="li",
        date="2026-05-29",
    )
    # apex derivation + trailing-dot strip
    assert "cutToFirstSignificantSubdomain" in sql
    # value normalization per response_type
    assert "response_type = 'A'" in sql and "ip4_address" in sql
    assert "ns_address" in sql and "mx_address" in sql
    # only the record types we care about
    assert "response_type IN ('A','AAAA','NS','MX','CNAME','DNAME','TXT','SOA','DS','DNSKEY')" in sql
    # drops empty-value rows
    assert "response != ''" in sql
    # literal partition columns
    assert "'zonefile'" in sql and "'li'" in sql and "toDate('2026-05-29')" in sql
    assert "url('https://object.openintel.nl/openintel-public/fdns/basis=zonefile/" in sql


def test_build_current_upsert_sql_uses_date_and_min_first_seen():
    sql = fc.build_current_upsert_sql("2026-05-29")
    assert "INSERT INTO dabi.dns_current" in sql
    assert "FROM dabi.dns_records" in sql
    assert "observed_date = toDate('2026-05-29')" in sql
    assert "GROUP BY apex, query_type, response" in sql


def test_enrich_doc_groups_records_into_os_fields():
    rows = [
        ("A", "64.190.63.222"),
        ("A", "64.190.63.223"),
        ("AAAA", "2001:db8::1"),
        ("NS", "ns1.sedoparking.com"),
        ("NS", "ns2.sedoparking.com"),
        ("MX", "mail.example.li"),
        ("DNSKEY", "8"),
    ]
    doc = fc.build_enrich_doc("coinstrader24.li", rows, "2026-05-29")
    assert sorted(doc["a_records"]) == ["64.190.63.222", "64.190.63.223"]
    assert doc["aaaa_records"] == ["2001:db8::1"]
    assert sorted(doc["nameservers"]) == ["ns1.sedoparking.com", "ns2.sedoparking.com"]
    assert doc["ns_apex"] == "sedoparking.com"
    assert doc["has_dnssec"] is True
    assert doc["mx_records"] == ["mail.example.li"]
    assert doc["record_count"] == 7
    assert doc["snapshot_date"] == "2026-05-29"


def test_enrich_doc_no_dnssec_when_absent():
    doc = fc.build_enrich_doc("x.li", [("A", "1.2.3.4")], "2026-05-29")
    assert doc["has_dnssec"] is False
    assert doc["nameservers"] == []
    assert doc["ns_apex"] == ""


def test_add_fdns_args_sets_expected_defaults():
    import argparse

    p = argparse.ArgumentParser()
    fc.add_fdns_args(p, default_sources=["li", "se"])
    ns = p.parse_args([])
    assert ns.sources is None  # None => auto-discover
    assert ns.look_back == 7
    assert ns.disk_max_pct == 85
    assert ns.skip_opensearch is False
    assert ns.force is False
    ns2 = p.parse_args(["--sources", "li", "--skip-opensearch"])
    assert ns2.sources == ["li"]
    assert ns2.skip_opensearch is True
