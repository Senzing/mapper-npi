"""Tests for the secondary practice locations carried on the provider (npi_mapper.map_locations / map_npi).

The provider below is a verbatim NPPES row (tests/fixtures/npidata_1063947125.csv); the pl_pfile rows are inserted into the
in-memory PL table the mapper reads.
"""

import json

from conftest import provider_row

PL_ROWS = [
    # a new place for the provider
    ("1063947125", "6855 Wilson Blvd", "Ste 2", "Jacksonville", "FL", "322103600", "", "5615095009", ""),
    # the provider's own practice address (must not be repeated), same street/ZIP5 spelled differently
    ("1063947125", "2 n. light st", "", "Lovettsville", "VA", "20180", "", "5551230000", ""),
    # the same new place again with another phone: one address on the provider, two location records
    ("1063947125", "6855 Wilson Blvd", "Ste 2", "Jacksonville", "FL", "322103600", "", "9045550000", "9045551111"),
]


def test_secondary_locations_become_provider_addresses_and_phones(mapper):
    """Each secondary location's address and phone numbers are on the provider (ADDR_TYPE SECONDARY), once, not repeated."""
    mapper.conn.executemany("insert into PL values (?,?,?,?,?,?,?,?,?)", PL_ROWS)
    produced = json.loads(mapper.map_npi(provider_row()))
    secondary = [f for f in produced["FEATURES"] if f.get("ADDR_TYPE") == "SECONDARY"]
    assert secondary == [
        {
            "ADDR_TYPE": "SECONDARY",
            "ADDR_LINE1": "6855 Wilson Blvd",
            "ADDR_LINE2": "Ste 2",
            "ADDR_CITY": "Jacksonville",
            "ADDR_STATE": "FL",
            "ADDR_POSTAL_CODE": "322103600",
        }
    ]
    # the provider's own addresses come first, so a consumer reading "the first BUSINESS address" is unchanged
    types = [f["ADDR_TYPE"] for f in produced["FEATURES"] if "ADDR_TYPE" in f]
    assert types.index("SECONDARY") > max(i for i, t in enumerate(types) if t != "SECONDARY")
    # every location's telephone is on the provider, each number once (also for the location at the provider's own address)
    phones = [f["PHONE_NUMBER"] for f in produced["FEATURES"] if "PHONE_NUMBER" in f and "PHONE_TYPE" not in f]
    for number in ("5615095009", "5551230000", "9045550000"):
        assert phones.count(number) == 1
    assert [f for f in produced["FEATURES"] if f.get("PHONE_TYPE") == "FAX" and f["PHONE_NUMBER"] == "9045551111"]
    # no nameless location records by default
    assert mapper.Locations_outFile.getvalue() == ""


def test_location_records_only_with_the_flag(mapper):
    mapper.conn.executemany("insert into PL values (?,?,?,?,?,?,?,?,?)", PL_ROWS)
    mapper.EMIT_LOCATION_RECORDS = True
    try:
        mapper.map_npi(provider_row())
    finally:
        mapper.EMIT_LOCATION_RECORDS = False
    lines = [json.loads(x) for x in mapper.Locations_outFile.getvalue().splitlines()]
    assert len(lines) == len(PL_ROWS)
    assert all(r["DATA_SOURCE"] == "NPI-LOCATIONS" for r in lines)


def test_address_identity_and_dedupe(mapper):
    ident = mapper.address_identity
    assert ident({"ADDR_LINE1": "44 S. Kidwell Ave", "ADDR_POSTAL_CODE": "20180-1234"}) == "44SKIDWELLAVE|20180"
    assert ident({"ADDR_CITY": "Nowhere"}) is None
    provider = [{"ADDR_TYPE": "BUSINESS", "ADDR_LINE1": "44 S KIDWELL AVE", "ADDR_POSTAL_CODE": "201801234"}]
    locations = [
        {"ADDR_TYPE": "SECONDARY", "ADDR_LINE1": "44 S Kidwell Ave.", "ADDR_POSTAL_CODE": "20180"},
        {"ADDR_TYPE": "SECONDARY", "ADDR_LINE1": "1 Main St", "ADDR_POSTAL_CODE": "20176"},
        {"ADDR_TYPE": "SECONDARY", "ADDR_LINE1": "1 MAIN ST", "ADDR_POSTAL_CODE": "20176-0001"},
    ]
    assert mapper.secondary_addresses(locations, provider) == [locations[1]]
