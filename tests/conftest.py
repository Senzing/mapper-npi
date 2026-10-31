"""Shared fixtures: npi_mapper backed by EMPTY in-memory reference tables, and the verbatim NPPES fixture row."""

import csv
import io
import os
import sqlite3
import sys

import pytest

SRC_DIR = os.path.join(os.path.dirname(os.path.dirname(os.path.abspath(__file__))), "src")
FIXTURES = os.path.join(os.path.dirname(os.path.abspath(__file__)), "fixtures")
sys.path.insert(0, SRC_DIR)


@pytest.fixture(name="mapper")
def fixture_mapper():
    """npi_mapper with the globals its __main__ block would set, backed by EMPTY reference tables."""
    # npi_mapper imports pandas at module level; only this fixture needs it, so import it here.
    # pylint: disable-next=import-outside-toplevel
    import npi_mapper

    conn = sqlite3.connect(":memory:")
    conn.execute(
        'create table OTHERNAME (NPI, "Provider Other Organization Name", "Provider Other Organization Name Type Code")'
    )
    pl_cols = [
        "Provider Secondary Practice Location Address- Address Line 1",
        "Provider Secondary Practice Location Address-  Address Line 2",
        "Provider Secondary Practice Location Address - City Name",
        "Provider Secondary Practice Location Address - State Name",
        "Provider Secondary Practice Location Address - Postal Code",
        "Provider Secondary Practice Location Address - Country Code (If outside U.S.)",
        "Provider Secondary Practice Location Address - Telephone Number",
        "Provider Practice Location Address - Fax Number",
    ]
    conn.execute("create table PL (NPI, %s)" % ", ".join('"%s"' % c for c in pl_cols))
    ep_cols = [
        "Affiliation",
        "Endpoint",
        "Affiliation Legal Business Name",
        "Affiliation Address Line One",
        "Affiliation Address Line Two",
        "Affiliation Address City",
        "Affiliation Address State",
        "Affiliation Address Country",
        "Affiliation Address Postal Code",
    ]
    conn.execute("create table ENDPOINT (NPI, %s)" % ", ".join('"%s"' % c for c in ep_cols))
    officials = io.StringIO()
    locations = io.StringIO()
    settings = {
        "conn": conn,
        "statPack": {},
        "Officials_outFile": officials,
        "Locations_outFile": locations,
        "JSON_row_count": 0,
        "NPIOfficials_row_count": 0,
        "NPILocations_row_count": 0,
        "idValuesToIgnore": {"=========": True, "NONE": True},
    }
    for name, value in settings.items():
        setattr(npi_mapper, name, value)
    yield npi_mapper
    conn.close()


def provider_row():
    """The verbatim NPPES row for NPI 1063947125 (an organization), as csv.DictReader yields it."""
    with open(os.path.join(FIXTURES, "npidata_1063947125.csv"), encoding="utf-8") as handle:
        return next(csv.DictReader(handle))
