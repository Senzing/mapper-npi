"""Tests for the identifier mapping of a provider record (npi_mapper.map_npi): the EIN as the canonical TAX_ID and the
de-duplication of the payload taxonomy groups."""

import json

import pytest

from conftest import provider_row

EIN_COLUMN = "Employer Identification Number (EIN)"


def tax_ids(record):
    return [f for f in record["FEATURES"] if "TAX_ID_NUMBER" in f]


@pytest.mark.parametrize("ein", ["12-3456789", "123456789", " 12-3456789 "])
def test_organization_ein_is_a_canonical_tax_id(mapper, ein):
    row = provider_row()
    row[EIN_COLUMN] = ein
    produced = json.loads(mapper.map_npi(row))
    assert tax_ids(produced) == [{"TAX_ID_TYPE": "EIN", "TAX_ID_NUMBER": ein.strip(), "TAX_ID_COUNTRY": "US"}]


@pytest.mark.parametrize(
    "ein",
    [
        "<UNAVAIL>",
        "=========",
        "",
        "NONE",
        "12-34567",
        "1234567890",
        "12-345678A",
        "\u0661\u0662-\u0663\u0664\u0665\u0666\u0667\u0668\u0669",
    ],
)
def test_ein_non_values_are_skipped(mapper, ein):
    row = provider_row()
    row[EIN_COLUMN] = ein
    assert not tax_ids(json.loads(mapper.map_npi(row)))


def test_ein_only_on_organizations(mapper):
    row = provider_row()
    row[EIN_COLUMN] = "12-3456789"
    row["Entity Type Code"] = "1"
    assert not tax_ids(json.loads(mapper.map_npi(row)))


def test_taxonomy_group_deduplicated_by_value(mapper):
    row = provider_row()
    row["Healthcare Provider Taxonomy Group_1"] = "193200000X MULTI-SPECIALTY GROUP"
    row["Healthcare Provider Taxonomy Group_2"] = "193200000X MULTI-SPECIALTY GROUP"
    row["Healthcare Provider Taxonomy Group_3"] = "193400000X SINGLE SPECIALTY GROUP"
    produced = json.loads(mapper.map_npi(row))
    groups = {k: v for k, v in produced.items() if k.startswith("Taxonomy Group_")}
    assert groups == {
        "Taxonomy Group_1": "193200000X MULTI-SPECIALTY GROUP",
        "Taxonomy Group_3": "193400000X SINGLE SPECIALTY GROUP",
    }


def test_missing_ein_column(mapper):
    row = provider_row()
    del row[EIN_COLUMN]
    assert not tax_ids(json.loads(mapper.map_npi(row)))
