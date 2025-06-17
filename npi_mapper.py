#! /usr/bin/env python3
#
# NPPES_ToJSON.py - New Program
#     Based on Butcher's example for us-small-employee-raw.csv and my UMF generator
#
# Assumptions:
#   ALL files are sorted by NPI Ascending prior to running this
#   File names are standard with only the period portion different
#
# Processing:
#   All Othernames are loaded as DBA, FORMER, or OTHER name types as part of the NPI record
#   All Secondary locations are loaded as different source, and can be multiple rows per NPI (appended -# to end of Record ID)
#   All Addresses and Email Addresses are loaded as a different source, and there can be multiple rows per NPI.   Each row that had both Email and Address were loaded into 1 identity record
#
# Sources Generated:
#   NPI-PROVIDERS   -  Main NPI data with Othernames included
#                   -  RECORD_ID: NPI
#   NPI-OFFICIALS   -  Authorized Personnel for the NPI
#   NPI-AFFILIATIONS-  Endpoint data with Email and Address
#                   -  RECORD_ID: NPI-#   where # is 1-n.. incremented for each line for this NPI
#                   -  Anchored back to NPI
#   NPI-LOCATIONS   -  Provider Locations date with address & Phone for these secondary locations
#                   -  RECORD_ID: NPI-#   where # is 1-n.. incremented for each line for this NPI
#                   -  Anchored back to NPI
#
# Maintenance Log:
#   v1.0 - 10/20/2020 - Peter Huber
#        - New Prog
#   v1.1 - 10/22/2020 - Peter Huber
#        - checked for existing values before creating Key/Value pairs
#        - Corrected de-duplication of Taxonomy Group Codes
#        - Added/Changed checks so that empty lists are not added to JSON
#        - Added total runtime messages
#        - Incorporated Butcher's change suggestions (except for IDs)
#   v1.2 - 10/23/2020 - Peter Huber
#        - Changed to process Provider Locations file with NPI file to capture the primary name for the NPI-LOCATIONS
#   v1.3 - 10/23/2020 - Peter Huber
#        - Ignoring Provider License IDs = "========="
#   v1.4 - 10/28/2020 - Peter Huber
#        - Stopped creating a NPI-LOCATION for the same address or phone for the same NPI
#   v1.5 - 10/30/2020 - Peter Huber
#        - Ignoring Provider License IDs & Other Provider IDs = "========="
#        - Ignoring Provider License IDs & Other Provider IDs = PENDING
#        - Ignoring Provider License IDs & Other Provider IDs = NA
#        - Ignoring Provider License IDs & Other Provider IDs = ENROLLED
#        - Ignoring Provider License IDs & Other Provider IDs = NONE
#        - Ignore any addr2 lines = NONE
#        - Ignore any Names = NONE'
#   v1.6 - 10/30/2020 - Peter Huber
#        - Changed code to leverage in memory DB vs processing reference input files at the same time
#        - Split output into 4 files.  1 for each generated source
#        - Changed -o parameter to be a output directory where the 4 files will be created
#   v1.7 - 11/20/2020 - Jeff Butcher
#        - remapped licenses and identifiers to their own features
#        - captured the websites and emails from non-affiliate affiliation records and added to NPI provider
#        - standardized the parameters to other senzing mappers
#   v1.8 - 11/30/2020 - Jeff Butcher
#        - removed all name defaulting on locations, turns ou its not reliable at all
#
# ----------------------------------------------------------------------------------------------------
import csv
import json
import argparse
import datetime
import time
import os
import sys
import pandas
import sqlite3
import signal
import random


# -------------------------------------------------------------
#  Map Provider Locations Reference file for this NPI
# -------------------------------------------------------------
def map_locations(inNPI, inName, inType):
    global NPILocations_row_count
    global JSON_row_count

    cntr = 0

    sql = "select distinct "
    sql += ' "Provider Secondary Practice Location Address- Address Line 1"                  as ADDR1,'
    sql += ' "Provider Secondary Practice Location Address-  Address Line 2"                 as ADDR2,'
    sql += ' "Provider Secondary Practice Location Address - City Name"                      as CITY,'
    sql += ' "Provider Secondary Practice Location Address - State Name"                     as STATE,'
    sql += ' "Provider Secondary Practice Location Address - Postal Code"                    as POSTAL_CODE,'
    sql += ' "Provider Secondary Practice Location Address - Country Code (If outside U.S.)" as COUNTRY,'
    sql += ' "Provider Secondary Practice Location Address - Telephone Number"               as PH1,'
    sql += ' "Provider Practice Location Address - Fax Number"                               as PH2'
    sql += " from PL where pl.NPI = '" + str(inNPI) + "'"

    plObj = conn.cursor()
    cursor1 = plObj.execute(sql)
    hdr1 = [col[0] for col in plObj.description]
    resultRow = cursor1.fetchone()
    while resultRow:
        cntr += 1
        rsltRecord = dict(zip(hdr1, resultRow))

        loc_data = {}
        loc_data["DATA_SOURCE"] = "NPI-LOCATIONS"
        loc_data["RECORD_ID"] = str(inNPI) + "-" + str(cntr)
        loc_data["RECORD_TYPE"] = "ORGANIZATION"
        updateStat("DATA_SOURCES", loc_data["DATA_SOURCE"])
        updateStat(loc_data["DATA_SOURCE"], loc_data["RECORD_TYPE"])

        if (
            False
        ):  # --cannot reliably say this is the name of the organization at that location
            if inType == "1":
                loc_data["PRIMARY_NAME_FULL"] = inName
                updateStat(loc_data["DATA_SOURCE"], "NAME_FULL(PERSON)", inName)
            else:
                loc_data["PRIMARY_NAME_ORG"] = inName
                updateStat(loc_data["DATA_SOURCE"], "NAME_ORG(ORGANIZATION)", inName)

        if rsltRecord["ADDR1"]:
            updateStat(loc_data["DATA_SOURCE"], "ADDR_LINE1", rsltRecord["ADDR1"])
            loc_data["BUSINESS_ADDR_LINE1"] = rsltRecord["ADDR1"]
            if rsltRecord["ADDR2"] and rsltRecord["ADDR2"] != "NONE":
                updateStat(loc_data["DATA_SOURCE"], "ADDR_LINE2", rsltRecord["ADDR2"])
                loc_data["BUSINESS_ADDR_LINE2"] = rsltRecord["ADDR2"]
            loc_data["BUSINESS_ADDR_CITY"] = rsltRecord["CITY"]
            loc_data["BUSINESS_ADDR_STATE"] = rsltRecord["STATE"]
            loc_data["BUSINESS_ADDR_POSTAL_CODE"] = rsltRecord["POSTAL_CODE"]
            loc_data["BUSINESS_ADDR_COUNTRY"] = rsltRecord["COUNTRY"]

        if rsltRecord["PH1"]:
            updateStat(loc_data["DATA_SOURCE"], "PHONE", rsltRecord["PH1"])
            loc_data["PHONE_NUMBER"] = rsltRecord["PH1"]
        if rsltRecord["PH2"]:
            updateStat(loc_data["DATA_SOURCE"], "FAX", rsltRecord["PH2"])
            loc_data["FAX_PHONE_NUMBER"] = rsltRecord["PH2"]

        # Disclose rel to NPI
        loc_data["REL_POINTER_DOMAIN"] = "NPI"
        loc_data["REL_POINTER_KEY"] = inNPI
        loc_data["REL_POINTER_ROLE"] = "Secondary Location"

        Locations_outFile.write(json.dumps(loc_data) + "\n")
        JSON_row_count += 1
        NPILocations_row_count += 1

        resultRow = cursor1.fetchone()


#
# -------------------------------------------------------------
#  Map Endpoint Reference file for this NPI
# -------------------------------------------------------------
def map_endpoints(inNPI):
    global NPIAffiliations_row_count
    global JSON_row_count

    endpointList = (
        []
    )  # --jb: for emails and websites that belong to the NPI, not affiliates

    cntr = 0

    sql = "select distinct "
    sql += ' "Affiliation"                      as IS_AFFILIATE,'  # --jb: added
    sql += ' "Endpoint"                         as ENDPOINT,'
    sql += ' "Affiliation Legal Business Name"  as NAME_ORG,'  # --jb: added
    sql += ' "Affiliation Address Line One"     as ADDR1,'
    sql += ' "Affiliation Address Line Two"     as ADDR2,'
    sql += ' "Affiliation Address City"         as CITY,'
    sql += ' "Affiliation Address State"        as STATE,'
    sql += ' "Affiliation Address Country"      as COUNTRY,'
    sql += ' "Affiliation Address Postal Code"  as POSTAL_CODE'
    sql += " From ENDPOINT where NPI = '" + str(inNPI) + "'"

    epObj = conn.cursor()
    cursor1 = epObj.execute(sql)
    hdr1 = [col[0] for col in epObj.description]
    resultRow = cursor1.fetchone()
    while resultRow:
        rsltRecord = dict(zip(hdr1, resultRow))
        cntr += 1
        ep_data = {}

        if rsltRecord["IS_AFFILIATE"] == "Y":
            ep_data["DATA_SOURCE"] = "NPI-AFFILIATIONS"
            ep_data["RECORD_ID"] = str(inNPI) + "-" + str(cntr)
            ep_data["RECORD_TYPE"] = "ORGANIZATION"
            updateStat("DATA_SOURCES", ep_data["DATA_SOURCE"])
            updateStat(ep_data["DATA_SOURCE"], ep_data["RECORD_TYPE"])

            # --jb: added name org
            if rsltRecord["NAME_ORG"]:
                updateStat(ep_data["DATA_SOURCE"], "NAME", rsltRecord["NAME_ORG"])
                ep_data["PRIMARY_NAME_ORG"] = rsltRecord["NAME_ORG"]
            else:
                updateStat(ep_data["DATA_SOURCE"], "MISSING_NAME", ep_data["RECORD_ID"])

            if rsltRecord["ADDR1"]:
                updateStat(ep_data["DATA_SOURCE"], "ADDR_LINE1", rsltRecord["ADDR1"])
                ep_data["BUSINESS_ADDR_LINE1"] = rsltRecord["ADDR1"]
                if rsltRecord["ADDR2"] and rsltRecord["ADDR2"] != "NONE":
                    updateStat(
                        ep_data["DATA_SOURCE"], "ADDR_LINE2", rsltRecord["ADDR2"]
                    )
                    ep_data["BUSINESS_ADDR_LINE2"] = rsltRecord["ADDR2"]
                ep_data["BUSINESS_ADDR_CITY"] = rsltRecord["CITY"]
                ep_data["BUSINESS_ADDR_STATE"] = rsltRecord["STATE"]
                ep_data["BUSINESS_ADDR_POSTAL_CODE"] = rsltRecord["POSTAL_CODE"]
                ep_data["BUSINESS_ADDR_COUNTRY"] = rsltRecord["COUNTRY"]

            # Disclose rel to NPI
            ep_data["REL_POINTER_DOMAIN"] = "NPI"
            ep_data["REL_POINTER_KEY"] = inNPI
            ep_data["REL_POINTER_ROLE"] = "Affiliate"

        # --jb: these could be for the affiliate or for the NPI
        if rsltRecord["ENDPOINT"]:
            if rsltRecord["IS_AFFILIATE"] == "Y":
                logDataSource = ep_data["DATA_SOURCE"]
            else:
                logDataSource = "NPI-PROVIDERS"

            if rsltRecord["ENDPOINT"].find("@") > 0:
                if rsltRecord["IS_AFFILIATE"] == "Y":
                    ep_data["EMAIL_ADDRESS"] = rsltRecord["ENDPOINT"]
                else:
                    endpointList.append({"EMAIL_ADDRESS": rsltRecord["ENDPOINT"]})
                updateStat(logDataSource, "EMAIL_ADDRESS", rsltRecord["ENDPOINT"])

            # --jb: if not email its a website or other url
            else:
                updateStat(logDataSource, "WEBSITE_ADDRESS", rsltRecord["ENDPOINT"])
                if rsltRecord["IS_AFFILIATE"] == "Y":
                    ep_data["WEBSITE_ADDRESS"] = rsltRecord["ENDPOINT"]
                else:
                    endpointList.append({"WEBSITE_ADDRESS": rsltRecord["ENDPOINT"]})

        # --jb: write it out to affiliate file
        if rsltRecord["IS_AFFILIATE"] == "Y":
            Affiliations_outFile.write(json.dumps(ep_data) + "\n")
            JSON_row_count += 1
            NPIAffiliations_row_count += 1

        resultRow = cursor1.fetchone()

    return endpointList


#
# -------------------------------------------------------------
#  Map Othernames for this specific NPI #
# -------------------------------------------------------------
def map_othernames(inNPI):
    oNames_Mapped = {}
    oNames = []

    sql = "select distinct "
    sql += ' "Provider Other Organization Name"            as name1,'
    sql += ' "Provider Other Organization Name Type Code"  as typCd'
    sql += (
        " from OTHERNAME where NPI = '"
        + str(inNPI)
        + "' and \"Provider Other Organization Name\" <> 'NONE'"
    )

    onObj = conn.cursor()
    onCur = onObj.execute(sql)
    hdr1 = [col[0] for col in onObj.description]
    resultRow = onCur.fetchone()
    while resultRow:
        rsltRecord = dict(zip(hdr1, resultRow))
        if rsltRecord["name1"] and rsltRecord["name1"] not in oNames_Mapped:
            oNames_Mapped[rsltRecord["name1"]] = True
            if rsltRecord["typCd"] == "3":  # DBA
                updateStat("NPI-PROVIDER", "NAME-DBA", rsltRecord["name1"])
                oNames.append({"DBA_NAME_ORG": rsltRecord["name1"]})
            elif rsltRecord["typCd"] == "4":  # Former Bus name
                updateStat("NPI-PROVIDER", "NAME-FORMER", rsltRecord["name1"])
                oNames.append({"FORMER_NAME_ORG": rsltRecord["name1"]})
            elif rsltRecord["typCd"] == "5":  # Other
                updateStat("NPI-PROVIDER", "NAME-OTHER", rsltRecord["name1"])
                oNames.append({"OTHER_NAME_ORG": rsltRecord["name1"]})

        resultRow = onCur.fetchone()

    return oNames


#
# -------------------------------------------------------------
#  Map Authorized Official
# -------------------------------------------------------------
def map_auth(input_row, npi_name):
    auth_data = {}

    # --required attributes
    auth_data["DATA_SOURCE"] = "NPI-OFFICIALS"
    auth_data["RECORD_ID"] = input_row["NPI"] + "-AUTH"
    auth_data["RECORD_TYPE"] = "PERSON"
    updateStat("DATA_SOURCES", auth_data["DATA_SOURCE"])
    updateStat(auth_data["DATA_SOURCE"], auth_data["RECORD_TYPE"])

    updateStat(
        auth_data["DATA_SOURCE"],
        "NAME",
        "%s %s"
        % (
            input_row["Authorized Official First Name"],
            input_row["Authorized Official Last Name"],
        ),
    )
    auth_data["PRIMARY_NAME_LAST"] = input_row["Authorized Official Last Name"]
    auth_data["PRIMARY_NAME_FIRST"] = input_row["Authorized Official First Name"]
    if (
        input_row["Authorized Official Middle Name"]
        and input_row["Authorized Official Middle Name"] != "NONE"
    ):
        auth_data["PRIMARY_NAME_MIDDLE"] = input_row["Authorized Official Middle Name"]
    if input_row["Authorized Official Name Prefix Text"]:
        auth_data["PRIMARY_NAME_PREFIX"] = input_row[
            "Authorized Official Name Prefix Text"
        ]
    if input_row["Authorized Official Name Suffix Text"]:
        auth_data["PRIMARY_NAME_SUFFIX"] = input_row[
            "Authorized Official Name Suffix Text"
        ]

    if input_row["Authorized Official Title or Position"]:
        updateStat(
            auth_data["DATA_SOURCE"],
            "TITLE",
            input_row["Authorized Official Title or Position"],
        )
        auth_data["Title or Position"] = input_row[
            "Authorized Official Title or Position"
        ]
        auth_data["Provider Name"] = npi_name

    if input_row["Authorized Official Telephone Number"]:
        updateStat(
            auth_data["DATA_SOURCE"],
            "PHONE",
            input_row["Authorized Official Telephone Number"],
        )
        auth_data["PHONE_NUMBER"] = input_row["Authorized Official Telephone Number"]

    # make disclosed -MODIFY
    auth_data["REL_POINTER_KEY"] = input_row["NPI"]
    auth_data["REL_POINTER_DOMAIN"] = "NPI"
    auth_data["REL_POINTER_ROLE"] = "Authorized Official"

    return json.dumps(auth_data)


#
# -------------------------------------------------------------
#  Maps the root after starting a new JSON row
# -------------------------------------------------------------
def map_npi(input_row):

    global JSON_row_count
    global NPIOfficials_row_count

    json_data = {}

    currNPI = input_row["NPI"]

    # --required attributes
    json_data["DATA_SOURCE"] = "NPI-PROVIDERS"
    json_data["RECORD_ID"] = input_row["NPI"]
    if input_row["Entity Type Code"] == "1":
        json_data["RECORD_TYPE"] = "PERSON"
    else:
        json_data["RECORD_TYPE"] = "ORGANIZATION"
    updateStat("DATA_SOURCES", json_data["DATA_SOURCE"])
    updateStat(json_data["DATA_SOURCE"], json_data["RECORD_TYPE"])

    # --attributes used for resolution
    # NPI (s)
    json_data["NPI_NUMBER"] = input_row["NPI"]
    if input_row["Replacement NPI"]:
        updateStat(json_data["DATA_SOURCE"], "REPL-NPI", input_row["Replacement NPI"])
        json_data["REPL_NPI_NUMBER"] = input_row["Replacement NPI"]

    #  define anchor point for disclosed relationships back to this NPI from NPI-LOCATIONS, NPI-AFFILIATES, and NPI-OFFICIALS
    json_data["REL_ANCHOR_KEY"] = input_row["NPI"]
    json_data["REL_ANCHOR_DOMAIN"] = "NPI"

    # Names
    if input_row["Entity Type Code"] == "1":
        json_data["PRIMARY_NAME_LAST"] = input_row["Provider Last Name (Legal Name)"]
        npi_name = input_row["Provider Last Name (Legal Name)"]
        json_data["PRIMARY_NAME_FIRST"] = input_row["Provider First Name"]
        npi_name = npi_name + ", " + input_row["Provider First Name"]
        updateStat(json_data["DATA_SOURCE"], "NAME_LAST/FIRST-PRIMARY", npi_name)
        if (
            input_row["Provider Middle Name"]
            and input_row["Provider Middle Name"] != "NONE"
        ):
            json_data["PRIMARY_NAME_MIDDLE"] = input_row["Provider Middle Name"]
            npi_name = npi_name + " " + input_row["Provider Middle Name"]
        if input_row["Provider Name Prefix Text"]:
            json_data["PRIMARY_NAME_PREFIX"] = input_row["Provider Name Prefix Text"]
        if input_row["Provider Name Suffix Text"]:
            json_data["PRIMARY_NAME_SUFFIX"] = input_row["Provider Name Suffix Text"]
    else:
        json_data["PRIMARY_NAME_ORG"] = input_row[
            "Provider Organization Name (Legal Business Name)"
        ]
        npi_name = input_row["Provider Organization Name (Legal Business Name)"]
        updateStat(json_data["DATA_SOURCE"], "NAME_ORG-PRIMARY", npi_name)

    if (
        input_row["Provider Other Organization Name"]
        and input_row["Provider Other Organization Name"] != "NONE"
    ):
        if input_row["Provider Other Organization Name Type Code"] == "3":  # DBA
            json_data["DBA_NAME_ORG"] = input_row["Provider Other Organization Name"]
            updateStat(
                json_data["DATA_SOURCE"],
                "NAME_ORG-DBA",
                input_row["Provider Other Organization Name"],
            )
        elif (
            input_row["Provider Other Organization Name Type Code"] == "4"
        ):  # Former Bus name
            json_data["FORMER_NAME_ORG"] = input_row["Provider Other Organization Name"]
            updateStat(
                json_data["DATA_SOURCE"],
                "NAME_ORG-FORMER",
                input_row["Provider Other Organization Name"],
            )
        elif input_row["Provider Other Organization Name Type Code"] == "5":  # Other
            json_data["OTHER_NAME_ORG"] = input_row["Provider Other Organization Name"]
            updateStat(
                json_data["DATA_SOURCE"],
                "NAME_ORG-OTHER",
                input_row["Provider Other Organization Name"],
            )
        else:
            updateStat(
                json_data["DATA_SOURCE"],
                "NAME_ORG-UNKNOWN!",
                input_row["Provider Other Organization Name"],
            )

    if input_row["Provider Other Last Name"] == "NONE":
        input_row["Provider Other Last Name"] = ""
    if input_row["Provider Other First Name"] == "NONE":
        input_row["Provider Other First Name"] = ""
    if input_row["Provider Other Middle Name"] == "NONE":
        input_row["Provider Other Middle Name"] = ""

    if (
        input_row["Provider Other Last Name Type Code"] == "1"
        and input_row["Provider Other Last Name"]
    ):  # Former Name
        json_data["FORMER_NAME_LAST"] = input_row["Provider Other Last Name"]
        json_data["FORMER_NAME_FIRST"] = input_row["Provider Other First Name"]
        updateStat(
            json_data["DATA_SOURCE"],
            "NAME_LAST/FIRST-FORMER",
            "%s, %s"
            % (
                input_row["Provider Other Last Name"],
                input_row["Provider Other First Name"],
            ),
        )
        if input_row["Provider Other Middle Name"]:
            json_data["FORMER_NAME_MIDDLE"] = input_row["Provider Other Middle Name"]
        if input_row["Provider Other Name Prefix Text"]:
            json_data["FORMER_NAME_PREFIX"] = input_row[
                "Provider Other Name Prefix Text"
            ]
        if input_row["Provider Other Name Suffix Text"]:
            json_data["FORMER_NAME_SUFFIX"] = input_row[
                "Provider Other Name Suffix Text"
            ]
    elif (
        input_row["Provider Other Last Name Type Code"] == "2"
        and input_row["Provider Other Last Name"]
    ):  # Professional Name
        json_data["PROFESSIONAL_NAME_LAST"] = input_row["Provider Other Last Name"]
        json_data["PROFESSIONAL_NAME_FIRST"] = input_row["Provider Other First Name"]
        updateStat(
            json_data["DATA_SOURCE"],
            "NAME_LAST/FIRST-PROFESSIONAL",
            "%s, %s"
            % (
                input_row["Provider Other Last Name"],
                input_row["Provider Other First Name"],
            ),
        )
        if input_row["Provider Other Middle Name"]:
            json_data["PROFESSIONAL_NAME_MIDDLE"] = input_row[
                "Provider Other Middle Name"
            ]
        if input_row["Provider Other Name Prefix Text"]:
            json_data["PROFESSIONAL_NAME_PREFIX"] = input_row[
                "Provider Other Name Prefix Text"
            ]
        if input_row["Provider Other Name Suffix Text"]:
            json_data["PROFESSIONAL_NAME_SUFFIX"] = input_row[
                "Provider Other Name Suffix Text"
            ]
    elif (
        input_row["Provider Other Last Name Type Code"] == "3"
        and input_row["Provider Other Last Name"]
    ):  # DBA
        json_data["DBA_NAME_LAST"] = input_row["Provider Other Last Name"]
        json_data["DBA_NAME_FIRST"] = input_row["Provider Other First Name"]
        updateStat(
            json_data["DATA_SOURCE"],
            "NAME_LAST/FIRST-DBA",
            "%s, %s"
            % (
                input_row["Provider Other Last Name"],
                input_row["Provider Other First Name"],
            ),
        )
        if input_row["Provider Other Middle Name"]:
            json_data["DBA_NAME_MIDDLE"] = input_row["Provider Other Middle Name"]
        if input_row["Provider Other Name Prefix Text"]:
            json_data["DBA_NAME_PREFIX"] = input_row["Provider Other Name Prefix Text"]
        if input_row["Provider Other Name Suffix Text"]:
            json_data["DBA_NAME_SUFFIX"] = input_row["Provider Other Name Suffix Text"]
    elif (
        input_row["Provider Other Last Name Type Code"] == "5"
        and input_row["Provider Other Last Name"]
    ):  # Other
        json_data["OTHER_NAME_LAST"] = input_row["Provider Other Last Name"]
        json_data["OTHER_NAME_FIRST"] = input_row["Provider Other First Name"]
        updateStat(
            json_data["DATA_SOURCE"],
            "NAME_LAST/FIRST-OTHER",
            "%s, %s"
            % (
                input_row["Provider Other Last Name"],
                input_row["Provider Other First Name"],
            ),
        )
        if input_row["Provider Other Middle Name"]:
            json_data["OTHER_NAME_MIDDLE"] = input_row["Provider Other Middle Name"]
        if input_row["Provider Other Name Prefix Text"]:
            json_data["OTHER_NAME_PREFIX"] = input_row[
                "Provider Other Name Prefix Text"
            ]
        if input_row["Provider Other Name Suffix Text"]:
            json_data["OTHER_NAME_SUFFIX"] = input_row[
                "Provider Other Name Suffix Text"
            ]
    else:
        updateStat(
            json_data["DATA_SOURCE"],
            "NAME_LAST/FIRST-UNKNOWN!",
            "%s, %s"
            % (
                input_row["Provider Other Last Name"],
                input_row["Provider Other First Name"],
            ),
        )

    # Addresses
    if input_row["Provider First Line Business Mailing Address"]:
        updateStat(
            json_data["DATA_SOURCE"],
            "ADDR_LINE1-MAILING",
            input_row["Provider First Line Business Mailing Address"],
        )
        json_data["MAILING_ADDR_LINE1"] = input_row[
            "Provider First Line Business Mailing Address"
        ]
        if (
            input_row["Provider Second Line Business Mailing Address"]
            and input_row["Provider Second Line Business Mailing Address"] != "NONE"
        ):
            updateStat(
                json_data["DATA_SOURCE"],
                "ADDR_LINE2-MAILING",
                input_row["Provider Second Line Business Mailing Address"],
            )
            json_data["MAILING_ADDR_LINE2"] = input_row[
                "Provider Second Line Business Mailing Address"
            ]
        json_data["MAILING_ADDR_CITY"] = input_row[
            "Provider Business Mailing Address City Name"
        ]
        json_data["MAILING_ADDR_STATE"] = input_row[
            "Provider Business Mailing Address State Name"
        ]
        json_data["MAILING_ADDR_POSTAL_CODE"] = input_row[
            "Provider Business Mailing Address Postal Code"
        ]
        json_data["MAILING_ADDR_COUNTRY"] = input_row[
            "Provider Business Mailing Address Country Code (If outside U.S.)"
        ]

    if input_row["Provider First Line Business Practice Location Address"]:

        if input_row["Entity Type Code"] == "1":
            address_label = "PRIMARY"
        else:
            address_label = "BUSINESS"

        updateStat(
            json_data["DATA_SOURCE"],
            "ADDR_LINE1-" + address_label,
            input_row["Provider First Line Business Practice Location Address"],
        )
        json_data[address_label + "_ADDR_LINE1"] = input_row[
            "Provider First Line Business Practice Location Address"
        ]
        if (
            input_row["Provider Second Line Business Practice Location Address"]
            and input_row["Provider Second Line Business Practice Location Address"]
            != "NONE"
        ):
            updateStat(
                json_data["DATA_SOURCE"],
                "ADDR_LINE2-" + address_label,
                input_row["Provider Second Line Business Practice Location Address"],
            )
            json_data[address_label + "_ADDR_LINE2"] = input_row[
                "Provider Second Line Business Practice Location Address"
            ]
        json_data[address_label + "_ADDR_CITY"] = input_row[
            "Provider Business Practice Location Address City Name"
        ]
        json_data[address_label + "_ADDR_STATE"] = input_row[
            "Provider Business Practice Location Address State Name"
        ]
        json_data[address_label + "_ADDR_POSTAL_CODE"] = input_row[
            "Provider Business Practice Location Address Postal Code"
        ]
        json_data[address_label + "_ADDR_COUNTRY"] = input_row[
            "Provider Business Practice Location Address Country Code (If outside U.S.)"
        ]

    #  Phone Numbers
    if input_row["Provider Business Mailing Address Telephone Number"]:
        updateStat(
            json_data["DATA_SOURCE"],
            "PHONE-MAILING-LOCATION",
            input_row["Provider Business Mailing Address Telephone Number"],
        )
        json_data["MAILING-LOCATION_PHONE_NUMBER"] = input_row[
            "Provider Business Mailing Address Telephone Number"
        ]
    if input_row["Provider Business Mailing Address Fax Number"]:
        updateStat(
            json_data["DATA_SOURCE"],
            "PHONE-MAILING-FAX",
            input_row["Provider Business Mailing Address Fax Number"],
        )
        json_data["MAILING-FAX_PHONE_NUMBER"] = input_row[
            "Provider Business Mailing Address Fax Number"
        ]
    if input_row["Provider Business Practice Location Address Telephone Number"]:
        updateStat(
            json_data["DATA_SOURCE"],
            "PHONE-BUSINESS-LOCATION",
            input_row["Provider Business Practice Location Address Telephone Number"],
        )
        json_data["BUSINESS-LOCATION_PHONE_NUMBER"] = input_row[
            "Provider Business Practice Location Address Telephone Number"
        ]
    if input_row["Provider Business Practice Location Address Fax Number"]:
        updateStat(
            json_data["DATA_SOURCE"],
            "PHONE-BUSINESS-FAX",
            input_row["Provider Business Practice Location Address Fax Number"],
        )
        json_data["BUSINESS-FAX_PHONE_NUMBER"] = input_row[
            "Provider Business Practice Location Address Fax Number"
        ]

    #  GENDER
    if input_row["Provider Gender Code"]:
        updateStat(
            json_data["DATA_SOURCE"], "GENDER", input_row["Provider Gender Code"]
        )
        json_data["GENDER"] = input_row["Provider Gender Code"]

    #  Provider License Numbers, Taxonomy Codes, and Taxonomy Groups (1-15) are mapped if available
    #  Provider License Numbers are NOT mapped as payload, the rest are
    looper = 1
    pLicNums_Mapped = {}  # Avoid duplicate License Numbers
    pLicNums = []
    pTaxyCds_Mapped = {}  # Avoid Duplicate Taxonomy Codes
    pTaxyCds = []
    txnmyGrp_Mapped = {}  # avoid Duplicate Taxonomy Group Codes
    txnmyGrp = []
    while looper < 16:
        if (
            input_row["Provider License Number_" + str(looper)]
            and input_row["Provider License Number_" + str(looper)] != "========="
        ):
            key1 = (
                input_row["Provider License Number_" + str(looper)]
                + "|"
                + input_row["Provider License Number State Code_" + str(looper)]
            )
            if key1 not in pLicNums_Mapped and check_id_value(
                input_row["Provider License Number_" + str(looper)].split()
            ):
                pLicNums_Mapped[key1] = True
                # --jb: moved to its own feature type
                # pLicNums.append({"OTHER_ID_TYPE": 'PROV_LIC_NUM' , "OTHER_ID_NUMBER": input_row['Provider License Number_' + str(looper)] ,"OTHER_ID_COUNTRY": input_row['Provider License Number State Code_' + str(looper)]})
                updateStat(
                    "PROVIDER_LICENSE",
                    input_row["Provider License Number State Code_" + str(looper)],
                    input_row["Provider License Number_" + str(looper)],
                )
                pLicNums.append(
                    {
                        "PROVIDER_LICENSE_NUMBER": input_row[
                            "Provider License Number_" + str(looper)
                        ],
                        "PROVIDER_LICENSE_STATE": input_row[
                            "Provider License Number State Code_" + str(looper)
                        ],
                    }
                )

        # --payload-- attributes (Taxonomy Codes & Groups)
        if (
            input_row["Healthcare Provider Taxonomy Code_" + str(looper)]
            and input_row["Healthcare Provider Taxonomy Code_" + str(looper)]
            not in pTaxyCds_Mapped
        ):
            pTaxyCds_Mapped[
                input_row["Healthcare Provider Taxonomy Code_" + str(looper)]
            ] = True
            updateStat(
                json_data["DATA_SOURCE"],
                "TAXONOMY_CODE",
                input_row["Healthcare Provider Taxonomy Code_" + str(looper)],
            )
            if (
                input_row["Healthcare Provider Primary Taxonomy Switch_" + str(looper)]
                == "Y"
            ):
                # --jb: moved to jsondata as payload attributes cannot be in a sublist, which is why they are numbered.
                # pTaxyCds.append({"PROVIDER_TAXONOMY_CD": input_row['Healthcare Provider Taxonomy Code_' + str(looper)] + '  (PRI)'})
                json_data["Taxonomy Code_" + str(looper)] = (
                    input_row["Healthcare Provider Taxonomy Code_" + str(looper)]
                    + " (primary)"
                )
            else:
                # --jb: moved to jsondata as payload attributes cannot be in a sublist, which is why they are numbered.
                # pTaxyCds.append({"PROVIDER_TAXONOMY_CD": input_row['Healthcare Provider Taxonomy Code_' + str(looper)]})
                json_data["Taxonomy Code_" + str(looper)] = input_row[
                    "Healthcare Provider Taxonomy Code_" + str(looper)
                ]

        if (
            input_row["Healthcare Provider Taxonomy Group_" + str(looper)]
            and input_row["Healthcare Provider Taxonomy Group_" + str(looper)]
            not in txnmyGrp_Mapped
        ):
            txnmyGrp_Mapped["Healthcare Provider Taxonomy Group_" + str(looper)] = True
            # --jb: moved to jsondata as payload attributes cannot be in a sublist, which is why they are numbered.
            # txnmyGrp.append({"TAXONOMY_GRP": input_row['Healthcare Provider Taxonomy Group_' + str(looper)]})
            json_data["Taxonomy Group_" + str(looper)] = input_row[
                "Healthcare Provider Taxonomy Group_" + str(looper)
            ]
            updateStat(
                json_data["DATA_SOURCE"],
                "TAXONOMY_GROUP",
                input_row["Healthcare Provider Taxonomy Group_" + str(looper)],
            )

        looper += 1

    if pLicNums:
        json_data["PROVIDER_LICENSE_NUMS"] = pLicNums
    # --jb: moved to jsondata as payload attributes cannot be in a sublist, which is why they are numbered.
    # if pTaxyCds:
    #    json_data['PROVIDER_TAXONOMY_CDS'] = pTaxyCds
    # if txnmyGrp:
    #    json_data['PROVIDER_TAXONOMY_GRPS'] = txnmyGrp

    #  Other Provider IDs - 1-51 are checked and mapped if available
    looper = 1
    opIDs_Mapped = {}
    opIDs = []
    while looper < 51:
        if input_row["Other Provider Identifier_" + str(looper)]:
            key1 = (
                input_row["Other Provider Identifier Type Code_" + str(looper)]
                + "|"
                + input_row["Other Provider Identifier_" + str(looper)]
                + "|"
                + input_row["Other Provider Identifier State_" + str(looper)]
            )
            if key1 not in opIDs_Mapped and check_id_value(
                input_row["Other Provider Identifier_" + str(looper)].split()
            ):
                opIDs_Mapped[key1] = True

                # --jb: moved to their own feature type
                if (
                    input_row["Other Provider Identifier Type Code_" + str(looper)]
                    == "05"
                ):
                    updateStat(
                        "PROVIDER_ID-MEDICARE-05",
                        input_row["Other Provider Identifier State_" + str(looper)],
                        input_row["Other Provider Identifier_" + str(looper)],
                    )
                    # opIDs.append({"OTHER_ID_TYPE": 'OTHER_PROV_ID' , "OTHER_ID_NUMBER": input_row['Other Provider Identifier_' + str(looper)], "OTHER_ID_COUNTRY": input_row['Other Provider Identifier State_' + str(looper)] })
                    opIDs.append(
                        {
                            "MEDICAID_PROVIDER_ID": input_row[
                                "Other Provider Identifier_" + str(looper)
                            ],
                            "MEDICAID_PROVIDER_STATE": input_row[
                                "Other Provider Identifier State_" + str(looper)
                            ],
                            "MEDICAID_PROVIDER_ISSUER": input_row[
                                "Other Provider Identifier Issuer_" + str(looper)
                            ],
                        }
                    )
                else:
                    updateStat(
                        "PROVIDER_ID-OTHER-"
                        + input_row[
                            "Other Provider Identifier Type Code_" + str(looper)
                        ],
                        input_row["Other Provider Identifier State_" + str(looper)],
                        input_row["Other Provider Identifier_" + str(looper)],
                    )
                    # opIDs.append({"OTHER_ID_TYPE": 'MEDICAID_PROV_ID' , "OTHER_ID_NUMBER": input_row['Other Provider Identifier_' + str(looper)], "OTHER_ID_COUNTRY": input_row['Other Provider Identifier State_' + str(looper)] })
                    opIDs.append(
                        {
                            "OTHER_PROVIDER_ID": input_row[
                                "Other Provider Identifier_" + str(looper)
                            ],
                            "OTHER_PROVIDER_STATE": input_row[
                                "Other Provider Identifier State_" + str(looper)
                            ],
                            "OTHER_PROVIDER_ISSUER": input_row[
                                "Other Provider Identifier Issuer_" + str(looper)
                            ],
                        }
                    )

        looper += 1

    if opIDs:
        json_data["PROVIDER_IDS"] = opIDs

    # --payload attributes
    if input_row["Provider Enumeration Date"]:
        updateStat(
            json_data["DATA_SOURCE"],
            "UNMAPPED: Provider Enumeration Date",
            input_row["Provider Enumeration Date"],
        )
        json_data["Provider Enumeration Date"] = input_row["Provider Enumeration Date"]
    if input_row["Last Update Date"]:
        updateStat(
            json_data["DATA_SOURCE"],
            "UNMAPPED: Last Update Date",
            input_row["Last Update Date"],
        )
        json_data["Last Update Date"] = input_row["Last Update Date"]
    if input_row["NPI Deactivation Reason Code"]:
        updateStat(
            json_data["DATA_SOURCE"],
            "UNMAPPED: NPI Deactivation Reason Code",
            input_row["NPI Deactivation Reason Code"],
        )
        json_data["NPI Deactivation Reason Code"] = input_row[
            "NPI Deactivation Reason Code"
        ]
    if input_row["NPI Deactivation Date"]:
        updateStat(
            json_data["DATA_SOURCE"],
            "UNMAPPED: NPI Deactivation Date",
            input_row["NPI Deactivation Date"],
        )
        json_data["NPI Deactivation Date"] = input_row["NPI Deactivation Date"]
    if input_row["NPI Reactivation Date"]:
        updateStat(
            json_data["DATA_SOURCE"],
            "UNMAPPED: NPI Reactivation Date",
            input_row["NPI Reactivation Date"],
        )
        json_data["NPI Reactivation Date"] = input_row["NPI Reactivation Date"]
    if (
        input_row["Parent Organization LBN"]
        and input_row["Parent Organization LBN"] != "NONE"
    ):
        updateStat(
            json_data["DATA_SOURCE"],
            "UNMAPPED: Parent Organization LBN",
            input_row["Parent Organization LBN"],
        )
        json_data["Parent Organization LBN"] = input_row["Parent Organization LBN"]

    #   Map the Othername reference data if there is any for this NPI
    onNames = map_othernames(input_row["NPI"])
    if onNames:
        json_data["OTHER_NAMES"] = onNames

    #  Map the authorized official if there is one
    if input_row["Authorized Official Last Name"]:
        Officials_outFile.write(map_auth(input_row, npi_name) + "\n")
        JSON_row_count += 1
        NPIOfficials_row_count += 1

    #   Map the Provider Locations reference data if there are any for this NPI
    map_locations(input_row["NPI"], npi_name, input_row["Entity Type Code"])

    #   Map the Endpoint reference data if there are any for this NPI
    endpointList = map_endpoints(input_row["NPI"])
    # --jb: some endpoints like email and website belong to the npi, others are affiliates
    if endpointList:
        json_data["ENDPOINT_LIST"] = endpointList

    return json.dumps(json_data)


# -------------------------------------------------------------
#  Load Reference data into DB
# -------------------------------------------------------------
def loadDB(inFileSpec, inTabName):

    msgOut(
        0, "  Populating " + inTabName + " DB Table from reference file ", "I", "", 0, 0
    )
    df = pandas.read_csv(
        inFileSpec, low_memory=False, encoding="latin-1", quotechar='"'
    )
    df.to_sql(inTabName, conn, if_exists="replace")
    msgOut(0, "        Building " + inTabName + ".NPI Index", "I", "", 0, 0)
    conn.cursor().execute("create index ix_%s on %s (NPI)" % (inTabName, inTabName))


# -------------------------------------------------------------
#  Check ID Values to see if they are something we should ignore.  Input is a list
# -------------------------------------------------------------
def check_id_value(inList):

    retValue = True

    for word1 in inList:
        if word1 in idValuesToIgnore:
            retValue = False
            break

    return retValue


# ---------------------------------------------------------------------
#   msgout - Used to standardize output messages displayed to std out
#      eDie    - Value of 1 will abort the processing
#      eMsg    - Message String to display
#      eType   - ''  just print the eMsg
#                'E' Prefix eMsg with '**** ERROR -'
#                'W' Prefix eMsg with '.... Warning ->'
#      eRow    - Input Record that generated the message - this is written to a 'bad' file (NOT USED)
#      eCode   - Error code to display
#      eRowNum - Input Record # that generated message (NOT USED)
# ----------------------------------------
def msgOut(eDie, eMsg, eType, eRow, eCode, eRowNum):

    if eDie == 1:
        print("{:%H:%M:%S}".format(datetime.datetime.now()) + eMsg + str(eCode))
        print(
            "{:%H:%M:%S}".format(datetime.datetime.now())
            + "  *****  ABORTING RUN ******"
        )
        sys.exit(eCode)
    else:
        if eType == "E":
            print(
                "{:%H:%M:%S}".format(datetime.datetime.now()) + "  *** ERROR - " + eMsg
            )
        elif eType == "W":
            print(
                "{:%H:%M:%S}".format(datetime.datetime.now())
                + "  ... Warning ->"
                + eMsg
                + msg2
            )
        else:
            print("{:%H:%M:%S} ".format(datetime.datetime.now()) + eMsg)


# ----------------------------------------
#    global stat update
# ----------------------------------------
def updateStat(cat1, cat2, example=None):
    global statPack

    if cat1 not in statPack:
        statPack[cat1] = {}
    if cat2 not in statPack[cat1]:
        statPack[cat1][cat2] = {}
        statPack[cat1][cat2]["count"] = 0

    statPack[cat1][cat2]["count"] += 1
    if example:
        if "examples" not in statPack[cat1][cat2]:
            statPack[cat1][cat2]["examples"] = []
        if example not in statPack[cat1][cat2]["examples"]:
            if len(statPack[cat1][cat2]["examples"]) < 5:
                statPack[cat1][cat2]["examples"].append(example)
            else:
                randomSampleI = random.randint(2, 4)
                statPack[cat1][cat2]["examples"][randomSampleI] = example
    return


# ----------------------------------------
#    interrupt handler
# ----------------------------------------
def signal_handler(signal, frame):
    print("USER INTERRUPT! Shutting down ... (please wait)")
    global shutDown
    shutDown = True
    return


# ---------------------------------------------------------------------
#   M A I N     P R O G R A M
# ---------------------------------------------------------------------
if __name__ == "__main__":

    global shutDown
    shutDown = False
    signal.signal(signal.SIGINT, signal_handler)

    global statPack
    statPack = {}

    procStartTime = time.time()
    abortRun = 0

    msgOut(0, "  - Starting processing", "I", "", 0, 0)
    # --   Checking Arguments passed in
    msgOut(0, "      - Checking parameters passed in", "I", "", 0, 0)

    argParser = argparse.ArgumentParser()
    argParser.add_argument(
        "-i",
        "--sourceDir",
        dest="sourceDir",
        default="",
        help="directory in which the source files are located",
        required=True,
    )
    argParser.add_argument(
        "-f",
        "--filePeriod",
        dest="filePeriod",
        default="",
        help='the period portion of the NPPES file naming convention such as "20050523-20201108"',
        required=True,
    )
    argParser.add_argument(
        "-o",
        "--outFileDir",
        dest="outputFilePath",
        default="",
        help="the file or directory to write the JSON files to",
        required=True,
    )
    argParser.add_argument(
        "-l",
        "--logFileName",
        dest="logFileName",
        default="",
        help="optional statistics output file name",
    )
    parms = argParser.parse_args()

    if (parms.filePeriod and len(parms.filePeriod) > 0) and (
        parms.sourceDir and len(parms.sourceDir) > 0
    ):
        #    Define all the file names
        parms.sourceDir = parms.sourceDir + (
            os.path.sep if parms.sourceDir[-1:] != os.path.sep else ""
        )
        npiDataFileSpec = parms.sourceDir + "npidata_pfile_" + parms.filePeriod + ".csv"
        onDataFileSpec = (
            parms.sourceDir + "othername_pfile_" + parms.filePeriod + ".csv"
        )
        plDataFileSpec = parms.sourceDir + "pl_pfile_" + parms.filePeriod + ".csv"
        epDataFileSpec = parms.sourceDir + "endpoint_pfile_" + parms.filePeriod + ".csv"

        npiDataFileSpec = os.path.abspath(npiDataFileSpec)
        onDataFileSpec = os.path.abspath(onDataFileSpec)
        plDataFileSpec = os.path.abspath(plDataFileSpec)
        epDataFileSpec = os.path.abspath(epDataFileSpec)

        if os.path.isfile(npiDataFileSpec):
            msgOut(
                0,
                "        NPI Main data Input File Name : " + npiDataFileSpec,
                "I",
                "",
                0,
                0,
            )
            npiDataErrFileSpec = npiDataFileSpec + ".err"
        else:
            abortRun = 1
            msgOut(
                0,
                " NPI Main data Input File Name  : "
                + npiDataFileSpec
                + "   <-  is not a file or does not exist",
                "E",
                "",
                2,
                0,
            )
        if os.path.isfile(onDataFileSpec):
            msgOut(
                0,
                "        Other Name reference data Input File Name : " + onDataFileSpec,
                "I",
                "",
                0,
                0,
            )
            onDataErrFileSpec = onDataFileSpec + ".err"
        else:
            abortRun = 1
            msgOut(
                0,
                " Other Name reference data Input File Name  : "
                + onDataFileSpec
                + "   <-  is not a file or does not exist",
                "E",
                "",
                2,
                0,
            )
        if os.path.isfile(plDataFileSpec):
            msgOut(
                0,
                "        Practice Location reference data Input File Name : "
                + plDataFileSpec,
                "I",
                "",
                0,
                0,
            )
            plDataErrFileSpec = plDataFileSpec + ".err"
        else:
            abortRun = 1
            msgOut(
                0,
                " Practice Location reference data Input File Name  : "
                + plDataFileSpec
                + "   <-  is not a file or does not exist",
                "E",
                "",
                2,
                0,
            )
        if os.path.isfile(epDataFileSpec):
            msgOut(
                0,
                "        Endpoint reference data Input File Name : " + epDataFileSpec,
                "I",
                "",
                0,
                0,
            )

        outputFilePath = os.path.abspath(parms.outputFilePath)
        if os.path.isdir(outputFilePath):
            outputOneFile = False
            msgOut(
                0, "        Output File Directory : " + outputFilePath, "I", "", 0, 0
            )
        else:
            outputOneFile = True
            msgOut(0, "        Output File Name : " + outputFilePath, "I", "", 0, 0)

        if abortRun == 1:
            msgOut(1, " Aborting Run after Command Line Validation", "E", "", 42, 0)

        #    Creating Output File names
        if not outputOneFile:
            outputFilePath = outputFilePath + (
                os.path.sep if outputFilePath[-1:] != os.path.sep else ""
            )
            Providers_outputFileSpec = (
                outputFilePath + "NPI_PROVIDERS_" + parms.filePeriod + ".json"
            )
            Officials_outputFileSpec = (
                outputFilePath + "NPI_OFFICIALS_" + parms.filePeriod + ".json"
            )
            Affiliations_outputFileSpec = (
                outputFilePath + "NPI_AFFILIATIONS_" + parms.filePeriod + ".json"
            )
            Locations_outputFileSpec = (
                outputFilePath + "NPI_LOCATIONS_" + parms.filePeriod + ".json"
            )

            #    Checking for existence of output files.  Delete if they exist.

            if not os.path.isfile(Providers_outputFileSpec):
                msgOut(
                    0,
                    "        NPI-PROVIDERS will be written to  : "
                    + Providers_outputFileSpec,
                    "I",
                    "",
                    0,
                    0,
                )
            else:
                msgOut(
                    0,
                    "        NPI-PROVIDERS output file exists and will be replaced  : "
                    + Providers_outputFileSpec,
                    "I",
                    "",
                    0,
                    0,
                )
                os.remove(Providers_outputFileSpec)

            if not os.path.isfile(Officials_outputFileSpec):
                msgOut(
                    0,
                    "        NPI-OFFICIALS will be written to  : "
                    + Officials_outputFileSpec,
                    "I",
                    "",
                    0,
                    0,
                )
            else:
                msgOut(
                    0,
                    "        NPI-OFFICIALS output file exists and will be replaced  : "
                    + Officials_outputFileSpec,
                    "I",
                    "",
                    0,
                    0,
                )
                os.remove(Officials_outputFileSpec)

            if not os.path.isfile(Affiliations_outputFileSpec):
                msgOut(
                    0,
                    "        NPI-AFFILIATIONS will be written to  : "
                    + Affiliations_outputFileSpec,
                    "I",
                    "",
                    0,
                    0,
                )
            else:
                msgOut(
                    0,
                    "        NPI-AFFILIATIONS output file exists and will be replaced  : "
                    + Affiliations_outputFileSpec,
                    "I",
                    "",
                    0,
                    0,
                )
                os.remove(Affiliations_outputFileSpec)

            if not os.path.isfile(Locations_outputFileSpec):
                msgOut(
                    0,
                    "        NPI-LOCATIONS will be written to  : "
                    + Locations_outputFileSpec,
                    "I",
                    "",
                    0,
                    0,
                )
            else:
                msgOut(
                    0,
                    "        NPI-LOCATIONS output file exists and will be replaced  : "
                    + Locations_outputFileSpec,
                    "I",
                    "",
                    0,
                    0,
                )
                os.remove(Locations_outputFileSpec)

    npiInputFile = open(npiDataFileSpec, "r", encoding="utf-8")

    if outputOneFile:
        one_outFile = open(outputFilePath, "w", encoding="utf-8")
        Providers_outFile = one_outFile
        Officials_outFile = one_outFile
        Affiliations_outFile = one_outFile
        Locations_outFile = one_outFile
    else:
        Providers_outFile = open(Providers_outputFileSpec, "w", encoding="utf-8")
        Officials_outFile = open(Officials_outputFileSpec, "w", encoding="utf-8")
        Affiliations_outFile = open(Affiliations_outputFileSpec, "w", encoding="utf-8")
        Locations_outFile = open(Locations_outputFileSpec, "w", encoding="utf-8")

    NPIinput_row_count = 0
    NPIProvider_row_count = 0
    NPIOfficials_row_count = 0
    NPILocations_row_count = 0
    NPIAffiliations_row_count = 0
    JSON_row_count = 1
    progressInterval = 10000  # Report every 'this-many' records processed.

    # Set up list of ID values to ignore.  To check, split value by space and check first word to cover 'NONE ISSUED', 'NONE REQUIRED'....:
    idValuesToIgnore = {}
    idValuesToIgnore["========="] = True
    idValuesToIgnore["PENDING"] = True
    idValuesToIgnore["NA"] = True
    idValuesToIgnore["ENROLLED"] = True
    idValuesToIgnore["NONE"] = True

    # --   open database connection and load from csv
    dbname = parms.sourceDir + "/NPPES.db"
    dbExists = os.path.exists(dbname)
    if dbExists:  # --purge and reload
        msgOut(
            0, "  Purging existing temp DB for reference data :" + dbname, "I", "", 0, 0
        )
        os.remove(dbname)
    else:
        msgOut(0, "  Initializing temp DB for reference data:" + dbname, "I", "", 0, 0)
    conn = sqlite3.connect(dbname)

    # Load up the reference files into the DB and index on NPI

    loadDB(onDataFileSpec, "OTHERNAME")
    loadDB(plDataFileSpec, "PL")
    loadDB(epDataFileSpec, "ENDPOINT")
    msgOut(
        0,
        "  Beginning Main NPI file processing nesting OtherNames & Locations ",
        "I",
        "",
        0,
        0,
    )

    #  Process main NPI file
    for NPIinput_row in csv.DictReader(npiInputFile):
        NPIinput_row_count += 1

        Providers_outFile.write(map_npi(NPIinput_row) + "\n")
        JSON_row_count += 1
        NPIProvider_row_count += 1

        #  Messages at intervals, or stop processing because of test mode
        if NPIinput_row_count % progressInterval == 0:
            msgOut(
                0,
                "          Main NPI rows processed(so far): " + str(NPIinput_row_count),
                "I",
                "",
                0,
                0,
            )

        if shutDown:  # --user abort
            break

    msgOut(
        0,
        "     Total Main NPI rows processed         : " + str(NPIinput_row_count),
        "I",
        "",
        0,
        0,
    )

    # --------------------------------------------------------------------------------------------
    # Wrap-up
    npiInputFile.close()
    if outputOneFile:
        one_outFile.close()
    else:
        Providers_outFile.close()
        Affiliations_outFile.close()
        Locations_outFile.close()
        Officials_outFile.close()

    msgOut(
        0,
        "     Total JSON rows produced              : " + str(JSON_row_count),
        "I",
        "",
        0,
        0,
    )
    msgOut(
        0,
        "     NPI-Provider JSON rows produced       : " + str(NPIProvider_row_count),
        "I",
        "",
        0,
        0,
    )
    msgOut(
        0,
        "     NPI-Officials JSON rows produced      : " + str(NPIOfficials_row_count),
        "I",
        "",
        0,
        0,
    )
    msgOut(
        0,
        "     NPI-Locations JSON rows produced      : " + str(NPILocations_row_count),
        "I",
        "",
        0,
        0,
    )
    msgOut(
        0,
        "     NPI-Affiliations JSON rows produced   : "
        + str(NPIAffiliations_row_count),
        "I",
        "",
        0,
        0,
    )

    # --write statistics file
    if parms.logFileName:
        with open(parms.logFileName, "w") as outfile:
            json.dump(statPack, outfile, indent=4, sort_keys=True)
        msgOut(0, f"Mapping stats written to {parms.logFileName}", "I", "", 0, 0)

    elapsedMins = round((time.time() - procStartTime) / 60, 1)
    if shutDown:
        msgOut(
            0, " Process aborted after " + str(elapsedMins) + " minutes!", "I", "", 0, 0
        )
    else:
        msgOut(
            0,
            " Process completed successfully in " + str(elapsedMins) + " minutes!",
            "I",
            "",
            0,
            0,
        )

    sys.exit(0)
