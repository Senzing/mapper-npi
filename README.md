# mapper-npi

## Overview

The [npi_mapper.py](npi_mapper.py) python script converts the NPPES NPI Registry located
[here](https://npiregistry.cms.hhs.gov).  It is a free directory of all active National Provider
Identifier (NPI) records provided by US government.


Loading this data into Senzing requires additional features and configurations. These are contained in the
[npi_config_updates.json](npi_config_updates.json) file.

Usage:

```console
python npi_mapper.py --help
usage: npi_mapper.py [-h] -i SOURCEDIR -f FILEPERIOD -o OUTPUTFILEDIR

optional arguments:
  -h, --help            show this help message and exit
  -i SOURCEDIR, --sourceDir SOURCEDIR
                        directory in which the source files are located
  -f FILEPERIOD, --filePeriod FILEPERIOD
                        the period portion of the NPPES files such as "20050523-20201108"
  -o OUTPUTFILEDIR, --outFileDir OUTPUTFILEDIR
                        the directory to write the JSON files to
```

## Contents

1. [Prerequisites](#prerequisites)
2. [Installation](#installation)
3. [Configuring Senzing](#configuring-senzing)
4. [Running the mapper](#running-the-mapper)
5. [Loading into Senzing](#loading-into-senzing)
6. [Mapping other data sources](#mapping-other-data-sources)

### Prerequisites

- python 3.6 or higher
- Senzing API version 2.1 or higher
- pandas (pip3 install pandas)

### Installation

Place the the following files on a directory of your choice ...

- [npi_mapper.py](npi_mapper.py)
- [npi_config_updates.g2c](npi_config_updates.g2c)

### Configuring Senzing

*Note:* This only needs to be performed one time! In fact you may want to add these configuration updates to a master configuration file for all your data sources.

From your Senzing project directory ...

```console
python3 G2ConfigTool.py <path-to-file>/npi_config_updates.json
```

This will step you through the process of adding the data sources, entity types, features, attributes and other settings needed to load this watch list data into Senzing. After each command you will see a status message saying "success" or "already exists".  For instance, if you run the script twice, the second time through they will all say "already exists" which is OK.

### Running the mapper

Download the raw files from ... [https://download.cms.gov/nppes/NPI_Files.html](https://download.cms.gov/nppes/NPI_Files.html)

The full monthly download contains many files.   These are the ones actually used by this mapper ...

- npidata_pfile_20050523-20201108.csv       main provider file
- othername_pfile_20050523-20201108.csv     additional names providers are known by
- endpoint_pfile_20050523-20201108.csv      emails and websites for providers as well as their affiliated entities
- pl_pfile_20050523-20201108.csv            additional locations for each provider

*Note the period date range "20050523-20201108" will be based on the date you download the zip file.*

Then run the mapper.  Example usage:

```console
python3 npi_mapper.py -i ./NPPES_Data_Dissemination_November_2020/ -f 20050523-20201108 -o ./output
```

This will create the following 4 output files ...
- NPI_LOCATIONS_20050523-20201108.json
- NPI_OFFICIALS_20050523-20201108.json
- NPI_PROVIDERS_20050523-20201108.json
- NPI_AFFILIATIONS_20050523-20201108.json


### Loading into Senzing

If you use the G2Loader program to load your data, from your project directory ...

```console
python3 G2Loader.py -f ./output/NPI_LOCATIONS_20050523-20201108.json
python3 G2Loader.py -f ./output/NPI_OFFICIALS_20050523-20201108.json
python3 G2Loader.py -f ./output/NPI_PROVIDERS_20050523-20201108.json
python3 G2Loader.py -f ./output/NPI_AFFILIATIONS_20050523-20201108.json
```

or if those are the only files in the output directory you can use a wild card like so ...

```
python3 G2Loader.py -f ./output/NPI*.json
```

This data set currently contains about 7 million providers and yields a total of 8.5 million records with the additional entities created for officials, locations and affiliations.
It may take a few hours to load depending on your harware.

If you use the API directly, then you just need to perform an addRecord() for each line of each file.

### Mapping other data sources

While not required, look the following identifiers in your other data sets..
- NPI number
- Other provider licenses issued by states
- Medicare provider IDs
- Any other provider IDs 
