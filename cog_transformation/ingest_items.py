import json
import re
import requests

import pystac
import rasterio

# Import rio_stac methods
from rio_stac.stac import (
    bbox_to_geom,
    get_dataset_geom,
    get_projection_info,
    get_raster_info,
)

import boto3

from date import extract_dates


dataset_definition ={
    "collection": "epa-ch4emission-yeargrid-v2express",
    "bucket": "ghgc-data-store-dev",
    "prefix": "epa-ch4emission-yeargrid-v2express/",
    "filename_regex": ".*Express_Extension_.*.tif$",
    "datetime_group": ".*_(.*).tif$",
    "assets": {
        "post-meter": {
            "title": "Post Meter (annual)",
            "description": "Annual methane emissions from downstream of natural gas distribution meters derived from residential, commercial, and industrial post-meter emissions data gridded based on population.",
            "regex": ".*emi_ch4_Supp_1B2b_PostMeter.*.tif$"
        },
        "enteric-fermentation": {
            "title": "Enteric Fermentation (3A)",
            "description": "Emissions from agriculture include enteric fermentation, manure management, rice cultivation, and field burning of agricultural residues. ",
            "regex": ".*emi_ch4_3A_Enteric_Fermentation.*.tif$"
        },
        "manure-management": {
            "title": "Manure Management (3B)",
            "description": "Emissions from agriculture include enteric fermentation, manure management, rice cultivation, and field burning of agricultural residues. ",
            "regex": ".*emi_ch4_3B_Manure_Management.*.tif$"
        },
        "rice-cultivation": {
            "title": "Rice Cultivation (3C)",
            "description": "Emissions from agriculture include enteric fermentation, manure management, rice cultivation, and field burning of agricultural residues. ",
            "regex": ".*emi_ch4_3C_Rice_Cultivation.*.tif$"
        },
        "field-burning": {
            "title": "Field Burning of Agricultural Residues* (3F)",
            "description": "Emissions from agriculture include enteric fermentation, manure management, rice cultivation, and field burning of agricultural residues. ",
            "regex": ".*emi_ch4_3F_Field_Burning.*.tif$"
        },
        "exploration-ngs": {
            "title": "Exploration (1B2b)",
            "description": "This source type includes emissionsfrom natural gas production, processing, transmission, and distribution. It does not include emissions from abandone dwells. ",
            "regex": ".*emi_ch4_1B2b_Natural_Gas_Exploration.*.tif$"
        },
        "production-ngs": {
            "title": "Production (1B2b)",
            "description": "This source type includes emissionsfrom natural gas production, processing, transmission, and distribution. It does not include emissions from abandoned wells. ",
            "regex": ".*emi_ch4_1B2b_Natural_Gas_Production.*.tif$"
        },
        "transmission-storage-ngs": {
            "title": "Transmission & Storage (1B2b)",
            "description": "This source type includes emissionsfrom natural gas production, processing, transmission, and distribution. It does not include emissions from abandoned wells. ",
            "regex": ".*emi_ch4_1B2b_Natural_Gas_TransmissionStorage.*.tif$"
        },
        "processing-ngs": {
            "title": "Processing (1B2b)",
            "description": "This source type includes emissionsfrom natural gas production, processing, transmission, and distribution. It does not include emissions from abandoned wells. ",
            "regex": ".*emi_ch4_1B2b_Natural_Gas_Processing.*.tif$"
        },
        "distribution-ngs": {
            "title": "Distribution (1B2b)",
            "description": "This source type includes emissionsfrom natural gas production, processing, transmission, and distribution. It does not include emissions from abandoned wells. ",
            "regex": ".*emi_ch4_1B2b_Natural_Gas_Distribution.*.tif$"
        },
        "exploration-ps": {
            "title": "Exploration* (1B2a)",
            "description": "The GHGI includes national emissions from different activities and equipment related to petroleum production, refining, and transport. ",
            "regex": ".*emi_ch4_1B2a_Petroleum_Systems_Exploration.*.tif$"
        },
        "production-ps": {
            "title": "Production (1B2a)",
            "description": "The GHGI includes national emissions from different activities and equipment related to petroleum production, refining, and transport. ",
            "regex": ".*emi_ch4_1B2a_Petroleum_Systems_Production.*.tif$"
        },
        "transport-ps": {
            "title": "Transport (1B2a)",
            "description": "The GHGI includes national emissions from different activities and equipment related to petroleum production, refining, and transport. ",
            "regex": ".*emi_ch4_1B2a_Petroleum_Systems_Transport.*.tif$"
        },
        "refining-ps": {
            "title": "Refining (1B2a)",
            "description": "The GHGI includes national emissions from different activities and equipment related to petroleum production, refining, and transport. ",
            "regex": ".*emi_ch4_1B2a_Petroleum_Systems_Refining.*.tif$"
        },
        "msw-landfill-waste": {
            "title": "Municipal Solid Waste (MSW) Landfills (5A1)",
            "description": "Waste emissions include landfills, wastewatertreatment, and composting, for which EPA provides national totals. ",
            "regex": ".*emi_ch4_5A1_Landfills_MSW.*.tif$"
        },
        "industrial-landfill-waste": {
            "title": "Industrial Landfills (5A1)",
            "description": "Waste emissions include landfills, wastewatertreatment, and composting, for which EPA provides national totals. ",
            "regex": ".*emi_ch4_5A1_Landfills_Industrial.*.tif$"
        },
        "dwtd-waste": {
            "title": "Domestic Wastewater Treatment & Discharge (5D)",
            "description": "Waste emissions include landfills, wastewatertreatment, and composting, for which EPA provides national totals. ",
            "regex": ".*emi_ch4_5D_Wastewater_Treatment_Domestic.*.tif$"
        },
        "iwtd-waste": {
            "title": "Industrial Wastewater Treatment & Discharge (5D)",
            "description": "Waste emissions include landfills, wastewatertreatment, and composting, for which EPA provides national totals. ",
            "regex": ".*emi_ch4_5D_Wastewater_Treatment_Industrial.*.tif$"
        },
        "composting-waste": {
            "title": "Composting (5B1)",
            "description": "Waste emissions include landfills, wastewatertreatment, and composting, for which EPA provides national totals. ",
            "regex": ".*emi_ch4_5B1_Composting.*.tif$"
        },
        "underground-coal": {
            "title": "Underground Coal Mining (1B1a)",
            "description": "Coal mining emissions using state-level emission estimates produced for the GHGI. ",
            "regex": ".*emi_ch4_1B1a_Underground_Coal.*.tif$"
        },
        "surface-coal": {
            "title": "Surface Coal Mining (1B1a)",
            "description": "Coal mining emissions using state-level emission estimates produced for the GHGI. ",
            "regex": ".*emi_ch4_1B1a_Surface_Coal.*.tif$"
        },
        "abn-underground-coal": {
            "title": "Abandoned Underground Coal Mines (1B1a)",
            "description": "Coal mining emissions using state-level emission estimates produced for the GHGI. ",
            "regex": ".*emi_ch4_1B1a_Abandoned_Coal.*.tif$"
        },
        "stationary-combustion-other": {
            "title": "Stationary combustion (1A)",
            "description": "Other refers to a number of smaller sources. ",
            "regex": ".*emi_ch4_1A_Combustion_Stationary.*.tif$"
        },
        "mobile-combustion-other": {
            "title": "Mobile Combustion (1A)",
            "description": "Other refers to a number of smaller sources.",
            "regex": ".*emi_ch4_1A_Combustion_Mobile.*.tif$"
        },
        "abn-ong-other": {
            "title": "Abandoned Oil and Gas Wells (1B2a & 1B2b)",
            "description": "Other refers to a number of smaller sources.",
            "regex": ".*emi_ch4_1B2ab_Abandoned_Oil_Gas.*.tif$"
        },
        "petro-production-other": {
            "title": "Petrochemical Production (2B8)",
            "description": "Other refers to a number of smaller sources.",
            "regex": ".*emi_ch4_2B8_Industry_Petrochemical.*.tif$"
        },
        "ferroalloy-production-other": {
            "title": "Ferroalloy Production (2C2)",
            "description": "Other refers to a number of smaller sources.",
            "regex": ".*emi_ch4_2C2_Industry_Ferroalloy.*.tif$"
        },
        "total-methane": {
            "title": "Total Methane (annual)",
            "description": "Total annual methane emission fluxes from all Agriculture, Energy, Waste, and ‘Other’ sources included in this dataset.",
            "regex": ".*Express_Extension_all-variables.*.tif$"
        },
        "total-agriculture": {
            "title": "Total Agriculture (annual)",
            "description": "Total annual methane emission fluxes from Agriculture sources (sum of inventory categories: 3A, 3B, 3C, 3F).",
            "regex": ".*Express_Extension_agriculture.*.tif$"
        },
        "total-natural-gas-systems": {
            "title": "Total Natural Gas Systems (annual)",
            "description": "Total annual methane emission fluxes from Natural Gas Systems (sum of inventory Energy 1B2b sub-categories).",
            "regex": ".*Express_Extension_natural-gas-systems.*.tif$"
        },
        "total-petroleum-systems": {
            "title": "Total Petroleum Systems (annual)",
            "description": "Total annual methane emission fluxes from Petroleum Systems (sum of inventory Energy 1B2a sub-categories)",
            "regex": ".*Express_Extension_petroleum-systems.*.tif$"
        },
        "total-waste": {
            "title": "Total Waste (annual)",
            "description": "Total annual methane emission fluxes from Waste (sum of inventory Waste categories: 5A1, 5B1, 5D)",
            "regex": ".*Express_Extension_waste.*.tif$"
        },
        "total-other": {
            "title": "Total Other (annual)",
            "description": "Total annual methane emission fluxes from ‘other’ remaining sources (sum of inventory categories 1A (energy combustion), 2B8 & 2C2 (petrochemical & ferroalloy production) and 1B2a & 1B2b (abandoned O&G well emissions)).",
            "regex": ".*Express_Extension_other.*.tif$"
        },
        "total-coal-mines": {
            "title": "Total Coal Mines (annual)",
            "description": "Total annual methane emission fluxes from Coal Mines (sum of inventory 1B1a sub-categories).",
            "regex": ".*Express_Extension_coal-mines.*.tif$"
        }
    }
}



def list_bucket(bucket, prefix, filename_regex):
    s3 = boto3.resource("s3")
    try:
        files = []
        bucket = s3.Bucket(bucket)
        # print(f"{bucket} {prefix} == epa_express_extension_Mg_km2_yr")
        for obj in bucket.objects.filter(Prefix=prefix):
            if filename_regex:
                if re.match(filename_regex, obj.key):
                    if obj.key.endswith("xml"):
                        # print(obj.key)
                        exit()
                    files.append(obj.key)
            else:
                files.append(obj.key)
        return files

    except:
        print("Failed during s3 item/asset discovery")
        raise


media_type = pystac.MediaType.COG
role = ["data", "layer"]

bucket = dataset_definition.get("bucket")
prefix = dataset_definition.get("prefix")
start_datetime = dataset_definition.get("start_datetime")
end_datetime = dataset_definition.get("end_datetime")
single_datetime = dataset_definition.get("single_datetime")
datetime_range = dataset_definition.get("datetime_range")
properties = dataset_definition.get("properties", {})
collection = dataset_definition.get("collection")
filename_regex = dataset_definition.get("filename_regex")
assets = dataset_definition.get("assets")
datetime_group = dataset_definition.get("datetime_group")

filenames = [
    f"s3://{bucket}/{name}"
    for name in list_bucket(bucket=bucket, prefix=prefix, filename_regex=filename_regex)
]

grouped_files = {}
for filename in filenames:
    for key, value in assets.items():
        if re.match(assets[key]["regex"], filename):
            grouped_files.setdefault(key, [])
            grouped_files[key].append(filename)

# based on datetime
items = {}
for key, values in grouped_files.items():
    for value in values:
        if match := re.match(datetime_group, value):
            try:
                date_grp = match.group(1)
                items.setdefault(date_grp, [])
                items[date_grp].append(value)
            except IndexError:
                pass

for key, value in items.items():
    id = f"{collection}-{key}"
    ass = []
    item_files = value
    start_datetime, end_datetime, single_datetime = extract_dates(
        f"_{key}", datetime_range
    )
    properties = {}
    if start_datetime and end_datetime:
        # these are added post-serialization to properties, unlike single_datetime
        properties["start_datetime"] = start_datetime.strftime("%Y-%m-%dT%H:%M:%SZ")
        properties["end_datetime"] = end_datetime.strftime("%Y-%m-%dT%H:%M:%SZ")
        single_datetime = None

    date_args = {
        "start_datetime": start_datetime,
        "end_datetime": end_datetime,
        "datetime": single_datetime,
    }
    # date_args = {k: v for k, v in date_args.items() if v}
    for asset_key, asset_value in assets.items():
        for file in item_files:
            if file in grouped_files[asset_key]:
                path = file
        ass.append(
            {
                "name": asset_key,
                "path": path,
                "href": None,
                "role": role,
                "type": media_type,
            }
        )

    bboxes = []
    pystac_assets = []
    img_datetimes = []

    for asset in ass:
        with rasterio.open(asset["path"]) as src_dst:
            # Get BBOX and Footprint
            dataset_geom = get_dataset_geom(src_dst, densify_pts=0, precision=-1)
            bboxes.append(dataset_geom["bbox"])

            proj_info = {
                f"proj:{name}": value
                for name, value in get_projection_info(src_dst).items()
            }
            raster_info = {"raster:bands": get_raster_info(src_dst, max_size=1024)}

            pystac_assets.append(
                (
                    asset["name"],
                    pystac.Asset(
                        href=asset["href"] or src_dst.name,
                        media_type=media_type,
                        extra_fields={**proj_info, **raster_info},
                        roles=asset["role"],
                    ),
                )
            )

    minx, miny, maxx, maxy = zip(*bboxes)
    bbox = [min(minx), min(miny), max(maxx), max(maxy)]

    # item
    item = pystac.Item(
        id=id,
        geometry=bbox_to_geom(bbox),
        bbox=bbox,
        collection=collection,
        stac_extensions=[],
        # datetime=single_datetime,
        **date_args,
        properties={},
    )

    # if we add a collection we MUST add a link
    if collection:
        item.add_link(
            pystac.Link(
                pystac.RelType.COLLECTION,
                collection,
                media_type=pystac.MediaType.JSON,
            )
        )

    for key, asset in pystac_assets:
        item.add_asset(key=key, asset=asset)

    username = "vgaur"
    password = "ManUnited_181995"

    # endpoint to get the token from
    token_url = "http://dev.ghg.center/api/publish/token"

    # authentication credentials to be passed to the token_url
    body = {
        "username": username,
        "password": password,
    }

    # request token
    response = requests.post(token_url, data=body)
    if not response.ok:
        raise Exception(
            "Couldn't obtain the token. Make sure the username and password are correct."
        )
    else:
        # get token from response
        token = response.json().get("AccessToken")
        # prepare headers for requests
        headers = {"Authorization": f"Bearer {token}"}

    url = "http://dev.ghg.center/api/publish/ingestions"

    headers = {"Authorization": f"bearer {token}"}

    response = requests.post(url, headers=headers, json=item.to_dict())
    print(id, response.status_code)

    # with open(f"{id}.json", "w") as f:
    #     json.dump(item.to_dict(), f)
