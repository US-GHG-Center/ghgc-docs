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


dataset_definition = {
    "collection": "micasa-carbonflux-monthgrid-v1",
    "bucket": "ghgc-data-store-dev",
    "prefix": "MiCASA/",
    "filename_regex": ".*MiCASA_v1.*.tif$",
    "datetime_group": ".*_(.*).tif$",
    "datetime_range": "month",
    "assets": {
        "npp": {
            "title": "Net Primary Production (NPP), MiCASA Model v1",
            "description": "Net Primary Production (carbon available from plants) in units of grams of carbon per square meter per day (monthly mean)",
            "regex": ".*MiCASA_v1_NPP_.*monthly.*.tif$"
        },
        "rh": {
            "title": "Heterotrophic respiration (Rh), MiCASA Model v1",
            "description": "Heterotrophic respiration (carbon flux from the soil to the atmosphere) in units of grams of carbon per square meter per day (monthly mean)",
            "regex": ".*MiCASA_v1_Rh_.*monthly.*.tif$"
        },
        "nee": {
            "title": "Net Ecosystem Exchange (NEE), MiCASA Model v1",
            "description": "Net Ecosystem Exchange (net carbon flux to the atmosphere) in units of grams of carbon per square meter per day (monthly mean)",
            "regex": ".*MiCASA_v1_NEE_.*monthly.*.tif$"
        },
        "fire": {
            "title": "Fire emissions (FIRE), MiCASA Model v1",
            "description": "Fire emissions (flux of carbon to the atmosphere from wildfires) in units of grams of carbon per square meter per day (monthly mean)",
            "regex": ".*MiCASA_v1_FIRE.*monthly.*.tif$"
        },
        "fuel": {
            "title": "Wood fuel emissions (FUEL), MiCASA Model v1",
            "description": "Wood fuel emissions (flux of carbon to the atmosphere from wood burned for fuel) in units of grams of carbon per square meter per day (monthly mean)",
            "regex": ".*MiCASA_v1_FUEL_.*monthly.*.tif$"
        },
        "nbe": {
            "title": "Net Biosphere Exchange (NBE), MiCASA Model v1",
            "description": "Net Biosphere Exchange (net carbon flux from the ecosystem) in units of grams of carbon per square meter per day (monthly mean)",
            "regex": ".*MiCASA_v1_NBE_.*monthly.*.tif$"
        },
        "atmc": {
            "title": "TBD",
            "description": "TBD",
            "regex": ".*MiCASA_v1_ATMC_.*monthly.*.tif$"
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
    token_url = "http://dev.ghg.center/ghgcenter/api/publish/token"

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

    url = "http://dev.ghg.center/ghgcenter/api/publish/ingestions"

    headers = {"Authorization": f"bearer {token}"}

    response = requests.post(url, headers=headers, json=item.to_dict())
    print(id, response.status_code)

    # with open(f"{id}.json", "w") as f:
    #     json.dump(item.to_dict(), f)
