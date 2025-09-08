import json
from os import path
import pandas as pd
import sys
import re
import time
import requests
from bs4 import BeautifulSoup
from .vgsi_objects import Property, InvalidPIDException

_VGSIURL_ = "https://www.vgsi.com/connecticut-online-database/"

def open_vgsi_cities(state='ct'):
    path = f"vgsi_cities_{state}.json"
    with open(path) as city_json:
        return json.load(city_json)
    
def get_vgsi_cities(url=_VGSIURL_, state='ct'):

    json_path = f"vgsi_cities_{state}.json"

    if path.exists(json_path):
        sys.stdout.write(f"Will overwrite the json for {state}.\n")
    
    city_dict = {}
    sys.stdout.write(f"creating new {json_path} from {_VGSIURL_}")

    page = requests.get(url, verify=False)
    soup = BeautifulSoup(page.content, "html.parser")

    # checks for all links with the source as https://gis.vgsi.com/,
    # also grabs the city names corresponding to the links.
    # writing to dictionary. example below
    # {newhaven: {url: gis.com, type: vgsi}}

    for city_row in soup.find_all(href=re.compile("https://gis.vgsi.com/")):

        url = city_row.get_attribute_list("href")[0]
        match = re.search(r"([\w]{2,}([cC][Tt])+)", url)
        if match:
            location = match.group(1).lower()
            city = location[:-2]
            state = location[-2:]

            city_dict[city] = {
                "city":city_row.get_text(),
                "state":state,
                "url": url,
                "type": None
            }

        city_dict[city]["type"] = "vgsi"
    
    with open(json_path, "w") as outfile:
        json.dump(city_dict, outfile, indent=4)
    sys.stdout.write(f"Loaded {json_path}.")

def load_city(city='newhaven', base_url=None, pid_min=1, pid_max=1000000, null_pages_seq=10, delay_seconds=1):
    property_list = []
    building_list = []
    assesment_list = []
    appraisal_list = []
    ownership_list = []

    if not base_url:
        city_json = open_vgsi_cities()
        vgsi_url = city_json[city]['url']
    else:
        vgsi_url = base_url

    # if pid_min and pid_max:
    #     for i in range (pid_min, pid_max+1):
    #         try:
    #             p = Property(url=vgsi_url, pid=i)
    #             property_list.append(p.data)
    #             building_list.extend(p.buildings)
    #             assesment_list.extend(p.assesments)
    #             appraisal_list.extend(p.appraisals)
    #             ownership_list.extend(p.ownership)
    #         except InvalidPIDException:
    #             pass
    # else:
    null_page_cnt = 0

    while null_page_cnt < null_pages_seq and pid_min <= pid_max:
        # print(f"Trying property id {pid_min} for city {city}")
        try:
            p = Property(url=vgsi_url, pid=pid_min)
            p.load_all()
            property_list.append(p.data)
            building_list.extend(p.buildings)
            assesment_list.extend(p.assesments)
            appraisal_list.extend(p.appraisals)
            ownership_list.extend(p.ownership)
            null_page_cnt = 0
            time.sleep(delay_seconds)
        except:
            null_page_cnt += 1
        pid_min += 1
    
    property_df = pd.DataFrame(property_list)
    building_df = pd.DataFrame(building_list)
    assesment_df = pd.DataFrame(assesment_list)
    appraisal_df = pd.DataFrame(appraisal_list)
    ownership_df = pd.DataFrame(ownership_list)

    return property_df, building_df, assesment_df, appraisal_df, ownership_df