import copy
import json
import os
import re
import time
from datetime import datetime

import geopy
import pymssql
import yaml
from geopy.geocoders import GoogleV3, Nominatim
from loguru import logger

logger.add(
    "sql2json.log",
    format="{time} {level} {message}",
    level="DEBUG",
    rotation="1 days",
    compression="zip",
)

logger.catch()
logger.info(
    "Phoenix 4 to Casl Cloud object converter by Andrii Pidlypnyi https://github.com/dreikk91"
)
try:
    with open("sql2json.yaml") as f:
        yaml_config = yaml.safe_load(f)
        logger.info("Config opened successful")
except FileNotFoundError:
    logger.info("Can't open config, generating new")
    geolocator: str = "Nominatim"
    api_key: str = "BpKmlnBpKmlnhdUiJSPAI16qAVqo2Ks2MHV0pKQ"
    host: str = "127.0.0.1"
    object_number: str = ""
    username: str = "sa"
    password: str = ""
    database: str = "Pult4DB"
    to_yaml: dict = {
        "geo_api": { "api_key": api_key, "geolocator": "Nominatim" },
        "db_connect": {
            "host": host,
            "username": username,
            "password": password,
            "database": database,
        },
        "object_number": { "from number": 1, "to number": 9999 },
        "object_numbers_from_list": True,
        "object_list": "1212",
    }

    with open("sql2json.yaml", "w") as f:
        yaml.dump(to_yaml, f, default_flow_style=False)

    with open("sql2json.yaml") as f:
        yaml_config = yaml.safe_load(f)

if yaml_config["geo_api"]["geolocator"] == "GoogleV3":
    geolocator: GoogleV3 = GoogleV3(api_key=yaml_config["geo_api"]["api_key"])
else:
    geolocator: Nominatim = Nominatim(
        user_agent="Mozilla/5.0 (Windows NT 10.0; WOW64) AppleWebKit/537.36 (KHTML, like Gecko) Chrome/80.0.3987.163 Safari/537.36"
    )
print("Connecting to server %s" % yaml_config["db_connect"]["host"])
host: str = yaml_config["db_connect"]["host"]
username: str = yaml_config["db_connect"]["username"]
password: str = yaml_config["db_connect"]["password"]
database: str = yaml_config["db_connect"]["database"]

conn = pymssql.connect(host, username, password, database, charset='cp1251')


def get_main_data():
    cursor = conn.cursor()
    cursor.execute(
        """SELECT p.Panel_id, p.CreateDate, a.Area_name, m.MasterName, c.CompanyName, c.address, cc.Contract_id, cc.ContractRent, cc.Remarks, c2.PhoneNumber 
            FROM Pult4DB.dbo.Panel p 
            INNER JOIN Pult4DB.dbo.Areas a ON  p.Area_id = a.Area_id 
            LEFT JOIN Pult4DB.dbo.Masters m  ON p.master_id = m.Master_id 
            INNER JOIN Pult4DB.dbo.Company c ON p.Panel_id = LEFT( c.company_id, 4 )
            LEFT JOIN Pult4DB.dbo.Company_Contracts cc ON p.Panel_id = LEFT( cc.company_id, 4 )
            LEFT JOIN Pult4DB.dbo.Central c2 ON p.Panel_id = c2.panel_id """
    )
    rows = cursor.fetchmany(100000)
    return rows


def find_groups_name(panel_id) -> object:
    cursor = conn.cursor()
    cursor.execute(
        """SELECT g.Panel_id, g.Group_, g.Message 
                        FROM Pult4DB.dbo.Groups g 
                        WHERE g.Panel_id = '{}'""".format(
            panel_id
        )
    )
    groups_name = cursor.fetchmany(100000)
    return groups_name


def get_zones_name_and_count(panel_id):
    cursor = conn.cursor()
    cursor.execute(
        """SELECT z.Group_ , z.[Zone], z.Message, z.Panel_id, z.IsAlarmButton
                        FROM Pult4DB.dbo.Zones z  
                        WHERE z.Panel_id = '{}'""".format(
            panel_id
        )
    )
    zones_name = cursor.fetchmany(100000)
    return zones_name


def find_lat_long(address):
    location = None
    cutted_address = (
        address.replace("вул.", "")
            .replace("м.", "")
            .replace(",", "")
            .replace("не реаг", " ")
            .replace("нереагувати", " ")
            .split()
    )
    while location == None:
        try:
            del cutted_address[-1]
            result = cutted_address[:]
            location = geolocator.geocode(result, language="uk")
            # print(" ".join(result))
        except IndexError as err:
            logger.debug("Handling run-time error:", err)
            break
        except geopy.exc.GeocoderQueryError as err:
            logger.debug("Handling run-time error: ", err)
        except geopy.exc.GeocoderTimedOut as err:
            logger.debug("Handling run-time error: ", err)
            time.sleep(1)

    logger.info(location.address)
    logger.info(str(location.latitude)[0:9])
    logger.info(str(location.longitude)[0:9])
    return location.latitude, location.longitude, location.address


device_count: int = 0

json_dict_origin = {
    "export_date": "2020-7-27",
    "type": "connection",
    "data": [
        {
            "guardedObject": {
                "name": "Магазин Щинка До Пиша",
                "address": "вул.Біло-Зелена, 1",
                "lat": "",
                "long": "",
                "description": "Смачна шинка",
                "contract": "1122Г-КЧПр",
                "manager": {
                    "email": "",
                    "last_name": "Горбань",
                    "first_name": "Кирило",
                    "middle_name": "Мефодійович",
                    "role": "MANAGER",
                    "images": [None],
                    "phone_numbers": [{ "active": True, "number": "" }],
                },
                "note": "Львів",
                "start_date": 1595710800000,
                "status": "Включено",
                "object_type": "Магазин",
                "id_request": "Що це?",
                "reacting_pult_id": "1",
                "images": [None],
                "rooms": [
                    {
                        "name": "Магазин 1",
                        "description": "Великий Магазин",
                        "images": [None],
                        "users": None,
                        "lines": {
                            "1": {
                                "adapter_type": "SYS",
                                "group_number": 1,
                                "adapter_number": 0,
                            }
                        },
                    }
                ],
            },
            "device": {
                "number": 1223,
                "name": "Аякс",
                "type": "TYPE_DEVICE_Ajax",
                "timeout": 240,
                "sim1": "+38 (067) 111-11-00",
                "sim2": "",
                "technician": {
                    "email": "",
                    "last_name": "Сталевий",
                    "first_name": "Юхим",
                    "middle_name": "Моісейович",
                    "role": "TECHNICIAN",
                    "images": [None],
                    "phone_numbers": [{ "active": None, "number": "+380675386200" }],
                },
                "units": "",
                "requisites": "",
                "change_date": None,
                "reglament_date": None,
                "block_time": [],
                "lines": {
                    "1": {
                        "adapter_type": "SYS",
                        "adapter_number": 0,
                        "line_type": "ALM_BTN",
                        "group_number": 1,
                        "description": "Немає опису",
                        "is_broken": 0,
                    }
                },
            },
        }
    ],
}


def format_phone_number(number: str) -> str:
    new_number: str = "%s (%s) %s %s %s" % (
        number[0:3],
        number[3:6],
        number[6:9],
        number[9:11],
        number[11:13],
    )
    return new_number


def update_guarded_object(
        object_name, object_address, crate_date, contract, city, manager
) -> object:
    """

    :type object_address: object
    :rtype: object
    """
    today = datetime.now().strftime("%Y-%m-%d")
    json_dict["export_date"] = today
    guarded_object = copy.deepcopy(json_dict["data"][0]["guardedObject"])
    guarded_object["rooms"][0]["lines"].clear()
    guarded_object["name"] = object_name
    guarded_object["address"] = object_address
    guarded_object["contract"] = str(contract)
    guarded_object["note"] = str(city)
    try:
        splited_name = object_name.split()
        guarded_object["object_type"] = str(splited_name[0])
    except AttributeError as err:
        guarded_object["object_type"] = " "
        logger.debug(err)

    guarded_object["start_date"] = int(time.mktime(crate_date.timetuple()) * 1000)
    guarded_object["manager"]["last_name"] = manager
    try:
        lat, long, loc_address = find_lat_long(object_address)
        guarded_object["lat"] = str(lat)[0:9]
        guarded_object["long"] = str(long)[0:9]
        guarded_object["description"] = loc_address
    except AttributeError as err:
        logger.debug("Handling run-time error:", err)
        guarded_object["lat"] = ""
        guarded_object["long"] = ""
        guarded_object["description"] = object_name
    return guarded_object


def find_max_group(find_groups_name):
    count: int = 1
    max_group: int = 1
    for group in find_groups_name:
        if int(group[1]) > max_group:
            max_group = int(group[1])
    return max_group


def find_group_name(find_groups_name: object):
    dict_group_name = { }
    count: int = 1

    for group_name in find_groups_name:
        try:
            new_dict = { count: group_name[2] }
        except KeyError as err:
            logger.debug(err)
        dict_group_name.update(new_dict)
        try:
            print(dict_group_name[count])
        except IndexError as err:
            logger.debug(err)
        except KeyError as err:
            logger.debug(err)
        count += 1
    return dict_group_name


def update_guarded_object_rooms(guarded_object, find_max_group, find_group_name):
    guarded_object_rooms = copy.deepcopy(guarded_object["rooms"])
    guarded_object_rooms.clear
    guarded_object_rooms = copy.deepcopy(guarded_object["rooms"])
    group_list = find_groups_name(data[0])
    for group_count in range(find_max_group):
        if group_count == 0:
            try:
                guarded_object_rooms[0]["name"] = copy.deepcopy(group_list[0][2])
                guarded_object_rooms[0]["description"] = copy.deepcopy(group_list[0][2])
            except KeyError as err:
                logger.debug(err)
        else:
            try:
                guarded_object_rooms.insert(
                    group_count, copy.deepcopy(guarded_object["rooms"][0])
                )
                guarded_object_rooms[group_count]["name"] = copy.deepcopy(
                    group_list[group_count][2]
                )
                guarded_object_rooms[group_count]["description"] = copy.deepcopy(
                    group_list[group_count][2]
                )
            except KeyError as err:
                logger.debug(err)
    return guarded_object_rooms


def update_guarded_device(object_name, central_phone_number, type_central, panel_id):
    guarded_device = json_dict["data"][0]["device"].copy()
    guarded_device["lines"] = { }
    guarded_device["lines"].clear()

    guarded_device["number"] = int(re.sub(r"[^0-9+]+", r"", panel_id))
    logger.info(int(re.sub(r"[^0-9+]+", r"", panel_id)))
    guarded_device["name"] = object_name
    guarded_device["type"] = "TYPE_DEVICE_Ajax"
    guarded_device["timeout"] = 1800
    try:
        if str(central_phone_number)[0] == "3":
            guarded_device["sim1"] = "+" + str(central_phone_number)
            guarded_device["sim2"] = "+" + str(central_phone_number)
        else:
            guarded_device["sim1"] = "+38" + str(central_phone_number)
            guarded_device["sim2"] = "+38" + str(central_phone_number)
    except IndexError:
        guarded_device["sim1"] = ""
        guarded_device["sim2"] = ""

    try:
        guarded_device["sim1"] = format_phone_number(guarded_device["sim1"])
        guarded_device["sim2"] = format_phone_number(guarded_device["sim2"])
    except IndexError:
        guarded_device["sim1"] = ""
        guarded_device["sim2"] = ""

    if type_central == "Ajax":
        guarded_device["type"] = "TYPE_DEVICE_Ajax"
    else:
        guarded_device["type"] = "TYPE_DEVICE_Lun"
    return guarded_device


def update_guarded_device_lines(get_zones_name_and_count):
    guarded_device_lines = { }
    guarded_device_lines.clear()
    for zone in get_zones_name_and_count:
        if zone[0] != 1 and zone[1] == 1:
            continue
        guarded_device_lines.update(
            {
                str(zone[1]): {
                    "adapter_type": "SYS",
                    "adapter_number": 0,
                    "line_type": "NORMAL",
                    "group_number": int(zone[0]),
                    "description": zone[2],
                }
            }
        )
        if zone[4] == True:
            # print(guarded_device_lines[str(zone[1])]["line_type"])
            guarded_device_lines[str(zone[1])]["line_type"] = "ALM_BTN"
    return guarded_device_lines


def update_guarded_object_rooms_lines_v2(guarded_device_lines):
    for line in guarded_device_lines.items():
        try:
            guarded_object_rooms[int(line[1]["group_number"]) - 1]["lines"].update(
                {
                    str(line[0]): {
                        "adapter_type": "SYS",
                        "group_number": int(line[1]["group_number"]),
                        "adapter_number": 0,
                    }
                }
            )
        except KeyError as err:
            logger.debug(line[1]["group_number"], err)
            guarded_object_rooms[int(line[1]["group_number"])]["lines"].update(
                {
                    str(line[0]): {
                        "adapter_type": "SYS",
                        "group_number": int(line[1]["group_number"]),
                        "adapter_number": 0,
                    }
                }
            )


json_dict = copy.deepcopy(json_dict_origin)
count = 1  # Global count for pipes etc
group_count = 0  # Device counter

for data in get_main_data():
    panel_id: int = data[0]
    create_obj_date: datetime = data[1]
    area_name: str = data[2]
    master_name: str = data[3]
    company_name: str = data[4]
    object_address: str = data[5]
    contract_id: str = data[6]
    remarks: str = data[8]
    phone_number: str = data[9]
    if yaml_config["object_numbers_from_list"] == True:

        if panel_id in yaml_config["object_list"]:

            guarded_object = update_guarded_object(
                company_name,
                object_address,
                create_obj_date,
                contract_id,
                area_name,
                master_name,
            )
            guarded_object_rooms = update_guarded_object_rooms(
                guarded_object,
                find_max_group(find_groups_name(panel_id)),
                find_group_name(find_groups_name(panel_id)),
            )
            guarded_device = update_guarded_device(
                company_name, phone_number, "Ajax", panel_id
            )
            guarded_device_lines = update_guarded_device_lines(
                get_zones_name_and_count(panel_id)
            )
            guarded_object_lines = update_guarded_object_rooms_lines_v2(
                guarded_device_lines
            )
            guarded_object["rooms"][0]["lines"].clear()
            guarded_device["lines"].clear()
            guarded_object["rooms"].clear()

            if device_count == 0:
                json_dict["data"][0]["guardedObject"].update(
                    copy.deepcopy(guarded_object)
                )
                for lst in copy.deepcopy(guarded_object_rooms):
                    json_dict["data"][0]["guardedObject"]["rooms"].append(lst)

                json_dict["data"][0]["device"].update(copy.deepcopy(guarded_device))
                json_dict["data"][0]["device"]["lines"].update(
                    copy.deepcopy(guarded_device_lines)
                )

            else:
                json_dict["data"].insert(
                    device_count, { "guardedObject": { }, "device": { } }
                )
                json_dict["data"][device_count]["guardedObject"].update(
                    copy.deepcopy(guarded_object)
                )
                for lst in copy.deepcopy(guarded_object_rooms):
                    json_dict["data"][device_count]["guardedObject"]["rooms"].append(
                        copy.deepcopy(lst)
                    )

                guarded_device["lines"].update(copy.deepcopy(guarded_device_lines))
                json_dict["data"][device_count]["device"].update(
                    copy.deepcopy(guarded_device)
                )
                json_dict["data"][device_count]["device"]["lines"].update(
                    copy.deepcopy(guarded_device_lines)
                )

            device_count += 1
    elif (
            int(re.sub(r"[^0-9+]+", r"", panel_id))
            >= yaml_config["object_number"]["from number"]
            and int(re.sub(r"[^0-9+]+", r"", panel_id))
            <= yaml_config["object_number"]["to number"]
    ):
        guarded_object = update_guarded_object(
            company_name,
            object_address,
            create_obj_date,
            contract_id,
            area_name,
            master_name,
        )
        guarded_object_rooms = update_guarded_object_rooms(
            guarded_object,
            find_max_group(find_groups_name(panel_id)),
            find_group_name(find_groups_name(panel_id)),
        )
        guarded_device = update_guarded_device(
            company_name, phone_number, "Ajax", panel_id
        )
        guarded_device_lines = update_guarded_device_lines(
            get_zones_name_and_count(panel_id)
        )
        guarded_object_lines = update_guarded_object_rooms_lines_v2(
            guarded_device_lines
        )
        guarded_object["rooms"][0]["lines"].clear()
        guarded_device["lines"].clear()
        guarded_object["rooms"].clear()

        if device_count == 0:
            json_dict["data"][0]["guardedObject"].update(copy.deepcopy(guarded_object))
            for lst in copy.deepcopy(guarded_object_rooms):
                json_dict["data"][0]["guardedObject"]["rooms"].append(lst)

            json_dict["data"][0]["device"].update(copy.deepcopy(guarded_device))
            json_dict["data"][0]["device"]["lines"].update(
                copy.deepcopy(guarded_device_lines)
            )

        else:
            json_dict["data"].insert(device_count, { "guardedObject": { }, "device": { } })
            json_dict["data"][device_count]["guardedObject"].update(
                copy.deepcopy(guarded_object)
            )
            for lst in copy.deepcopy(guarded_object_rooms):
                json_dict["data"][device_count]["guardedObject"]["rooms"].append(
                    copy.deepcopy(lst)
                )

            guarded_device["lines"].update(copy.deepcopy(guarded_device_lines))
            json_dict["data"][device_count]["device"].update(
                copy.deepcopy(guarded_device)
            )
            json_dict["data"][device_count]["device"]["lines"].update(
                copy.deepcopy(guarded_device_lines)
            )

        device_count += 1

json_result = (
    json.dumps(json_dict, ensure_ascii=False, indent=4).encode("utf8").decode("utf8")
)
try:
    with open(
            "json_output\\converted_sql_{0}.json".format(
                str(datetime.now().strftime("%d-%m-%Y %H-%M-%S"))
            ),
            "w",
            encoding="utf8",
    ) as outfile:
        outfile.write(json_result)
except FileNotFoundError as err:
    logger.debug(err)
    os.mkdir("json_output")
    with open(
            "json_output\\converted_sql_{0}.json".format(
                str(datetime.now().strftime("%d-%m-%Y %H-%M-%S"))
            ),
            "w",
            encoding="utf8",
    ) as outfile:
        outfile.write(json_result)
