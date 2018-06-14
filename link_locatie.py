"""
This script will link session IDs to locations.
"""

import logging
import pandas
from geopy.geocoders import Nominatim
from geopy.location import Location
from lib import my_env
from lib import neostore
from urllib.parse import unquote

# Node Labels
lbl_person = "Person"
lbl_locatie = "Locatie"

cfg = my_env.init_env("vdab", __file__)
logging.info("Start Application")
geolocator = Nominatim()
ns = neostore.NeoStore(cfg)
vej_file = cfg["Main"]["vej_set"]
df = pandas.read_csv(vej_file)


# Collect all known persons sets
logging.info("Collect all person nodes")
node_arr = {}
person_nodes = ns.get_nodes(lbl_person)
if person_nodes:
    for node in person_nodes:
        node_arr[node["person_id"]] = node
    logging.info("{c} person nodes found".format(c=len(node_arr)))
else:
    logging.info("No person nodes found")

# Collect all known vacatures
logging.info("Collect all locatie nodes")
loc_arr = {}
loc_nodes = ns.get_nodes(lbl_locatie)
if loc_nodes:
    for node in loc_nodes:
        loc_arr[node["plaats"]] = node
    logging.info("{c} locatie nodes found".format(c=len(loc_nodes)))
else:
    logging.info("No locatie nodes found")

loc = "locatie="
my_loop = my_env.LoopInfo("Param entries", 100)
for row in df.iterrows():
    cnt = my_loop.info_loop()
    if cnt > 217300:
        break
    # Get excel row in dict format
    xl = row[1].to_dict()
    # Check row only if urlquery is available
    urlquery = xl.pop("urlquery")
    if pandas.notnull(urlquery):
        queryparms = urlquery[1:].split("&")
        try:
            plaats = next(x for x in queryparms if x[:len(loc)] == loc)
        except StopIteration:
            # No locatie in query, take next line in excel
            continue
        plaats = unquote(plaats[len(loc):])
        try:
            loc_node = loc_arr[plaats]
        except KeyError:
            # Get lat, lon for locatie
            location = geolocator.geocode(plaats, timeout=20)
            if isinstance(location, Location):
                # Use Cypher command to create spatial properties. Not sure how to do this in py2neo.
                query = """
                CREATE (n:{lbl} {{ plaats:"{plaats}",
                        loc:point({{latitude:{lat}, longitude:{lon} }}) }})
                RETURN n
                """.format(lbl=lbl_locatie, plaats=plaats, lat=location.latitude, lon=location.longitude)
                cursor = ns.get_query(query)
                loc_node = next(cursor)["n"]
            else:
                logging.error("No geolocation found for plaats {p}".format(p=plaats))
                props = dict(
                    plaats=plaats
                )
                loc_node = ns.create_node(lbl_locatie, **props)
            loc_arr[plaats] = loc_node
        # New person?
        person_id = str(xl["id"])
        try:
            person_node = node_arr[person_id]
        except KeyError:
            logging.error("Found new person {p} for locatie {l}".format(p=person_id, l=plaats))
            props = dict(person_id=person_id)
            person_node = ns.create_node(lbl_person, **props)
            node_arr[person_id] = person_node
        ns.create_relation(person_node, "in", loc_node)
my_loop.end_loop()
logging.info("End Application")
