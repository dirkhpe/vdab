"""
This script will link session IDs to vacatures.
"""

import logging
import pandas
from lib import my_env
from lib import neostore

# Node Labels
lbl_person = "Person"
lbl_vacature = "Vacature"

cfg = my_env.init_env("vdab", __file__)
logging.info("Start Application")
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
logging.info("Collect all vacature nodes")
vac_arr = {}
vac_nodes = ns.get_nodes(lbl_vacature)
if vac_nodes:
    for node in vac_nodes:
        vac_arr[node["vac_id"]] = node
    logging.info("{c} vacature nodes found".format(c=len(vac_nodes)))
else:
    logging.info("No vacature nodes found")

my_loop = my_env.LoopInfo("Param entries", 20)
for row in df.iterrows():
    cnt = my_loop.info_loop()
    """
    if cnt > 15:
        break
    """
    # Get excel row in dict format
    xl = row[1].to_dict()
    # Check row only if urlpath in format /api/vindeenjob/vacatures/vacature_id
    urlpath = xl.pop("urlpath")
    if pandas.notnull(urlpath):
        path_arr = urlpath.split("/")
        if len(path_arr) == 5:
            vac_id = path_arr[4]
            if vac_id.isdigit():
                # OK - vacature has been found
                # New vacature?
                vac_id = str(vac_id)
                try:
                    vac_node = vac_arr[vac_id]
                except KeyError:
                    props = dict(vac_id=vac_id)
                    vac_node = ns.create_node(lbl_vacature, **props)
                    vac_arr[vac_id] = vac_node
                # New person?
                person_id = str(xl["id"])
                try:
                    person_node = node_arr[person_id]
                except KeyError:
                    props = dict(person_id=person_id)
                    person_node = ns.create_node(lbl_person, **props)
                    node_arr[person_id] = person_node
                ns.create_relation(person_node, "interest", vac_node)
my_loop.end_loop()
logging.info("End Application")
