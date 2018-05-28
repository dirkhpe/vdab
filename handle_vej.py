"""
This script will read the VEJ file and convert it to a Neo4J database.
"""

import logging
import pandas
from lib import my_env
from lib import neostore

whitelist = ["arbeidsregime", "jobdomein", "ervaring", "arbeidscircuit", "arbeidsduur", "internationaal",
             "diplomaniveau", "sinds", "vakgebieden", "doelgroepen", "leervorm", "lesvorm", "organisator",
             "knelpuntberoep"]
blacklist = ["ts", "offset", "limit", "expand"]

# Node Labels
lbl_person = "Person"

cfg = my_env.init_env("vdab", __file__)
logging.info("Start Application")
ns = neostore.NeoStore(cfg)
vej_file = cfg["Main"]["vej_set"]
df = pandas.read_csv(vej_file)

node_arr = {}

# Collect all known parameter sets
param_nodes = ns.get_nodes()
for node in param_nodes:
    lbl = list(node.labels())[0]
    val = node["waarde"]
    param = "{l}={v}".format(l=lbl, v=val)
    node_arr[param] = node

my_loop = my_env.LoopInfo("Param entries", 20)
for row in df.iterrows():
    cnt = my_loop.info_loop()
    """
    if cnt > 10:
        break
    """
    # Get excel row in dict format
    xl = row[1].to_dict()
    # Check row only if urlquery is available
    urlquery = xl.pop("urlquery")
    if pandas.notnull(urlquery):
        query2process = urlquery[1:].lower()
        # Get node for Person
        person_id = str(xl["id"])
        try:
            person_node = node_arr[person_id]
        except KeyError:
            props = dict(person_id=person_id)
            person_node = ns.create_node(lbl_person, **props)
            node_arr[person_id] = person_node
        # Investigate Query
        queryparms = query2process.split("&")
        for param in queryparms:
            param_name, param_value = param.split("=")
            if param_name in whitelist:
                try:
                    param_node = node_arr[param]
                except KeyError:
                    logging.error("Param {p} not found...".format(p=param))
                    props = dict(waarde=param_value, definitie=param_value)
                    param_node = ns.create_node(param_name, **props)
                    node_arr[param] = param_node
                ns.create_relation(person_node, "to", param_node)
my_loop.end_loop()
logging.info("End Application")
