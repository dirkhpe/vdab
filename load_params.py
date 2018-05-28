"""
This script will load the known parameters and load them into the Neo4J database.
"""

import logging
import pandas
from lib import my_env
from lib import neostore

cfg = my_env.init_env("vdab", __file__)
logging.info("Start Application")
ns = neostore.NeoStore(cfg)
vej_file = cfg["Main"]["vej_params"]
df = pandas.read_excel(vej_file, skiprows=1)

node_arr = {}

my_loop = my_env.LoopInfo("Param definitions", 20)
for row in df.iterrows():
    cnt = my_loop.info_loop()
    # Get excel row in dict format
    xl = row[1].to_dict()
    param_name = xl["ParameterNaam"].lower()
    param_val = str(xl["ParameterWaarde"]).lower()
    param_def = xl["Functionele definitie"].lower()
    param = "{n}={v}".format(n=param_name, v=param_val)
    try:
        param_node = node_arr[param]
    except KeyError:
        props = dict(waarde=param_val, definitie=param_def)
        param_node = ns.create_node(param_name, **props)
        node_arr[param] = param_node
my_loop.end_loop()
