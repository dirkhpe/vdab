"""
This procedure will rebuild the neo4j vdab database
"""

import logging
from lib import my_env
from lib.neostore import NeoStore


cfg = my_env.init_env("vdab", __file__)
logging.info("Start application")
# Get Neo4J Connection and clean Database
ns = NeoStore(cfg, refresh="Yes")
logging.info("Neo4J store clean")
logging.info("End application")
