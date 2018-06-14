"""
This class consolidates functions related to the neo4J datastore.
"""

import logging
import sys
from pandas import DataFrame
from py2neo import Graph, Node, Relationship, NodeMatcher


class NeoStore:

    def __init__(self, config, refresh='No'):
        """
        Method to instantiate the class in an object for the neostore module.

        :param config object, to get connection parameters.

        :param refresh: If Yes, then database will be made empty.

        :return: Object to handle neostore commands.
        """
        logging.debug("Initializing Neostore object")
        self.config = config
        self.graph = self._connect2db()
        if refresh == 'Yes':
            self._delete_all()
        self.matcher = NodeMatcher(self.graph)
        return

    def _connect2db(self):
        """
        Internal method to create a database connection. This method is called during object initialization.

        :return: Database handle and cursor for the database.
        """
        logging.debug("Creating Neostore object.")
        neo4j_config = {
            'user': self.config['Graph']['username'],
            'password': self.config['Graph']['password'],
        }
        # Connect to Graph
        graph = Graph(**neo4j_config)
        # Check that we are connected to the expected Neo4J Store - to avoid accidents...
        dbname = graph.database.name
        if dbname != self.config['Graph']['neo_db']:
            logging.fatal("Connected to Neo4J database {d}, but expected to be connected to {n}"
                          .format(d=dbname, n=self.config['Graph']['neo_db']))
            sys.exit(1)
        return graph

    def create_node(self, *labels, **props):
        """
        Function to create node. The function will return the node object.

        :param labels: Labels for the node

        :param props: Value dictionary with values for the node.

        :return: node object
        """
        logging.debug("Trying to create node with params {p}".format(p=props))
        component = Node(*labels, **props)
        self.graph.create(component)
        return component

    def create_relation(self, from_node, rel, to_node):
        """
        Function to create relationship between nodes. If the relation exists already, it will not be created again.

        :param from_node:

        :param rel:

        :param to_node:

        :return:
        """
        rel = Relationship(from_node, rel, to_node)
        self.graph.merge(rel)
        return

    def _delete_all(self):
        """
        Function to remove all nodes and relations from the graph database.
        Then create calendar object.

        :return:
        """
        logging.info("Remove all nodes and relations from database.")
        self.graph.delete_all()
        return

    def get_nodes(self, *labels, **props):
        """
        This method will select all nodes that have labels and properties

        :param labels:

        :param props:

        :return: list of nodes that fulfill the criteria, or False if no nodes are found.
        """
        nodes = self.matcher.match(*labels, **props)
        nodelist = list(nodes)
        if len(nodelist) == 0:
            # No nodes found that fulfil the criteria
            return False
        else:
            return nodelist

    def get_query(self, query):
        """
        This function will run a query and return the result as a cursor.

        :param query:

        :return: cursor containing the query result
        """
        return self.graph.run(query)

    def get_query_as_df(self, query):
        """
        This function will run a query and return the result as a dataframe.

        :param query:

        :return: Dataframe as result
        """
        return DataFrame(self.graph.data(query))
