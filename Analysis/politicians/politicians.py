from py2neo import Graph, Node, Relationship
import sys

databases = {'ictd_database': '10.237.27.60'}

database = databases[sys.argv[1]]
graph = Graph('bolt://neo4j:neo4j@'+database+':1337')

# print(graph.nodes.match("politician").limit(2))
