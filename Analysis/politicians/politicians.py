from py2neo import Graph, Node, Relationship
import sys

databases = {'ictd': '10.237.27.60', 'localhost': 'localhost'}

database = databases[sys.argv[1]]
graph = Graph('bolt://'+database+':1337')

# print(graph.nodes.match("politician").limit(2))
