from pymongo import MongoClient
import sys, os
from glob import glob
sys.path.append(os.path.abspath("../freq/"))
from freq import get_schemes
from pprint import pprint

client = MongoClient('localhost', 27017)
db = client['media-db2']
schemes_path = "../schemes/"


if __name__ == "__main__":
	schemes_clubbed = get_schemes(schemes_path)
	for coll, schemes in schemes_clubbed.items():
		nested_schemes = list(schemes.values())
		schemes = []

		for scheme in nested_schemes:
			schemes.extend(scheme)
		collection = db[coll+'_schemes']
		# cursor = collection.find({'related_scheme':{'$exists':False}}, no_cursor_timeout = True)
		cursor = collection.find({}, no_cursor_timeout = True)
		print(coll, cursor.count())
		for article in cursor:
			text = article['text']
			related_schemes = article['related_schemes'] if 'related_schemes' in article else []
			related_schemes = set(related_schemes)
			for scheme in schemes:
				if scheme.lower() in text.lower():
					related_schemes.add(scheme)
			collection.update_one({"_id": article['_id']}, {"$set": {'related_schemes': list(related_schemes)}}, upsert = False)
		cursor.close()
