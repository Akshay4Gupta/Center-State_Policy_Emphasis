from freq import print_stats
import pickle

with open('all_stats.pkl', 'rb') as file:
    all_stats = pickle.load(file)
    print_stats(all_stats)
