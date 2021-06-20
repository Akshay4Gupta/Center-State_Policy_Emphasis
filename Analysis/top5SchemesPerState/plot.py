import pickle, os
import numpy as np
from pprint import pprint
from tqdm import tqdm
import matplotlib.pyplot as plt
import matplotlib as mpl
from const import *
from univ import *
import argparse
parser = argparse.ArgumentParser(description='policy wise analysis',
                                       formatter_class=argparse.ArgumentDefaultsHelpFormatter)
parser.add_argument('--task', nargs='?', default=task[0], help='=' + str(task))
args = parser.parse_args()

with open("cache/coalition", 'rb') as f:
    coalition  = pickle.load(f)

# party  Alliance during UPA in center
nda_2009 = coalition['nda_2009']
upa_2009 = coalition['upa_2009']

# party  Alliance during NDA in center
nda_2014_19 = coalition['nda_2014'] + coalition['nda_2019']
upa_2014_19 = coalition['upa_2014'] + coalition['upa_2019']

with open("cache/"+args.task, 'rb') as f:
	articles = pickle.load(f)


state_type = {0: 'large_states', 1: 'medium_states', 2: 'small_states'}
states_partition = [large_states, medium_states, small_states]
proc = {0: 'upa', 1: 'nda'}

def split(term):
	res = []
	for i in range(len(term[0])):
		res.append([])
	for i in term:
		for l, j in enumerate(i):
			res[l].append(j)
	for x in range(len(res)):
		res[x] = np.array(res[x])
	return res

def annotate(graph, title, reloc):
	for p in graph:
	    width, height =p.get_width(),p.get_height()
	    x=p.get_x()+width+reloc
	    y=p.get_y()+height
	    plt.annotate(title,(x,y))

def plot_merge(coll, scheme_list, st = ''):
	global state_type, proc
	width = 0.35       # the width of the bars: can also be len(x) sequence
	n = 2
	fig, ax = plt.subplots(n, 1, sharex = True, figsize=(12,5))
	prev = [[[],[]], [[],[]]]
	plt.suptitle('comparison of schemes for aligned and non aligned govt at center state for ' + coll)
	color = ['blue', 'orange', 'red', 'purple', 'brown', 'cyan', 'green']
	y_max = 0
	total_schemes = len(scheme_list)
	for c, (scheme, state_data) in enumerate(scheme_list.items()):
		X_axis = np.arange(len(state_data[0]))
		# (states_list, upa_term, nda_term) = state_data
		for i in range(n):
			aligned, not_aligned = split(state_data[i+1])
			if len(prev[i][0]) == 0: 
				ax[i].bar(X_axis-width/2, aligned, width, label=scheme, color = color[c])
				ax[i].bar(X_axis+width/2, not_aligned, width, color = color[c])
				prev[i] = [aligned.astype('float64'), not_aligned.astype('float64')]
			else: 
				graph1 = ax[i].bar(X_axis-width/2, aligned, width, bottom=prev[i][0], label=scheme, color = color[c])
				graph2 = ax[i].bar(X_axis+width/2, not_aligned, width, bottom=prev[i][1], color = color[c])
				prev[i][0] += aligned
				prev[i][1] += not_aligned
			if c == total_schemes-1 and i == 1:
				annotate(graph1, 'A', -width/2)
				annotate(graph2, 'NA', -width)
			# comment the for loop if you don't want short state names
			# for temp in range(len(state_data[0])):
			# 	state_data[0][temp] = state_abbr[state_data[0][temp]]
		y_max = max(y_max, max([np.amax(prev[i][0]), np.amax(prev[i][1])]))
	for i in range(n):
		ax[i].set_ylim([0, y_max])
		ax[i].set_ylabel(proc[i])
		ax[i].legend(bbox_to_anchor=(1.05, 1), loc='upper left', borderaxespad=0.)
		ax[i].set_xticks(X_axis)
		ax[i].set_xticklabels(state_data[0], rotation=90)
	plt.tight_layout()
	# fig.set_figwidth(12)
	# fig.set_figheight(5)
	if st == '':
		path = 'output_graphs/v1/undivided/'
		make_dir(path)
		plt.savefig(path+coll+'.png')
	else:
		path = 'output_graphs/v1/divided/'
		make_dir(path)
		plt.savefig(path+coll+str(st)+'.png')
	plt.clf()

def plot(coll, scheme_list, st = ''):
	global state_type, proc
	width = 0.35       # the width of the bars: can also be len(x) sequence
	n = 2
	color = ['blue', 'orange', 'red', 'purple', 'brown', 'cyan', 'green']
	for c, (scheme, state_data) in enumerate(scheme_list.items()):
		fig, ax = plt.subplots(n, 1, sharex = True, figsize=(12,5))
		plt.suptitle(coll + ' comparison of schemes for aligned and non aligned govt at center state for ' + scheme)
		y_max = 0
		# (states_list, upa_term, nda_term) = state_data
		X_axis = np.arange(len(state_data[0]))
		for i in range(n):
			aligned, not_aligned = split(state_data[i+1])
			ax[i].bar(X_axis-width/2, aligned, width, label='aligned with ' + proc[i])
			ax[i].bar(X_axis+width/2, not_aligned, width, label='not aligned with ' + proc[i])
			# comment the for loop if you don't want short state names
			# for temp in range(len(state_data[0])):
			# 	state_data[0][temp] = state_abbr[state_data[0][temp]]
			y_max = max(y_max, max(max(aligned), max(not_aligned)))
		for i in range(n):
			ax[i].set_ylim([0, y_max])
			ax[i].set_ylabel(proc[i])
			ax[i].legend(bbox_to_anchor=(1.05, 1), loc='upper left', borderaxespad=0.)
			ax[i].set_xticks(X_axis)
			ax[i].set_xticklabels(state_data[0], rotation=90)
		plt.tight_layout()
		# fig.set_figwidth(12)
		# fig.set_figheight(5)

		if st == '':
			path = 'output_graphs/v1/undivided/'+coll+'/'
			make_dir(path)
			plt.savefig(path+scheme+'.png')
		else:
			path = 'output_graphs/v1/divided/'+coll+'/'
			make_dir(path)
			plt.savefig('output_graphs/v1/divided/'+coll+'/'+scheme+str(st)+'.png')
		plt.clf()

def plot_sentiment(coll, scheme_list):
	global state_type, proc
	width = 0.35       # the width of the bars: can also be len(x) sequence
	n = 2
	color = ['blue', 'orange', 'red', 'purple', 'brown', 'cyan', 'green']
	for c, (scheme, state_data) in enumerate(scheme_list.items()):
		fig, ax = plt.subplots(n, 1, sharex = True)
		plt.title(coll + ' comparision UPA v/s NDA of ' + scheme)
		y_max = 0
		# (states_list, upa_term, nda_term) = state_data
		X_axis = np.arange(len(state_data[0]))
		for i in range(n):
			# shape = no_of_states, aligned/not_aligned, pos/neg
			temp = np.array(state_data[i+1])
			aligned_pos, not_aligned_pos = temp[:,0,0], temp[:,1,0]
			aligned_neg, not_aligned_neg = temp[:,0,1], temp[:,1,1]
			ax[i].bar(X_axis-width/2, aligned_pos, width, label='aligned with ' + proc[i])
			ax[i].bar(X_axis-width/2, aligned_neg, width, label='aligned with ' + proc[i])
			ax[i].bar(X_axis+width/2, not_aligned_pos, width, label='not aligned with ' + proc[i])
			ax[i].bar(X_axis+width/2, not_aligned_neg, width, label='not aligned with ' + proc[i])
			# comment the for loop if you don't want short state names
			# for temp in range(len(state_data[0])):
			# 	state_data[0][temp] = state_abbr[state_data[0][temp]]
			y_max = max(y_max, max([np.amax(aligned_pos), np.amax(aligned_neg), np.amax(not_aligned_pos), np.amax(not_aligned_neg)]))
		for i in range(n):
			ax[i].set_ylim([0, y_max])
			ax[i].set_ylabel(proc[i])
			ax[i].legend(bbox_to_anchor=(1.05, 1), loc='upper left', borderaxespad=0.)
			ax[i].set_xticks(X_axis)
			ax[i].set_xticklabels(state_data[0], rotation=90)
		plt.tight_layout()
		fig.set_figwidth(12)
		fig.set_figheight(5)
		path = 'output_graphs/v1/'+coll+'/sentiment/'
		make_dir(path)
		plt.savefig(path + scheme+'.png')
		plt.clf()

def state_or_scheme_plot_divided():
	global articles, state_type, states_partition, proc, nda_2009, nda_2014_19, upa_2009, upa_2014_19
	for coll, schemes in tqdm(articles.items(), desc = 'schemes'):
		scheme_list = {}
		for scheme_name, states in schemes.items():
			for st in state_type:
				states_list = []
				nda_term, upa_term = [], []
				if st not in scheme_list:
					scheme_list[st] = {}
				for state in states_partition[st]:
					upa_state, nda_state = [np.zeros(2, dtype='float64'), np.zeros(2, dtype='float64')], [np.zeros(2, dtype='float64'), np.zeros(2, dtype='float64')] # [upa, nda, others]
					total = np.zeros(2, dtype='float64')
					for key, val in states[state].items():
						c, s = key.split(" : ")
						total += val
						if c.strip().lower() == 'inc':
							if s.strip().lower() in upa_2009: upa_state[0] += val
							else: upa_state[1] += val
						elif c.strip().lower() == 'bjp':
							if s.strip().lower() in nda_2014_19: nda_state[0] += val
							else: nda_state[1] += val
					if state in large_states or state in medium_states or state in small_states or state in UT:
						states_list.append(state)
						if upa_state[0][1]: upa_state[0][0] /= upa_state[0][1]/365.25
						if nda_state[0][1]: nda_state[0][0] /= nda_state[0][1]/365.25
						if upa_state[1][1]: upa_state[1][0] /= upa_state[1][1]/365.25
						if nda_state[1][1]: nda_state[1][0] /= nda_state[1][1]/365.25
						upa_state = [upa_state[0][0], upa_state[1][0]]
						nda_state = [nda_state[0][0], nda_state[1][0]]
						upa_term.append(upa_state)
						nda_term.append(nda_state)
				scheme_list[st][scheme_name] = [states_list, upa_term, nda_term]
		for st in scheme_list:
			plot_merge(coll, scheme_list[st], st)

def sentiment_plot():
	global articles
	for coll, schemes in tqdm(articles.items(), desc = 'schemes', leave = False):
		scheme_list = {}
		# delete
		if coll != 'humanDevelopment': continue
		for scheme_name, states in schemes.items():
			states_list = []
			nda_term, upa_term = [], []
			for state, party_range in states.items():
				upa_state, nda_state = [np.zeros(3, dtype='float64'), np.zeros(3, dtype='float64')], [np.zeros(3, dtype='float64'), np.zeros(3, dtype='float64')] # [upa, nda, others]
				total = np.zeros(3, dtype='float64')
				if len(party_range) == 0: continue
				for key, val in party_range.items():
					c, s = key.split(" : ")
					total += val
					if c.strip().lower() == 'inc':
						if s.strip().lower() in upa_2009: upa_state[0] += val
						else: upa_state[1] += val
					elif c.strip().lower() == 'bjp':
						if s.strip().lower() in nda_2014_19: nda_state[0] += val
						else: nda_state[1] += val
				if state in large_states or state in medium_states or state in small_states or state in UT:
					states_list.append(state)
					if upa_state[0][2]: 
						upa_state[0][0] /= upa_state[0][2]/365.25
						upa_state[0][1] /= upa_state[0][2]/365.25
					if nda_state[0][2]: 
						nda_state[0][0] /= nda_state[0][2]/365.25
						nda_state[0][1] /= nda_state[0][2]/365.25
					if upa_state[1][2]: 
						upa_state[1][0] /= upa_state[1][2]/365.25
						upa_state[1][0] /= upa_state[1][2]/365.25
					if nda_state[1][2]: 
						nda_state[1][0] /= nda_state[1][2]/365.25
						nda_state[1][0] /= nda_state[1][2]/365.25
					upa_state = [upa_state[0][:2], upa_state[1][:2]]
					nda_state = [nda_state[0][:2], nda_state[1][:2]]
					upa_term.append(upa_state)
					nda_term.append(nda_state)
			scheme_list[scheme_name] = [states_list, upa_term, nda_term]
		plot_sentiment(coll, scheme_list)	
		break

if __name__ == '__main__':
	if args.task == task[0]:
		state_or_scheme_plot_divided()
		# state_or_scheme_plot()
	elif args.task == task[4]:
		sentiment_plot()
