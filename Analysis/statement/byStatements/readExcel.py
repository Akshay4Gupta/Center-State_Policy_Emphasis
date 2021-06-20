import os, sys, argparse, json
import pandas as pd
from datetime import datetime

now = datetime.now()
parser = argparse.ArgumentParser(description='Extract top5 policies per state',
                                       formatter_class=argparse.ArgumentDefaultsHelpFormatter)
parser.add_argument('--path', nargs='?', default='schemes/', help='path where the schemes to be evaluated are stored')
parser.add_argument('--output', nargs='?', default='output/', help='relative path where you want to store the output')
parser.add_argument('--typ', nargs='?', default='mla', help='relative path where you want to store the output')
args = parser.parse_args()

years = ['2009', '2014', '2019']
sheets = ['agriculture.xlsx', 'health.xlsx', 'humanDevelopment.xlsx']
scheme_names = ['agriculture', 'health_hygiene', 'humanDevelopment']
def MPs(year, typ):
	readFrom = '../../'+ typ +'/output/' + typ + year + '.xlsx'
	print(readFrom)
	reader = pd.read_excel(readFrom, sheet_name = sheets)
	for idx, sheet in enumerate(sheets):
		for i in range(1, 6):
			for j in range(reader[sheet][i].shape[0]):
				if not pd.isna(reader[sheet][i][j]): reader[sheet][i][j] = {eval(reader[sheet][i][j])[0]['Name '].strip(): eval(reader[sheet][i][j])[3]['Alliance'].strip()}
		reader[scheme_names[idx]] = reader.pop(sheet)
	return reader

# df = MPs(years[0])
# print(df)
# print(df[scheme_names[0]][0].to_list())