# imports
import os
import sys
import pickle
from tqdm import tqdm
from pprint import pprint
from pymongo import MongoClient
import pandas as pd
import matplotlib.pyplot as plt
sys.path.append(os.path.abspath("../freq/"))
from freq import get_schemes

# initialization
path = 'schemes/'
output = 'output/'

# definations
def schemes_per_state(collection, schemes):
    # (str, pymongo.collection.Collection, dict) -> dict
    stats = {}
    for scheme_name, keywords_list in schemes.items():
        keywords = '|'.join(keywords_list)
        scheme_articles = collection.find({'$and':[{'text': {'$regex': keywords, '$options': 'i'}}]},
                            no_cursor_timeout=True)
        # scheme_state = {}
        for art in scheme_articles:
            if 'states' in art:
                for state in art['states']:
                    if state not in stats:
                        stats[state] = {}
                    if scheme_name not in stats[state]:
                        stats[state][scheme_name] = 0
                    stats[state][scheme_name] += 1
    return stats

def get_stats():
    backup = 'all_stats.pkl'
    if len(sys.argv) >= 2 and sys.argv[1].lower()[0] == 'n':
        os.remove(output+backup)
    if os.path.exists(output+backup):
        with open(output+backup, 'rb') as file:
            all_stats = pickle.load(file)
            return all_stats
    else:
        schemes_clubbed = get_schemes(path)

        client = MongoClient('localhost', 27017)
        db = client['media-db2']

        all_stats = {}
        for coll in schemes_clubbed:
            collName = coll + '_schemes'
            collection = db[collName]
            all_stats[coll] = schemes_per_state(collection, schemes_clubbed[coll])
        if not os.path.exists(output):
            os.mkdir(output)
        with open(output+backup, 'wb') as file:
            pickle.dump(all_stats, file)
        return all_stats

def correct_and_save_xlsx(df, writer):
    workbook = writer.book
    wrap_format = workbook.add_format({'text_wrap': False, 'align': 'left'})

    for scheme in df:
        df[scheme].to_excel(writer, sheet_name=scheme, index = False)
        worksheet = writer.sheets[scheme]
        worksheet.set_column(0, len(df[scheme].columns) - 1, cell_format=wrap_format)
        for j, col in enumerate(df[scheme].columns):
            column_len = max(df[scheme][col].astype(str).str.len().max(), len(str(col)) + 2)
            worksheet.set_column(j, j, column_len)
        worksheet.freeze_panes(1, 0)
    writer.save()

def getPerCapitaCoverage():
    all_stats = get_stats()

    writer = pd.ExcelWriter(output+'top5_perCapita.xlsx', engine='xlsxwriter')
    workbook = writer.book
    wrap_format = workbook.add_format({'text_wrap': False, 'align': 'left'})

    df = pd.read_excel(output+'top5_perCapita.xlsx', sheet_name = ['Population', 'MP', 'MLA'])
    for key in df:
        # df[key].sort_values(by = ["State"], ascending = [True], inplace = True)
        # df[key]['State'] = df[key]['State'].str.lower()
        df[key].to_excel(writer, sheet_name=key, index = False)

    for scheme, per_scheme in all_stats.items():
        per_scheme = {key: val for key, val in sorted(per_scheme.items(), key = lambda ele: ele[0], reverse = True)}
        sheet = []
        for keys in per_scheme:
            sheet.append([keys])
            total = sum(per_scheme[keys].values())
            sheet[-1].append(total)
            try:
                percapita = df['Population'][df['Population']['State'] == keys]['total'].values[0]/1000000
                perMLA = df['MLA'][df['MLA']['State'] == keys]['total'].values[0]
                perMP = df['MP'][df['MP']['State'] == keys]['total'].values[0]
                sheet[-1].append(round(total/percapita, 2))
                sheet[-1].append(round(total/perMLA, 2))
                sheet[-1].append(round(total/perMP, 2))
            except:
                while(len(sheet[-1]) < 5):
                    sheet[-1].append(0)
        df[scheme] = pd.DataFrame(sheet, columns = ['State', 'total Articles', 'perCapitaCoverage', 'perMLACoverage', 'perMPCoverage'])
        df[scheme].sort_values(by = ["total Articles"], ascending = [False], inplace = True)
    correct_and_save_xlsx(df, writer)

def printExcel():
    all_stats = get_stats()

    writer = pd.ExcelWriter(output+'top5.xlsx', engine='xlsxwriter')
    df = {}
    for scheme, per_scheme in all_stats.items():
        # print(per_scheme['uttar pradesh'])
        per_scheme = {key: val for key, val in sorted(per_scheme.items(), key = lambda ele: ele[0], reverse = True)}
        sheet = []
        for keys in per_scheme:
            sheet.append([keys])
            per_scheme[keys] = sorted(per_scheme[keys].items(), key = lambda ele: ele[1], reverse = True)[:5]
            for j, x in enumerate(per_scheme[keys]):
                sheet[-1].append(x[0])
                sheet[-1].append(x[1])
            while(len(sheet[-1]) < 11):
                sheet[-1].append("")

        # Saving to excel and formating the sheet
        df[scheme] = pd.DataFrame(sheet, columns = ['State', 1, "total1", 2, "total2", 3, "total3", 4, "total4", 5, "total5"])
        df[scheme].sort_values(by = ["total1", "total2", "total3", "total4", "total5"], ascending = [False]*5, inplace = True)
    correct_and_save_xlsx(df, writer)

# main
if __name__ == '__main__':
    print('python3 top5.py new - to process it again')
    choice = input('enter "p" for per capita coverage stats and "o" for schemes stats else will exit: ').lower()[0]
    if(choice == 'o'):
        printExcel()
    elif(choice == 'p'):
        getPerCapitaCoverage()
    else:
        pprint(get_stats())
