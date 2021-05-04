import pandas as pd
import pickle

data = pd.read_csv("./DT_code_cencus.csv")
DT_code_to_ST = dict()
ST_code_ST_name = dict()
for index, row  in data.iterrows():
    DTcode = row['DTCode']
    STCode = row['STCode']
    name = row['TownVillgName']
    if STCode not in ST_code_ST_name:
        ST_code_ST_name[STCode] = name

    if DTcode not in DT_code_to_ST:
        DT_code_to_ST[DTcode] = (STCode, ST_code_ST_name[STCode])

with open("DTCode_to_State", "wb") as f:
    pickle.dump(DT_code_to_ST, f)

with open("state_code_state_name", "wb") as f:
    pickle.dump(ST_code_ST_name, f)
