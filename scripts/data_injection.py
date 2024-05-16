from pathlib import Path
import os
import sys
sys.path.append("..")

print(Path.cwd().parent)

import numpy as np
import pandas as pd
import datetime as dt
from dotenv import dotenv_values
from simpledbf import Dbf5

from lente_ist import WarehouseIST

env_vars = dotenv_values(Path.cwd().parent.joinpath(".env"))
warehouse_location = Path(env_vars.get("WAREHOUSE_LOCATION"))
warehouse_name = env_vars.get("WAREHOUSE_DB_NAME")

# -- create interface for command line arguments
# ---- 1. ...
# ---- 2. ...
# ---- 3. ...
# ---- 4. ...
# ---- 5. ...

delete_before = True

# ---------- Database Connection ----------
dbpath = warehouse_location.joinpath(warehouse_name)
engine_url = f"sqlite:///{dbpath}"

if delete_before:
    warehouse = WarehouseIST(engine_url)
    engine = warehouse.db_init()
    warehouse.delete_table('sinan_pessoa', is_sure=True, authkey="###!Y!.")
    #warehouse.delete_table('sinan_info', is_sure=True, authkey="###!Y!.")

warehouse = WarehouseIST(engine_url)
engine = warehouse.db_init()

# ---------- Inject Data ----------

# -- folder where downloaded data is stored
filename = "AIDSANET.DBF"
basefolder = Path.home().joinpath("Documents", "data", "sinan", "aids", "FILE_FOR_DB")

data_df = Dbf5(basefolder.joinpath(filename), codec='latin').to_dataframe()

notific_fmt = data_df["DT_NOTIFIC"].apply(lambda x: f"{x.day:2.0f}{x.month:2.0f}{x.year}".replace(" ", "0"))
id_municip_fmt = data_df["ID_MUNICIP"].apply(lambda x: f"{x}")
data_df["ID_SINAN"] = data_df["ID_AGRAVO"]+data_df["NU_NOTIFIC"]+id_municip_fmt+notific_fmt

min_year, max_year = data_df["DT_NOTIFIC"].min().year, data_df["DT_NOTIFIC"].max().year
list_of_ids = []
for current_year in np.arange(min_year, max_year+1, 1):
    list_of_ids += [ pd.DataFrame(warehouse.query_id('sinan_pessoa', current_year)) ]
list_of_ids = pd.concat(list_of_ids)
if list_of_ids.shape[0]>0:
    list_of_ids = list_of_ids["ID_SINAN"]

# -- remove from the dbf the records already present in the database
data_df_new = data_df[~data_df["ID_SINAN"].isin(list_of_ids)].copy()
print(f"{data_df_new.shape[0]} new records to be added to the database ... ", end='')
# -- insert records
warehouse.insert('sinan_pessoa', data_df_new, batchsize=200, verbose=False)
#warehouse.insert('sinan_info', data_df_new, batchsize=200, verbose=False)