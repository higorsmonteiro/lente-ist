'''

'''
import sys
sys.path.append("..")

from pathlib import Path
import numpy as np
import pandas as pd
import datetime as dt
from dotenv import dotenv_values
from simpledbf import Dbf5
from lente_ist import WarehouseIST
from lente_ist.utils import select_folder

expected_files = [
    Path("AIDSANET.DBF"),
    Path("AIDSCNET.DBF")
]

msg1 = "Selecionar Pasta Contendo os Arquivos DBF"
msg2 = "Selecionar Pasta Para Dados Processados"
basefolder = Path(select_folder(msg=msg1))
warehouse_location = Path(select_folder(msg=msg2))
warehouse_name = "ist_pessoa.db"

#env_vars = dotenv_values(Path.cwd().parent.joinpath(".env"))
#warehouse_location = Path(env_vars.get("WAREHOUSE_LOCATION"))
#warehouse_name = env_vars.get("WAREHOUSE_DB_NAME")

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
for current_file in expected_files:
    source_name = current_file.stem
    if basefolder.joinpath(current_file).exists():
        # -- load and preprocess the data for injection
        data_df = Dbf5(basefolder.joinpath(current_file), codec='latin').to_dataframe()

        # -- process the unique ID of the database
        notific_fmt = data_df["DT_NOTIFIC"].apply(lambda x: f"{x.day:2.0f}{x.month:2.0f}{x.year}".replace(" ", "0"))
        id_municip_fmt = data_df["ID_MUNICIP"].apply(lambda x: f"{x}")
        # -- ID_SINAN: ID_AGRAVO+NU_NOTIFIC+ID_MUNICIP+DT_NOTIFIC
        data_df["ID_SINAN"] = data_df["ID_AGRAVO"]+data_df["NU_NOTIFIC"]+id_municip_fmt+notific_fmt
        data_df["FONTE"] = [ source_name for n in range(data_df.shape[0]) ]

        # -- check whether a ID already exists in the db
        if not delete_before:
            min_year, max_year = data_df["DT_NOTIFIC"].min().year, data_df["DT_NOTIFIC"].max().year
            list_of_ids = []
            for current_year in np.arange(min_year, max_year+1, 1):
                list_of_ids += [ pd.DataFrame(warehouse.query_id('sinan_pessoa', current_year)) ]
            list_of_ids = pd.concat(list_of_ids)
            if list_of_ids.shape[0]>0:
                list_of_ids = list_of_ids["ID_SINAN"]
            # -- remove from the dbf the records already present in the database
            data_df = data_df[~data_df["ID_SINAN"].isin(list_of_ids)].copy()

        # -- insert records to a given table
        print(f"{data_df.shape[0]} new records to be added to the database ... ", end='')
        warehouse.insert('sinan_pessoa', data_df, batchsize=200, verbose=False)
        if source_name=="AIDSANET":
            warehouse.insert('sinan_aids_adulto_info', data_df, batchsize=200, verbose=False)
        elif source_name=="AIDSCNET":
            warehouse.insert('sinan_aids_crianca_info', data_df, batchsize=200, verbose=False)
        print("done.")
        # -- we could create different tables for different types of data - for instance: aidsanet
        #warehouse.insert('sinan_info_adulto', data_df(tem que ser aidsanet), batchsize=200, verbose=False)