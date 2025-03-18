'''
    ...
'''
import sys
sys.path.append("..")

from pathlib import Path
import numpy as np
import pandas as pd
import datetime as dt
from simpledbf import Dbf5
from lente_ist import WarehouseHIV, InjectorHIV
from lente_ist.utils import select_folder

msg1 = "Selecionar Pasta Contendo os Arquivos Para Processamento"
msg2 = "Selecionar Pasta Para Armazenamento Final"
basefolder = Path(select_folder(msg=msg1))
#basefolder = Path.home().joinpath("Documents", "data", "sinan", "aids", "DBF")
warehouse_location = Path(select_folder(msg=msg2))
#warehouse_location = Path.home().joinpath("Documents", "data", "sinan", "aids", "SQL_WAREHOUSE")
warehouse_name = "hiv_aids_paciente_18mar25.db"

# -- create interface for command line arguments
# ---- 1. ...
# ---- 2. ...
# ---- 3. ...
# ---- 4. ...
# ---- 5. ...

delete_before = True
injector = InjectorHIV(source_data_location=basefolder, warehouse_location=warehouse_location, warehouse_name=warehouse_name)
if delete_before:
    injector.drop_tables()
injector.inject_data(verbose=True)
