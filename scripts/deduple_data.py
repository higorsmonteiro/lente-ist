'''

'''
import sys
sys.path.append("..")

from pathlib import Path
import numpy as np
import pandas as pd
import datetime as dt
from simpledbf import Dbf5
from lente_ist import WarehouseIST, DedupleAgent
from lente_ist.utils import select_folder

msg2 = "Selecionar Pasta Para Dados Processados"
#warehouse_location = Path(select_folder(msg=msg2))
warehouse_location = Path.home().joinpath("Documents", "data", "sinan", "aids", "SQL_WAREHOUSE")
path_to_models = Path.cwd().parent.joinpath("lente_ist", "ml_models")
warehouse_name = "ist_pessoa.db"

# -- create interface for command line arguments
# ---- 1. ...
# ---- 2. ...
# ---- 3. ...
# ---- 4. ...
# ---- 5. ...

agent = DedupleAgent(warehouse_location, warehouse_name, path_to_models, field_id="ID")
agent.retrieve_compared_pairs().perform(number_of_blocks=10)