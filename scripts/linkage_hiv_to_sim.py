'''

'''
import sys
sys.path.append("..")

from pathlib import Path
import numpy as np
import pandas as pd
import datetime as dt
from simpledbf import Dbf5
from sqlalchemy import create_engine
from lente_ist import WarehouseHIV, DedupleAgent, LinkageRecordsHIV
from lente_ist.utils import select_folder

msg2 = "Selecionar Folder do Banco de Dados"
warehouse_location = Path(select_folder(msg=msg2))
#warehouse_location = Path.home().joinpath("Documents", "data", "sinan", "aids", "SQL_WAREHOUSE")
path_to_models = Path.cwd().parent.joinpath("lente_ist", "ml_models")
warehouse_name = "hiv_aids_paciente_18mar25.db"

#engine_url = f"sqlite:///{warehouse_location.joinpath(database_name)}"
#db_engine = create_engine(engine_url)

agent = LinkageRecordsHIV(warehouse_location, warehouse_name, path_to_models, field_id="ID")
list_of_fracs = [0.3, 0.3, 0.3, 0.4, 0.4, 0.4, 0.5, 0.5, 0.5, 0.6]
for current_frac in list_of_fracs:
    print(f'current fraction: {current_frac}')
    agent.perform_hiv_to_sim(number_of_blocks=10, frac=current_frac)