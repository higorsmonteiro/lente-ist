'''

'''
import joblib
from pathlib import Path
import pandas as pd
import numpy as np
import datetime as dt
from tqdm import tqdm
from lente_ist import WarehouseIST
from lente_ist.process_layer.sus_specific import ProcessSinan
from lente_ist.data_matching.matching_data import Deduple

class DedupleAgent:
    '''

    '''
    def __init__(self, warehouse_location, warehouse_name, path_to_models, field_id="ID"):
        self.warehouse_location = warehouse_location
        self.warehouse_name = warehouse_name
        self.field_id = field_id

        self.engine_url = f"sqlite:///{self.warehouse_location.joinpath(self.warehouse_name)}"
        self.warehouse = WarehouseIST(self.engine_url)
        self.engine = self.warehouse.db_init()
        self.current_pairs = []
        self.deduple = None

        self.models = {
            "GBT": joblib.load(Path(path_to_models).joinpath("GRADBOOST_SIVEP04SET2023.joblib")),
            "RNF": joblib.load(Path(path_to_models).joinpath("RANDFOREST_SIVEP04SET2023.joblib")),
            "LGT": joblib.load(Path(path_to_models).joinpath("LOGITREG_SIVEP04SET2023.joblib")),
        }

    def retrieve_compared_pairs(self):
        '''
            Retrieve the pairs that were already compared previously.
        '''
        # -- 
        query_pairs = pd.DataFrame( self.warehouse.query_all(table_name='pairs_label') )
        if query_pairs.shape[0]>0:
            query_pairs = query_pairs[[f"{self.field_id}_1", f"{self.field_id}_2"]].copy()
        else:
            query_pairs = []

        if len(query_pairs):
            self.current_pairs = list(query_pairs.itertuples(index=False, name=None))
        return self
    
    def perform(self, period=(dt.datetime(2000, 1, 1), dt.datetime.today()), number_of_blocks=1):
        '''

        '''
        query_data = pd.DataFrame( self.warehouse.query_period(table_name='pessoa', date_col="DATA_NOTIFICACAO", period=period) )

        # -- process the data for standardization
        processor = ProcessSinan(query_data, self.field_id)
        processor.basic_standardize().specific_standardize()
        processed_data = processor.data

        # -- build the similarity matrix
        self.deduple = Deduple(processed_data, left_id=self.field_id, env_folder=None)
        linkage_vars = [
            ("cpf", "cpf", "exact", 'cpf'),
            ("cns", "cns", "exact", 'cns'),
            ("cep", "cep", "exact", 'cep'),
            ("sexo", "sexo", "exact", 'sexo'),
            ("bairro", "bairro", "string", 'bairro'),
            ("nascimento_dia", "nascimento_dia", "exact", 'nascimento_dia'),
            ("nascimento_mes", "nascimento_mes", "exact", 'nascimento_mes'),
            ("nascimento_ano", "nascimento_ano", "exact", 'nascimento_ano'),
            ("primeiro_nome", "primeiro_nome", "string", 'primeiro_nome'),
            ("primeiro_nome_mae", "primeiro_nome_mae", "string", 'primeiro_nome_mae'),
            ("complemento_nome", "complemento_nome", "string", 'complemento_nome'),
            ("complemento_nome_mae", "complemento_nome_mae", "string", 'complemento_nome_mae'),
        ]
        self.deduple.set_linkage_variables(linkage_vars, string_method="damerau_levenshtein").define_pairs("FONETICA_N", window=3)

        # ---- remove pairs that were already compared previously
        self.deduple.candidate_pairs = self.deduple.candidate_pairs.drop(self.current_pairs, errors='ignore')
        print(f"Pairs to be effectively compared: {self.deduple.candidate_pairs.shape[0]}")
        # -- compare and generate the similarity matrix
        self.deduple.perform_linkage(threshold=0.60, number_of_blocks=number_of_blocks)

        # -- classify pairs
        pair_ids, X_sel = self.deduple.comparison_matrix.reset_index().iloc[:,:2],  self.deduple.comparison_matrix.reset_index().iloc[:,2:].values

        batchsize = 6000
        Y_neg1, Y_neg2, Y_neg3 = [], [], []
        for batch in tqdm(np.array_split(X_sel, np.arange(batchsize, X_sel.shape[0]+1, batchsize))):
            Y_neg1 += [ res[0] for res in self.models["GBT"].predict_proba(batch) ]
            Y_neg2 += [ res[0] for res in self.models["RNF"].predict_proba(batch) ]
            Y_neg3 += [ res[0] for res in self.models["LGT"].predict_proba(batch) ]

        pair_ids["FMT_ID"] = pair_ids[f"{self.field_id}_1"] + "-" + pair_ids[f"{self.field_id}_2"]
        pair_ids["PROBA_NEGATIVO_MODELO_1"] = Y_neg1
        pair_ids["PROBA_NEGATIVO_MODELO_2"] = Y_neg2
        pair_ids["PROBA_NEGATIVO_MODELO_3"] = Y_neg3

        self.warehouse.insert('pairs_label', pair_ids, batchsize=500, verbose=True)