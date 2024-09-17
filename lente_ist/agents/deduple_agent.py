'''
    ...
'''
import joblib
from pathlib import Path
import pandas as pd
import numpy as np
import datetime as dt
from tqdm import tqdm
from lente_ist import WarehouseHIV
from lente_ist.utils import perform_query
from lente_ist.process_layer.sus_specific import ProcessSinan
from lente_ist.data_matching.matching_data import Deduple, PLinkage

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

# -- example
class LinkageRecordsHIV:
    def __init__(self, warehouse_location, warehouse_name, path_to_models, field_id="ID"):
        self.warehouse_location = warehouse_location
        self.warehouse_name = warehouse_name
        self.field_id = field_id
        self.engine_url = f"sqlite:///{self.warehouse_location.joinpath(self.warehouse_name)}"
        self.warehouse = WarehouseHIV(self.engine_url)
        self.engine = self.warehouse.db_init()
        self.current_pairs = []
        self.linkage = None

        self.models = dict()
        self.models["GBT"] = joblib.load(Path(path_to_models).joinpath("GRADBOOST_SIVEP04SET2023.joblib"))
        self.models["RNF"] = joblib.load(Path(path_to_models).joinpath("RANDFOREST_SIVEP04SET2023.joblib"))
        self.models["LGT"] = joblib.load(Path(path_to_models).joinpath("LOGITREG_SIVEP04SET2023.joblib"))

    def perform_without_sim(self, number_of_blocks, frac=1.0, chunksize=5000):
        ''' 
            Perform Deduplication/Linkage within the connected database.

            This function performs a deduplication within the table 'pessoa' for all records that do not
            belong to SIM. That is, records from SINAN, SICLOM and SIMC are included. The deduplication
            here is equivalent to the record linkage process. 
        '''
        query_data = pd.DataFrame( self.warehouse.query_all(table_name='pessoa'))
        # -- remove records from SIM (it will not be compared here)
        left_df = query_data[~query_data["FONTE"].str.contains("DO")]
        processor_left = ProcessSinan(left_df, self.field_id)
        processor_left.basic_standardize().specific_standardize()
        processed_left = processor_left.data

        # -- build the similarity matrix
        #self.linkage = PLinkage(processed_left, processed_right, left_id=f"{self.field_id}_1", right_id=f"{self.field_id}_2", env_folder=None)
        self.linkage = Deduple(processed_left, left_id=f"{self.field_id}", env_folder=None)
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
        self.linkage.set_linkage_variables(linkage_vars, string_method="damerau_levenshtein").define_pairs("FONETICA_N", window=3)

        # -- remove the pairs already compared
        ids_to_remove = []
        for j in tqdm(range(0, len(self.linkage.candidate_pairs), chunksize)):
            current_chunk = list(self.linkage.candidate_pairs[j:j + chunksize])
            chunk_tuple = tuple([ '"'+elem[0]+'-'+elem[1]+'"' for elem in current_chunk ])

            q = f'''
                SELECT * FROM likely_negative_pairs WHERE FMT_ID IN ({','.join(chunk_tuple)})
            '''
            found_ids = perform_query(q, self.engine)
            # ["FMT_ID"].tolist()
            if found_ids.shape[0]>0:
                found_ids = [ (elem.split("-")[0], elem.split("-")[1]) for elem in found_ids["FMT_ID"].tolist() ]                
                ids_to_remove += found_ids
        print(f"Pairs already compared before: {len(ids_to_remove)}")

        # ---- remove pairs that were already compared previously
        self.linkage.candidate_pairs = self.linkage.candidate_pairs.drop(ids_to_remove, errors='ignore')
        # -- free the space
        del ids_to_remove
        print(f"Pairs to be effectively compared: {self.linkage.candidate_pairs.shape[0]}")
        # -- compare and generate the similarity matrix
        self.linkage.perform_linkage(threshold=0.60, number_of_blocks=number_of_blocks)

        # -- classify pairs
        pair_ids, X_sel = self.linkage.comparison_matrix.reset_index().iloc[:,:2],  self.linkage.comparison_matrix.reset_index().iloc[:,2:].values

        batchsize = 6000
        Y_neg1, Y_neg2, Y_neg3 = [], [], []
        for batch in tqdm(np.array_split(X_sel, np.arange(batchsize, X_sel.shape[0]+1, batchsize))):
            Y_neg1 += [ res[0] for res in self.models["GBT"].predict_proba(batch) ]
            Y_neg2 += [ res[0] for res in self.models["RNF"].predict_proba(batch) ]
            Y_neg3 += [ res[0] for res in self.models["LGT"].predict_proba(batch) ]

        # -- create the subset of very likely positive pairs Yp and very likely negative pairs Yn
        border_thr = 0.75
        Yp = [ ( pair_ids[f"{self.field_id}_1"].iloc[index]+"-"+pair_ids[f"{self.field_id}_2"].iloc[index], yl[0], yl[1], yl[2]) for index, yl in enumerate(zip(Y_neg1, Y_neg2, Y_neg3)) if yl[0] <= border_thr and yl[1] <= border_thr and yl[2] <= border_thr]
        Yn = [ ( pair_ids[f"{self.field_id}_1"].iloc[index]+"-"+pair_ids[f"{self.field_id}_2"].iloc[index], yl[0], yl[1], yl[2]) for index, yl in enumerate(zip(Y_neg1, Y_neg2, Y_neg3)) if not (yl[0] <= border_thr and yl[1] <= border_thr and yl[2] <= border_thr)]

        # -- insert the likely positive pairs
        likely_positive = pd.DataFrame({
            "FMT_ID": [ y_elem[0] for y_elem in Yp ],
            "PROBA_NEGATIVO_MODELO_1": [ y_elem[1] for y_elem in Yp ],
            "PROBA_NEGATIVO_MODELO_2": [ y_elem[2] for y_elem in Yp ],
            "PROBA_NEGATIVO_MODELO_3": [ y_elem[3] for y_elem in Yp ], 
        })
        self.warehouse.insert('likely_positive_pairs', likely_positive, batchsize=500, verbose=True)

        # -- insert the likely negative pairs
        likely_negative = pd.DataFrame({
            "FMT_ID": [ y_elem[0] for y_elem in Yn ],
            "PROBA_NEGATIVO_MODELO_1": [ y_elem[1] for y_elem in Yn ],
            "PROBA_NEGATIVO_MODELO_2": [ y_elem[2] for y_elem in Yn ],
            "PROBA_NEGATIVO_MODELO_3": [ y_elem[3] for y_elem in Yn ], 
        })
        self.warehouse.insert('likely_negative_pairs', likely_negative, batchsize=500, verbose=True)

    def perform_hiv_to_sim(self, number_of_blocks, frac=1.0, chunksize=5000):
        ''' 
            Perform Deduplication/Linkage within the connected database.
        '''
        query_data = pd.DataFrame( self.warehouse.query_all(table_name='pessoa'))
        # -- remove records from SIM (it will not be compared here)
        left_df = query_data[~query_data["FONTE"].str.contains("DO")]
        right_df = query_data[query_data["FONTE"].str.contains("DO")]
        #left_df = query_data[query_data[self.field_id].isin(target_ids)]
        processor_left = ProcessSinan(left_df, self.field_id)
        processor_left.basic_standardize().specific_standardize()
        processed_left = processor_left.data
        processor_right = ProcessSinan(right_df, self.field_id)
        processor_right.basic_standardize().specific_standardize()
        processed_right = processor_right.data
        processed_left = processed_left.rename({self.field_id: f"{self.field_id}_1"}, axis=1)
        processed_right = processed_right.rename({self.field_id: f"{self.field_id}_2"}, axis=1)
        query_data = None

        # -- build the similarity matrix
        self.linkage = PLinkage(processed_left, processed_right, left_id=f"{self.field_id}_1", right_id=f"{self.field_id}_2", env_folder=None)
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
        self.linkage.set_linkage_variables(linkage_vars, string_method="damerau_levenshtein").define_pairs("FONETICA_N", "FONETICA_N", window=3)

        # -- remove the pairs already compared
        ids_to_remove = []
        for j in tqdm(range(0, len(self.linkage.candidate_pairs), chunksize)):
            current_chunk = list(self.linkage.candidate_pairs[j:j + chunksize])
            chunk_tuple = tuple([ '"'+elem[0]+'-'+elem[1]+'"' for elem in current_chunk ])

            q = f'''
                SELECT * FROM likely_negative_pairs WHERE FMT_ID IN ({','.join(chunk_tuple)})
            '''
            found_ids = perform_query(q, self.engine)
            # ["FMT_ID"].tolist()
            if found_ids.shape[0]>0:
                found_ids = [ (elem.split("-")[0], elem.split("-")[1]) for elem in found_ids["FMT_ID"].tolist() ]                
                ids_to_remove += found_ids
        print(f"Pairs already compared before: {len(ids_to_remove)}")

        # ---- remove pairs that were already compared previously
        self.linkage.candidate_pairs = self.linkage.candidate_pairs.drop(ids_to_remove, errors='ignore')
        # -- free the space
        del ids_to_remove
        print(f"Pairs to be effectively compared: {self.linkage.candidate_pairs.shape[0]}")
        # -- compare and generate the similarity matrix
        self.linkage.perform_linkage(threshold=0.60, number_of_blocks=number_of_blocks)

        # -- classify pairs
        pair_ids, X_sel = self.linkage.comparison_matrix.reset_index().iloc[:,:2],  self.linkage.comparison_matrix.reset_index().iloc[:,2:].values

        batchsize = 6000
        Y_neg1, Y_neg2, Y_neg3 = [], [], []
        for batch in tqdm(np.array_split(X_sel, np.arange(batchsize, X_sel.shape[0]+1, batchsize))):
            Y_neg1 += [ res[0] for res in self.models["GBT"].predict_proba(batch) ]
            Y_neg2 += [ res[0] for res in self.models["RNF"].predict_proba(batch) ]
            Y_neg3 += [ res[0] for res in self.models["LGT"].predict_proba(batch) ]

        # -- create the subset of very likely positive pairs Yp and very likely negative pairs Yn
        border_thr = 0.75
        Yp = [ ( pair_ids[f"{self.field_id}_1"].iloc[index]+"-"+pair_ids[f"{self.field_id}_2"].iloc[index], yl[0], yl[1], yl[2]) for index, yl in enumerate(zip(Y_neg1, Y_neg2, Y_neg3)) if yl[0] <= border_thr and yl[1] <= border_thr and yl[2] <= border_thr]
        Yn = [ ( pair_ids[f"{self.field_id}_1"].iloc[index]+"-"+pair_ids[f"{self.field_id}_2"].iloc[index], yl[0], yl[1], yl[2]) for index, yl in enumerate(zip(Y_neg1, Y_neg2, Y_neg3)) if not (yl[0] <= border_thr and yl[1] <= border_thr and yl[2] <= border_thr)]

        # -- insert the likely positive pairs
        likely_positive = pd.DataFrame({
            "FMT_ID": [ y_elem[0] for y_elem in Yp ],
            "PROBA_NEGATIVO_MODELO_1": [ y_elem[1] for y_elem in Yp ],
            "PROBA_NEGATIVO_MODELO_2": [ y_elem[2] for y_elem in Yp ],
            "PROBA_NEGATIVO_MODELO_3": [ y_elem[3] for y_elem in Yp ], 
        })
        self.warehouse.insert('likely_positive_pairs', likely_positive, batchsize=500, verbose=True)

        # -- insert the likely negative pairs
        likely_negative = pd.DataFrame({
            "FMT_ID": [ y_elem[0] for y_elem in Yn ],
            "PROBA_NEGATIVO_MODELO_1": [ y_elem[1] for y_elem in Yn ],
            "PROBA_NEGATIVO_MODELO_2": [ y_elem[2] for y_elem in Yn ],
            "PROBA_NEGATIVO_MODELO_3": [ y_elem[3] for y_elem in Yn ], 
        })
        self.warehouse.insert('likely_negative_pairs', likely_negative, batchsize=500, verbose=True)