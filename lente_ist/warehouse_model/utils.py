# -*- coding: utf-8 -*- 
'''
    Processamentos específicos.

    Autor: Higor S. Monteiro
    Email: higormonteiros@gmail.com
'''
from pathlib import Path
import pandas as pd
import numpy as np
from unidecode import unidecode

def process_sinan(df, source_name="SICLOM"):
    '''
        ...
    '''
    # -- ID_SINAN: ID_AGRAVO+NU_NOTIFIC+ID_MUNICIP+DT_NOTIFIC
    notific_fmt = df["DT_NOTIFIC"].apply(lambda x: f"{x.day:2.0f}{x.month:2.0f}{x.year}".replace(" ", "0"))
    id_municip_fmt = df["ID_MUNICIP"].apply(lambda x: f"{x}")
    df["ID"] = df["ID_AGRAVO"]+df["NU_NOTIFIC"]+id_municip_fmt+notific_fmt
    df["CPF"] = [ np.nan for n in range(df.shape[0]) ]
    df["FONTE"] = [ source_name for n in range(df.shape[0]) ]
    return df

def process_siclom(df, source_name="SICLOM"):
    '''
        ...
    '''
    df["ID"] = df["codigo_paciente"].apply(lambda x: unidecode(x.upper()))
    df['NM_PACIENT'] = df['nome_paciente'].apply(lambda x: unidecode(x.upper()) if pd.notna(x) else np.nan)
    df['DT_NASC'] = pd.to_datetime(df['data_nascimento'], errors="coerce")
    df["NM_MAE_PAC"] = df["nome_mae"].apply(lambda x: unidecode(x.upper()) if pd.notna(x) else np.nan)
    df['DT_NOTIFIC'] = pd.to_datetime(df['data_cadastro'], errors="coerce") # auxiliary only
    df["DT_DIAG"] = [ np.nan for n in range(df.shape[0]) ]
    df["CS_SEXO"] = df["sexo"].apply(lambda x: unidecode(x.upper()) if pd.notna(x) else np.nan).map({'1': 'M', '2': 'F', '3': np.nan, '4': np.nan})
    df["NM_BAIRRO"] = df["bairro"].apply(lambda x: unidecode(x.upper()) if pd.notna(x) else np.nan)
    df["ID_MN_RESI"] = df["codigo_ibge_resid"].apply(lambda x: unidecode(x)[:6] if pd.notna(x) else np.nan)
    df["NM_LOGRADO"] = df["endereco"].apply(lambda x: unidecode(x.upper()) if pd.notna(x) else np.nan)
    df["NU_CEP"] = df["Cep"].apply(lambda x: unidecode(x.upper()) if pd.notna(x) else np.nan)
    df["NU_NUMERO"] = [ np.nan for n in range(df.shape[0]) ]
    df["ID_CNS_SUS"] = [ np.nan for n in range(df.shape[0]) ]
    df["FONTE"] = [ source_name for n in range(df.shape[0]) ]
    df["CPF"] = df["cpf"].apply(lambda x: unidecode(x.upper()) if pd.notna(x) else np.nan)

    df["UDM"] = df["UDM"].apply(lambda x: unidecode(x.upper()) if pd.notna(x) else np.nan)
    df["CODIGO_IBGE_NASC"] = df["codigo_ibge_nasc"].apply(lambda x: unidecode(x) if pd.notna(x) else np.nan)
    df["RACA"] = df["raca"].apply(lambda x: unidecode(x.upper()) if pd.notna(x) else np.nan)
    df["ESTADO_CIVIL"] = df["estado_civil"].apply(lambda x: unidecode(x.upper()) if pd.notna(x) else np.nan)
    df["ESCOLARIDADE"] = df["escolaridade"].apply(lambda x: unidecode(x.upper()) if pd.notna(x) else np.nan)
    df["CD4_INICIO_TARV"] = df["cd4_inicio_TARV"].apply(lambda x: unidecode(x.upper()) if pd.notna(x) else np.nan)
    df["CV_INICIO_TARV"] = df["cv_inicio_TARV"].apply(lambda x: unidecode(x.upper()) if pd.notna(x) else np.nan)
    df["ANO_INICIO_TARV"] = df["ano_inicio_TARV"].apply(lambda x: unidecode(x.upper()) if pd.notna(x) else np.nan)
    df["DT_CADASTRO"] = pd.to_datetime(df['data_cadastro'], errors="coerce")
    df["DT_DIGITACAO"] = pd.to_datetime(df['data_digitacao'], errors="coerce")
    df["DT_ULT_ATU"] = pd.to_datetime(df['data_ult_atu'], errors="coerce")
    df["ST_PACIENTE"] = df['st_paciente'].apply(lambda x: unidecode(x.upper()) if pd.notna(x) else np.nan)
    return df
