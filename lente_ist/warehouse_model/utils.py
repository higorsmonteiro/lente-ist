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
    df["ID"] = df["codigo_paciente"].apply(lambda x: unidecode(x.upper()).strip())
    df['NM_PACIENT'] = df['nome_paciente'].apply(lambda x: unidecode(x.upper()).strip() if pd.notna(x) else np.nan)
    df['DT_NASC'] = pd.to_datetime(df['data_nascimento'], errors="coerce")
    df["NM_MAE_PAC"] = df["nome_mae"].apply(lambda x: unidecode(x.upper()).strip() if pd.notna(x) else np.nan)
    df['DT_NOTIFIC'] = pd.to_datetime(df['data_cadastro'], errors="coerce") # auxiliary only
    df["DT_DIAG"] = [ np.nan for n in range(df.shape[0]) ]
    df["CS_SEXO"] = df["sexo"].apply(lambda x: unidecode(x.upper()).strip() if pd.notna(x) else np.nan).map({'1': 'M', '2': 'F', '3': np.nan, '4': np.nan})
    df["NM_BAIRRO"] = df["bairro"].apply(lambda x: unidecode(x.upper()).strip() if pd.notna(x) else np.nan)
    df["ID_MN_RESI"] = df["codigo_ibge_resid"].apply(lambda x: unidecode(x)[:6] if pd.notna(x) else np.nan)
    df["NM_LOGRADO"] = df["endereco"].apply(lambda x: unidecode(x.upper()).strip() if pd.notna(x) else np.nan)
    df["NU_CEP"] = df["Cep"].apply(lambda x: unidecode(x.upper()).strip() if pd.notna(x) else np.nan)
    df["NU_NUMERO"] = [ np.nan for n in range(df.shape[0]) ]
    df["ID_CNS_SUS"] = [ np.nan for n in range(df.shape[0]) ]
    df["FONTE"] = [ source_name for n in range(df.shape[0]) ]
    df["CPF"] = df["cpf"].apply(lambda x: unidecode(x.upper()).strip() if pd.notna(x) else np.nan)

    df["UDM"] = df["UDM"].apply(lambda x: unidecode(x.upper()).strip() if pd.notna(x) else np.nan)
    df["CODIGO_IBGE_NASC"] = df["codigo_ibge_nasc"].apply(lambda x: unidecode(x) if pd.notna(x) else np.nan)
    df["RACA"] = df["raca"].apply(lambda x: unidecode(x.upper()).strip() if pd.notna(x) else np.nan)
    df["ESTADO_CIVIL"] = df["estado_civil"].apply(lambda x: unidecode(x.upper()).strip() if pd.notna(x) else np.nan)
    df["ESCOLARIDADE"] = df["escolaridade"].apply(lambda x: unidecode(x.upper()).strip() if pd.notna(x) else np.nan)
    df["CD4_INICIO_TARV"] = df["cd4_inicio_TARV"].apply(lambda x: unidecode(x.upper()).strip() if pd.notna(x) else np.nan)
    df["CV_INICIO_TARV"] = df["cv_inicio_TARV"].apply(lambda x: unidecode(x.upper()).strip() if pd.notna(x) else np.nan)
    df["ANO_INICIO_TARV"] = df["ano_inicio_TARV"].apply(lambda x: unidecode(x.upper()).strip() if pd.notna(x) else np.nan)
    df["DT_CADASTRO"] = pd.to_datetime(df['data_cadastro'], errors="coerce")
    df["DT_DIGITACAO"] = pd.to_datetime(df['data_digitacao'], errors="coerce")
    df["DT_ULT_ATU"] = pd.to_datetime(df['data_ult_atu'], errors="coerce")
    df["ST_PACIENTE"] = df['st_paciente'].apply(lambda x: unidecode(x.upper()).strip() if pd.notna(x) else np.nan)
    return df

def process_sim(df, source_name="SIM"):
    '''
        ...
    '''
    df["ID"] = df["NUMERODO"].apply(lambda x: f'DO{x}')
    df["NM_PACIENT"] = df["NOME"].copy()
    df["NM_MAE_PAC"] = df["NOMEMAE"].copy()
    df["ID_MN_RESI"] = df["CODMUNRES"].copy()
    df['DT_NOTIFIC'] = [ np.nan for n in range(df.shape[0]) ]
    df["DT_DIAG"] = [ np.nan for n in range(df.shape[0]) ]
    df["NM_BAIRRO"] = df["BAIRES"].copy()
    df["DT_NASC"] = pd.to_datetime(df["DTNASC"], format="%d%m%Y", errors='coerce')
    df["NM_LOGRADO"] = df["ENDRES"].copy()
    df["NU_NUMERO"] = df["NUMRES"].copy()
    df["NU_CEP"] = df["CEPRES"].copy()
    df["ID_CNS_SUS"] = df["NUMSUS"].copy()
    df["CPF"] = [ np.nan for elem in range(df.shape[0]) ]
    df["CS_SEXO"] = df["SEXO"].copy()
    df["FONTE"] = [ source_name for elem in range(df.shape[0]) ]

    df["DTOBITO"] = pd.to_datetime(df["DTOBITO"], format="%d%m%Y", errors='coerce')
    df["CAUSABAS"] = df["CAUSABAS"].copy()
    df["LINHAA"] = df["LINHAA"].copy()
    df["LINHAB"] = df["LINHAB"].copy()
    df["LINHAC"] = df["LINHAC"].copy()
    df["LINHAD"] = df["LINHAD"].copy() 
    df["LINHAII"] = df["LINHAII"].copy()
    return df

# -- the raw data needs pd.read_excel(filename, header=6)
# -- better to provide a minimally processed data where 'header=6' is not needed.
def process_simc(df, source_name="SIMC"):
    '''
        ...
    '''
    # -- create an index hash based on the values of the row
    df = df.dropna(how='all')
    cols = [
       'Instituição solicitante', 'Nome Paciente', 'Nome mãe', 'Data Nascimento', 
       'Idade', 'Sexo/Gênero', 'Raça/cor', 'Escolaridade', 'CPF'
    ]
    row_str = df[cols].apply(lambda x: abs(hash('_'.join([ f'{x[col]}' for col in cols if pd.notna(x[col]) ]))) % (10**12) , axis=1)
    df["ID"] = row_str.apply(lambda x: f'{x:.0f}')
    df['NM_PACIENT'] = df['Nome Paciente'].apply(lambda x: unidecode(x.upper()).strip() if pd.notna(x) else np.nan)
    df['DT_NASC'] = pd.to_datetime(df['Data Nascimento'], errors="coerce")
    df["NM_MAE_PAC"] = df['Nome mãe'].apply(lambda x: unidecode(x.upper()).strip() if pd.notna(x) else np.nan)
    df['DT_NOTIFIC'] = [ np.nan for elem in range(df.shape[0]) ]
    df['DT_DIAG'] = [ np.nan for elem in range(df.shape[0]) ]
    df["CS_SEXO"] = df['Sexo/Gênero'].apply(lambda x: unidecode(x.upper()).strip() if pd.notna(x) else np.nan).map({'Masculino': 'M', 'Feminino': 'F'})
    df["NM_BAIRRO"] = [ np.nan for elem in range(df.shape[0]) ]
    df["ID_MN_RESI"] = df['Município de residência'].copy() # -- needs transformation
    df["NM_LOGRADO"] = [ np.nan for elem in range(df.shape[0]) ]
    df["NU_CEP"] = [ np.nan for elem in range(df.shape[0]) ]
    df["NU_NUMERO"] = [ np.nan for elem in range(df.shape[0]) ]
    df["ID_CNS_SUS"] = [ np.nan for elem in range(df.shape[0]) ]
    df["FONTE"] = [ source_name for n in range(df.shape[0]) ]
    df["CPF"] = df["CPF"].apply(lambda x: unidecode(f'{x:11.0f}'.replace(" ", "0").upper()).strip() if pd.notna(x) else np.nan)

    df['DT_ULTIMA_DESPENSA'] = pd.to_datetime(df['Data últ. dispensa'], errors='coerce')
    df['DURACAO'] = df['Duração'].copy()
    df["DT_RETORNO"] = pd.to_datetime(df["Data retorno"], errors='coerce')
    df["DIAS_ATRASO"] = df['Dias atraso'].copy()
    df['INST_SOLICITANTE'] = df['Instituição solicitante'].copy()
    return df

# -- the raw data needs pd.read_excel(filename, header=6)
# -- better to provide a minimally processed data where 'header=6' is not needed.
def process_siscel(df, source_name="SISCEL"):
    '''
        ...
    '''
    # -- create an index hash based on the values of the row
    df = df.dropna(how='all')
    cols = [
       'Instituição solicitante', 'Nome Paciente', 'Nome mãe', 'Data Nascimento', 
       'Idade', 'Sexo/Gênero', 'Raça/cor', 'Escolaridade', 'CPF'
    ]
    row_str = df[cols].apply(lambda x: abs(hash('_'.join([ f'{x[col]}' for col in cols if pd.notna(x[col]) ]))) % (10**12) , axis=1)
    df["ID"] = row_str.apply(lambda x: f'{x:.0f}')
    df['NM_PACIENT'] = df['Nome Paciente'].apply(lambda x: unidecode(x.upper()).strip() if pd.notna(x) else np.nan)
    df['DT_NASC'] = pd.to_datetime(df['Data Nascimento'], errors="coerce")
    df["NM_MAE_PAC"] = df['Nome mãe'].apply(lambda x: unidecode(x.upper()).strip() if pd.notna(x) else np.nan)
    df['DT_NOTIFIC'] = [ np.nan for elem in range(df.shape[0]) ]
    df['DT_DIAG'] = [ np.nan for elem in range(df.shape[0]) ]
    df["CS_SEXO"] = df['Sexo/Gênero'].apply(lambda x: unidecode(x.upper()).strip() if pd.notna(x) else np.nan).map({'Masculino': 'M', 'Feminino': 'F'})
    df["NM_BAIRRO"] = [ np.nan for elem in range(df.shape[0]) ]
    df["ID_MN_RESI"] = df['Município de residência'].copy() # -- needs transformation
    df["NM_LOGRADO"] = [ np.nan for elem in range(df.shape[0]) ]
    df["NU_CEP"] = [ np.nan for elem in range(df.shape[0]) ]
    df["NU_NUMERO"] = [ np.nan for elem in range(df.shape[0]) ]
    df["ID_CNS_SUS"] = [ np.nan for elem in range(df.shape[0]) ]
    df["FONTE"] = [ source_name for n in range(df.shape[0]) ]
    df["CPF"] = df["CPF"].apply(lambda x: unidecode(f'{x:11.0f}'.replace(" ", "0").upper()).strip() if pd.notna(x) else np.nan)

    df['DT_ULTIMA_DESPENSA'] = pd.to_datetime(df['Data últ. dispensa'], errors='coerce')
    df['DURACAO'] = df['Duração'].copy()
    df["DT_RETORNO"] = pd.to_datetime(df["Data retorno"], errors='coerce')
    df["DIAS_ATRASO"] = df['Dias atraso'].copy()
    df['INST_SOLICITANTE'] = df['Instituição solicitante'].copy()
    return df

def process_siscel(df, source_name="SISCEL"):
    '''
        ...
    '''
    # -- create an index hash based on the values of the row (needs to choose the 'cols')
    df = df.dropna(how='all')
    cols = [
       'Instituição solicitante', 'Nome Paciente', 'Nome mãe', 'Data Nascimento', 
       'Idade', 'Sexo/Gênero', 'Raça/cor', 'Escolaridade', 'CPF'
    ]
    df["NM_PACIENT"] = df["Nome Civil"].apply(lambda x: unidecode(x.upper()).strip() if pd.notna(x) else np.nan)
    df['DT_NASC'] = pd.to_datetime(df['Data de Nascimento'], errors="coerce")
    df["NM_MAE_PAC"] = df['Mãe'].apply(lambda x: unidecode(x.upper()).strip() if pd.notna(x) else np.nan)
    df['DT_NOTIFIC'] = [ np.nan for elem in range(df.shape[0]) ]
    df['DT_DIAG'] = [ np.nan for elem in range(df.shape[0]) ]
    df["CS_SEXO"] = df['Sexo'].apply(lambda x: unidecode(x.upper()).strip() if pd.notna(x) else np.nan).map({'Masculino': 'M', 'Feminino': 'F', 
                                                                                                                       'Intersexo': 'I', 'Não Informado': 'I',
                                                                                                                       '-': 'I'})
    df["NM_BAIRRO"] = [ np.nan for elem in range(df.shape[0]) ]
    df["ID_MN_RESI"] = df['Cidade de Residência'].copy() # -- needs transformation
    df["NM_LOGRADO"] = df["Endereço"].apply(lambda x: unidecode(x.upper()).strip() if pd.notna(x) else np.nan)
    df["NU_CEP"] = df["CEP"].apply(lambda x: unidecode(x.upper()).strip() if pd.notna(x) else np.nan)
    df["NU_NUMERO"] = [ np.nan for elem in range(df.shape[0]) ]
    df["ID_CNS_SUS"] = [ np.nan for elem in range(df.shape[0]) ]
    df["CPF"] = [ np.nan for elem in range(df.shape[0]) ]
    df["FONTE"] = [ source_name for n in range(df.shape[0]) ]



    
    
    
    
    
