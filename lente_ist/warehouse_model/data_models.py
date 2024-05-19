'''
    Define the data models to store the main information on individuals 
    and linkage between different records.

    Author: Higor S. Monteiro
    Email: higor.monteiro@fisica.ufc.br
'''

import datetime as dt
from sqlalchemy import Column, Table, MetaData
from sqlalchemy import DateTime, Integer, Numeric, String, Float, Sequence, ForeignKey, CheckConstraint
from sqlalchemy.exc import InternalError, IntegrityError

# ---------- datasus models ----------
class Pessoa:
    def __init__(self, metadata):
        self.metadata = metadata
        self.table_name = 'pessoa'

        # -- define schema for table.
        self.model = Table(
            self.table_name, self.metadata,
            Column("ID", String, primary_key=True),
            Column("DATA_NOTIFICACAO", DateTime, nullable=True),
            Column("DATA_DIAGNOSTICO", DateTime, nullable=True),
            Column("NOME_PACIENTE", String, nullable=True),
            Column("DATA_NASCIMENTO", DateTime, nullable=True),
            Column("SEXO", String, nullable=True),
            Column("NOME_MAE", String, nullable=True),
            Column("LOGRADOURO", String, nullable=True),
            Column("LOGRADOURO_NUMERO", String, nullable=True),
            Column("BAIRRO_RESIDENCIA", String, nullable=True),
            Column("MUNICIPIO_RESIDENCIA", String, nullable=True),
            Column("CEP", String, nullable=True),
            Column("CNS", String, nullable=True),
            Column("CPF", String, nullable=True),
            Column("FONTE", String, nullable=False),
            Column("CRIADO_EM", DateTime, default=dt.datetime.now),
            Column("ATUALIZADO_EM", DateTime, default=dt.datetime.now, onupdate=dt.datetime.now),
        )

        # -- define data mapping (could be import if too big)
        # -- 'ID' is four columns: NU_NOTIFIC+ID_AGRAVO+DT_NOTIFIC+ID_MUNICIPIO
        self.mapping = {
            "ID" : "ID", 
            "DT_NOTIFIC": "DATA_NOTIFICACAO",
            "DT_DIAG": "DATA_DIAGNOSTICO",
            "NM_PACIENT": "NOME_PACIENTE", 
            "CS_SEXO": "SEXO",
            "DT_NASC": "DATA_NASCIMENTO",
            "NM_MAE_PAC": "NOME_MAE",
            "ID_MN_RESI": "MUNICIPIO_RESIDENCIA",
            "NM_BAIRRO": "BAIRRO_RESIDENCIA", 
            "NM_LOGRADO": "LOGRADOURO",
            "NU_NUMERO": "LOGRADOURO_NUMERO", 
            "NU_CEP": "CEP", 
            "ID_CNS_SUS": "CNS",
            "CPF": "CPF",
            "FONTE": "FONTE",
        }

    def define(self):
        '''
            Return dictionary elements containing the data model and 
            the data mapping, respectively.
        '''
        table_elem = { self.table_name : self.model }
        mapping_elem = { self.table_name : self.mapping }
        return table_elem, mapping_elem


class SinanAidsAdultoInfo:
    def __init__(self, metadata):
        self.metadata = metadata
        self.table_name = 'sinan_aids_adulto_info'
        self._dummy_ = ['ID','ID_OCUPA_N', 'ANT_TRASMI', 'ANTRELSE_N', 'ANT_DROGA',
                        'ANT_HEMOLF', 'ANTTRANS_M', 'ANT_ACIDEN', 'ANTDTTRANS', 'ANTUFTRANS',
                        'ANTMUNTRAN', 'ANT_INSTTR', 'ANT_INVEST', 'LAB_TRIAGE', 'DTTRIAGEM1',
                        'LAB_CONFIR', 'DT_CONFIRM', 'TPRAPIDO1', 'TPRAPIDO2', 'TPRAPIDO3',
                        'DT_RAPIDO', 'ANT_SARCOM', 'ANT_TUBERC', 'ANT_CANDID', 'ANT_PULMON',
                        'ANT_HERPES', 'ANT_DISFUN', 'ANT_DIARRE', 'ANT_FEBRE', 'ANT_CAQUEX',
                        'ANT_ASTERI', 'ANT_DERMAT', 'ANT_ANEMIA', 'ANT_TOSSE', 'ANT_LINFO',
                        'ANT_CANCER', 'ANT_ESOF_N', 'ANT_PULM_N', 'ANT_CITO', 'ANT_CRIPTO',
                        'ANT_CRIP_1', 'ANT_H_SIMP', 'ANT_HISTO', 'ANT_ISOPOR', 'ANT_LEUCO',
                        'ANT_LINFOM', 'ANT_LINFO_', 'ANT_MICRO', 'ANT_PNEUMO', 'ANT_CHAGAS',
                        'ANT_SALMO', 'ANT_TOXO', 'ANT_CONTAG', 'DEF_DIAGNO', 'TRA_UF',
                        'TRA_MUNICI', 'TRA_UNIDAD', 'EVOLUCAO', 'DT_OBITO', 'CRITERIO', 'ANT_REL_CA']

        # -- define schema for table.
        self.model = Table(
            self.table_name, self.metadata,
            Column("ID", String, ForeignKey('pessoa.ID')),
            Column('ID_OCUPA_N', String, nullable=True),
            Column('ANT_TRASMI', String, nullable=True),
            Column('ANTRELSE_N', String, nullable=True),
            Column('ANT_DROGA', String, nullable=True),
            Column('ANT_HEMOLF', String, nullable=True),
            Column('ANTTRANS_M', String, nullable=True),
            Column('ANT_ACIDEN', String, nullable=True),
            Column('ANTDTTRANS', DateTime, nullable=True),
            Column('ANTUFTRANS', String, nullable=True),
            Column('ANTMUNTRAN', String, nullable=True),
            Column('ANT_INSTTR', String, nullable=True),
            Column('ANT_INVEST', String, nullable=True),
            Column('LAB_TRIAGE', String, nullable=True),
            Column('DTTRIAGEM1', DateTime, nullable=True),
            Column('LAB_CONFIR', String, nullable=True),
            Column('DT_CONFIRM', DateTime, nullable=True),
            Column('TPRAPIDO1', String, nullable=True),
            Column('TPRAPIDO2', String, nullable=True),
            Column('TPRAPIDO3', String, nullable=True),
            Column('DT_RAPIDO', DateTime, nullable=True),
            Column('ANT_SARCOM', String, nullable=True),
            Column('ANT_TUBERC', String, nullable=True),
            Column('ANT_CANDID', String, nullable=True),
            Column('ANT_PULMON', String, nullable=True),
            Column('ANT_HERPES', String, nullable=True),
            Column('ANT_DISFUN', String, nullable=True),
            Column('ANT_DIARRE', String, nullable=True),
            Column('ANT_FEBRE', String, nullable=True),
            Column('ANT_CAQUEX', String, nullable=True),
            Column('ANT_ASTERI', String, nullable=True),
            Column('ANT_DERMAT', String, nullable=True),
            Column('ANT_ANEMIA', String, nullable=True),
            Column('ANT_TOSSE', String, nullable=True),
            Column('ANT_LINFO', String, nullable=True),
            Column('ANT_CANCER', String, nullable=True),
            Column('ANT_ESOF_N', String, nullable=True),
            Column('ANT_PULM_N', String, nullable=True),
            Column('ANT_CITO', String, nullable=True),
            Column('ANT_CRIPTO', String, nullable=True),
            Column('ANT_CRIP_1', String, nullable=True),
            Column('ANT_H_SIMP', String, nullable=True),
            Column('ANT_HISTO', String, nullable=True),
            Column('ANT_ISOPOR', String, nullable=True),
            Column('ANT_LEUCO', String, nullable=True),
            Column('ANT_LINFOM', String, nullable=True),
            Column('ANT_LINFO_', String, nullable=True),
            Column('ANT_MICRO', String, nullable=True),
            Column('ANT_PNEUMO', String, nullable=True),
            Column('ANT_CHAGAS', String, nullable=True),
            Column('ANT_SALMO', String, nullable=True),
            Column('ANT_TOXO', String, nullable=True),
            Column('ANT_CONTAG', String, nullable=True),
            Column('DEF_DIAGNO', String, nullable=True),
            Column('TRA_UF', String, nullable=True),
            Column('TRA_MUNICI', String, nullable=True),
            Column('TRA_UNIDAD', String, nullable=True),
            Column('EVOLUCAO', String, nullable=True),
            Column('DT_OBITO', DateTime, nullable=True),
            Column('CRITERIO', String, nullable=True),
            Column('ANT_REL_CA', String, nullable=True),
            Column("CRIADO_EM", DateTime, default=dt.datetime.now),
            Column("ATUALIZADO_EM", DateTime, default=dt.datetime.now, onupdate=dt.datetime.now),
        )

        # -- define data mapping (could be import if too big)
        # -- 'ID' is four columns: NU_NOTIFIC+ID_AGRAVO+DT_NOTIFIC+ID_MUNICIPIO
        self.mapping = { n:n for n in self._dummy_ }

    def define(self):
        '''
            Return dictionary elements containing the data model and 
            the data mapping, respectively.
        '''
        table_elem = { self.table_name : self.model }
        mapping_elem = { self.table_name : self.mapping }
        return table_elem, mapping_elem


class SinanAidsCriancaInfo:
    def __init__(self, metadata):
        self.metadata = metadata
        self.table_name = 'sinan_aids_crianca_info'
        self._dummy_ = ['IDADE_MAE', 'ESC_MAE', 'RACA_MAE', 'ID_OCUP_MA', 'TIPO_INVES',
                        'ANT_PERINA', 'ANT_REL_N', 'ANT_DROGA', 'ANT_T_HEMO', 'ANT_TRANS_',
                        'ANT_ACIDEN', 'ANTDTTRANS', 'ANTUFTRANS', 'ANTMUNTRAN', 'ANTINSTTRA',
                        'ANT_INVEST', 'LAB_TRIAGE', 'DT_TRIA_11', 'CONFIRMA', 'DTCONFIRMA',
                        'TPRAPIDO1', 'TPRAPIDO2', 'TPRAPIDO3', 'DTRAPIDO1', 'LAB_PCR_1',
                        'DT_PCR_1', 'LAB_PCR_2', 'DT_PCR_2', 'LAB_PCR_3', 'DT_PCR_3',
                        'CLI_PAROTI', 'CLI_DERMA', 'CLI_ESPLEN', 'CLI_HEPATO', 'CLI_INFEC',
                        'CLI_LINFA', 'CLI_ANEMIA', 'CLI_CDC_CA', 'CLI_PULMAO', 'CLI_CA_ORA',
                        'CLI_CDC_CI', 'CLI_CDCCRE', 'CLI_CDC_CR', 'CLI_CRONIC', 'CLI_CDC_EN',
                        'CLI_FEBRE', 'CLI_CDC_GE', 'CLI_HEPATI', 'CLI_HERPE2', 'CLI_CDC_HE',
                        'CLI_HERPES', 'CLI_CDC_HI', 'CLI_CDC_IN', 'CLI_INFCIT', 'CLI_CDC_IS',
                        'CLI_LEIOMI', 'CLI_CDC_LE', 'CLI_LINFO', 'CLI_CDCLIH', 'CLI_CDC_LI',
                        'CLI_MIOCAR', 'CLI_CDC_MI', 'CLI_CDC_ME', 'CLI_NEFRO', 'CLI_NOCAR',
                        'CLI_CDC_PN', 'CLI_CDC_PC', 'CLI_CDC_SA', 'CLI_CDC_SK', 'CLI_CDC_SI',
                        'CLI_CDC_TO', 'CLI_TOX1M', 'CLI_TUPULM', 'CLI_TUBERC', 'CLI_DISSEM',
                        'CLI_VARICE', 'CRI_1500', 'CRI_1000', 'CRI_500', 'EVO_DIAG', 'TRA_UF',
                        'TRA_MUNIC', 'TRA_UNIDAD', 'EVOLUCAO', 'EVO_DT_OBI', 'CRITERIO', 'ANT_CAT_EX']

        # -- define schema for table.
        self.model = Table(
            self.table_name, self.metadata,
            Column("ID", String, ForeignKey('pessoa.ID')),
            Column('IDADE_MAE', String, nullable=True),
            Column('ESC_MAE', String, nullable=True),
            Column('RACA_MAE', String, nullable=True),
            Column('ID_OCUP_MA', String, nullable=True),
            Column('TIPO_INVES', String, nullable=True),
            Column('ANT_PERINA', String, nullable=True),
            Column('ANT_REL_N', String, nullable=True),
            Column('ANT_DROGA', String, nullable=True),
            Column('ANT_T_HEMO', String, nullable=True),
            Column('ANT_TRANS_', String, nullable=True),
            Column('ANT_ACIDEN', String, nullable=True),
            Column('ANTDTTRANS', String, nullable=True),
            Column('ANTUFTRANS', String, nullable=True),
            Column('ANTMUNTRAN', String, nullable=True),
            Column('ANTINSTTRA', String, nullable=True),
            Column('ANT_INVEST', String, nullable=True),
            Column('LAB_TRIAGE', String, nullable=True),
            Column('DT_TRIA_11', DateTime, nullable=True),
            Column('CONFIRMA', String, nullable=True),
            Column('DTCONFIRMA', DateTime, nullable=True),
            Column('TPRAPIDO1', String, nullable=True),
            Column('TPRAPIDO2', String, nullable=True),
            Column('TPRAPIDO3', String, nullable=True),
            Column('DTRAPIDO1', String, nullable=True),
            Column('LAB_PCR_1', String, nullable=True),
            Column('DT_PCR_1', DateTime, nullable=True),
            Column('LAB_PCR_2', String, nullable=True),
            Column('DT_PCR_2', DateTime, nullable=True),
            Column('LAB_PCR_3', String, nullable=True),
            Column('DT_PCR_3', DateTime, nullable=True),
            Column('CLI_PAROTI', String, nullable=True),
            Column('CLI_DERMA', String, nullable=True),
            Column('CLI_ESPLEN', String, nullable=True),
            Column('CLI_HEPATO', String, nullable=True),
            Column('CLI_INFEC', String, nullable=True),
            Column('CLI_LINFA', String, nullable=True),
            Column('CLI_ANEMIA', String, nullable=True),
            Column('CLI_CDC_CA', String, nullable=True),
            Column('CLI_PULMAO', String, nullable=True),
            Column('CLI_CA_ORA', String, nullable=True),
            Column('CLI_CDC_CI', String, nullable=True),
            Column('CLI_CDCCRE', String, nullable=True),
            Column('CLI_CDC_CR', String, nullable=True),
            Column('CLI_CRONIC', String, nullable=True),
            Column('CLI_CDC_EN', String, nullable=True),
            Column('CLI_FEBRE', String, nullable=True),
            Column('CLI_CDC_GE', String, nullable=True),
            Column('CLI_HEPATI', String, nullable=True),
            Column('CLI_HERPE2', String, nullable=True),
            Column('CLI_CDC_HE', String, nullable=True),
            Column('CLI_HERPES', String, nullable=True),
            Column('CLI_CDC_HI', String, nullable=True),
            Column('CLI_CDC_IN', String, nullable=True),
            Column('CLI_INFCIT', String, nullable=True),
            Column('CLI_CDC_IS', String, nullable=True),
            Column('CLI_LEIOMI', String, nullable=True),
            Column('CLI_CDC_LE', String, nullable=True),
            Column('CLI_LINFO', String, nullable=True),
            Column('CLI_CDCLIH', String, nullable=True),
            Column('CLI_CDC_LI', String, nullable=True),
            Column('CLI_MIOCAR', String, nullable=True),
            Column('CLI_CDC_MI', String, nullable=True),
            Column('CLI_CDC_ME', String, nullable=True),
            Column('CLI_NEFRO', String, nullable=True),
            Column('CLI_NOCAR', String, nullable=True),
            Column('CLI_CDC_PN', String, nullable=True),
            Column('CLI_CDC_PC', String, nullable=True),
            Column('CLI_CDC_SA', String, nullable=True),
            Column('CLI_CDC_SK', String, nullable=True),
            Column('CLI_CDC_SI', String, nullable=True),
            Column('CLI_CDC_TO', String, nullable=True),
            Column('CLI_TOX1M', String, nullable=True),
            Column('CLI_TUPULM', String, nullable=True),
            Column('CLI_TUBERC', String, nullable=True),
            Column('CLI_DISSEM', String, nullable=True),
            Column('CLI_VARICE', String, nullable=True),
            Column('CRI_1500', String, nullable=True),
            Column('CRI_1000', String, nullable=True),
            Column('CRI_500', String, nullable=True),
            Column('EVO_DIAG', String, nullable=True),
            Column('TRA_UF', String, nullable=True),
            Column('TRA_MUNIC', String, nullable=True),
            Column('TRA_UNIDAD', String, nullable=True),
            Column('EVOLUCAO', String, nullable=True),
            Column('EVO_DT_OBI', DateTime, nullable=True),
            Column('CRITERIO', String, nullable=True),
            Column('ANT_CAT_EX', String, nullable=True),
            Column("CRIADO_EM", DateTime, default=dt.datetime.now),
            Column("ATUALIZADO_EM", DateTime, default=dt.datetime.now, onupdate=dt.datetime.now),
        )

        # -- define data mapping (could be import if too big)
        # -- 'ID' is four columns: NU_NOTIFIC+ID_AGRAVO+DT_NOTIFIC+ID_MUNICIPIO
        self.mapping = { n:n for n in self._dummy_ }

    def define(self):
        '''
            Return dictionary elements containing the data model and 
            the data mapping, respectively.
        '''
        table_elem = { self.table_name : self.model }
        mapping_elem = { self.table_name : self.mapping }
        return table_elem, mapping_elem

class SiclomInfo:
    def __init__(self, metadata):
        self.metadata = metadata
        self.table_name = 'siclom_info'
        self._dummy_ = ["ID", "UDM", "CODIGO_IBGE_NASC", "RACA", "ESTADO_CIVIL", "ESCOLARIDADE", 
                        "CD4_INICIO_TARV", "CV_INICIO_TARV", "ANO_INICIO_TARV",
                        "DT_CADASTRO", "DT_DIGITACAO", "DT_ULT_ATU", "ST_PACIENTE"]

        # -- define schema for table.
        self.model = Table(
            self.table_name, self.metadata,
            Column("ID", String, ForeignKey('pessoa.ID')),
            Column('UDM', String, nullable=True),
            Column('CODIGO_IBGE_NASC', String, nullable=True),
            Column('RACA', String, nullable=True),
            Column('ESTADO_CIVIL', String, nullable=True),
            Column('ESCOLARIDADE', String, nullable=True),
            Column('CD4_INICIO_TARV', String, nullable=True),
            Column('CV_INICIO_TARV', String, nullable=True),
            Column('ANO_INICIO_TARV', String, nullable=True),
            Column('DT_CADASTRO', DateTime, nullable=True),
            Column('DT_DIGITACAO', DateTime, nullable=True),
            Column('DT_ULT_ATU', DateTime, nullable=True),
            Column('ST_PACIENTE', String, nullable=True),
            Column("CRIADO_EM", DateTime, default=dt.datetime.now),
            Column("ATUALIZADO_EM", DateTime, default=dt.datetime.now, onupdate=dt.datetime.now),
        )

        # -- define data mapping (could be import if too big)
        self.mapping = { n:n for n in self._dummy_ }

    def define(self):
        '''
            Return dictionary elements containing the data model and 
            the data mapping, respectively.
        '''
        table_elem = { self.table_name : self.model }
        mapping_elem = { self.table_name : self.mapping }
        return table_elem, mapping_elem

    
# ---------- MATCHING DATA MODELS ----------
class PairsLabel:
    def __init__(self, metadata):
        self.metadata = metadata
        self.table_name = 'pairs_label'

        # --> define schema for table.
        self.model = Table(
            self.table_name, self.metadata,
            Column("FMT_ID", String, primary_key=True),
            Column("ID_1", String, nullable=False),
            Column("ID_2", String, nullable=False),
            Column("PROBA_NEGATIVO_MODELO_1", Float(5), nullable=True),
            Column("PROBA_NEGATIVO_MODELO_2", Float(5), nullable=True),
            Column("PROBA_NEGATIVO_MODELO_3", Float(5), nullable=True),
            Column("CRIADO_EM", DateTime, default=dt.datetime.now),
        )

        # -- define data mapping (could be imported if too big) - include all columns!
        self.mapping = {
            "ID_1" : "ID_1",  "ID_2" : "ID_2", 
            "FMT_ID": "FMT_ID",
            "PROBA_NEGATIVO_MODELO_1" : "PROBA_NEGATIVO_MODELO_1",
            "PROBA_NEGATIVO_MODELO_2": "PROBA_NEGATIVO_MODELO_2",
            "PROBA_NEGATIVO_MODELO_3": "PROBA_NEGATIVO_MODELO_3",  
        }

    def define(self):
        '''
            Return dictionary elements containing the data model and 
            the data mapping, respectively.
        '''
        table_elem = { self.table_name : self.model }
        mapping_elem = { self.table_name : self.mapping }
        return table_elem, mapping_elem