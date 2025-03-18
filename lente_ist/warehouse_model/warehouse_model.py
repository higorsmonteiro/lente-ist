'''
    Define the warehouse to store relevant data for IST disease.

    The focus of this warehouse is to store relevant data for CEVEPI that are not 
    directly accesible with SQL queries in official databases. Examples include
    the ones from SINAN online (arboviruses) or SIVEP-GRIPE.

    Author: Higor S. Monteiro
    Email: higor.monteiro@fisica.ufc.br
'''

from sqlalchemy import create_engine, MetaData
# -- import the data models
from lente_ist.warehouse_model.warehouse_base import WarehouseBase
from lente_ist.warehouse_model.data_models import Pessoa, SinanAidsAdultoInfo, SinanAidsCriancaInfo, SiclomInfo, SimInfo, SimcInfo, SiscelInfo 
from lente_ist.warehouse_model.data_models import PositivePairsLabel, NegativePairsLabel

class WarehouseHIV(WarehouseBase):
    def __init__(self, engine_url):
        self._engine = create_engine(engine_url, future=True)
        self._metadata = MetaData()
        self._tables = {}
        self._mappings = {}

        # -- include the data models
        self._imported_data_models = [ Pessoa(self._metadata).define(),
                                       SinanAidsAdultoInfo(self._metadata).define(),
                                       SinanAidsCriancaInfo(self._metadata).define(),
                                       SiclomInfo(self._metadata).define(),
                                       SimInfo(self._metadata).define(),
                                       SimcInfo(self._metadata).define(), 
                                       SiscelInfo(self._metadata).define(),  
                                       PositivePairsLabel(self._metadata).define(),
                                       NegativePairsLabel(self._metadata).define() ]

        for elem in self._imported_data_models:
            self._tables.update(elem[0])
            self._mappings.update(elem[1])