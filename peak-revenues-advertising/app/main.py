# pylint: disable=no-member
# utf-8
import sys
import logging
import pandas as pd
from infraestructure.conf import getConf
from infraestructure.psql import Database
from utils.query import Query
from utils.read_params import ReadParams
from utils.time_execution import TimeExecution
from utils.api_request import get_drive_sheet_dataframe

# Query data from Pulse bucket
def source_data_google_sheet(sheet: str, config: getConf) -> pd.DataFrame:
    data_sheet = get_drive_sheet_dataframe(sheet,
                                           config)
    if sheet == 'Venta Directa':
        data_sheet = data_sheet[data_sheet['monto acumulado'] != '']
        data_sheet = data_sheet. \
                        rename(columns={'monto acumulado': 'monto_acumulado'})
    elif sheet == 'Redes':
        data_sheet = data_sheet[data_sheet['monto usd'] != '']
        data_sheet = data_sheet.rename(columns={'tipo de red': 'tipo_de_red'})
        data_sheet = data_sheet.rename(columns={'monto usd': 'monto_usd'})
        data_sheet = data_sheet.rename(columns={'monto pesos': 'monto_pesos'})
        data_sheet = data_sheet.rename(columns={'fecha ': 'fecha'})
    return data_sheet

# Write data to data warehouse
def write_data_dwh(params: ReadParams,
                   config: getConf,
                   data_direct_sales: pd.DataFrame,
                   data_network_sales: pd.DataFrame) -> None:
    query = Query(config)
    DB_WRITE = Database(conf=config.db)
    DB_WRITE.execute_command(query.delete_direct_sales())
    DB_WRITE.execute_command(query.delete_network_sales())
    DB_WRITE.insert_data_direct_sales(data_direct_sales)
    DB_WRITE.insert_data_network_sales(data_network_sales)
    DB_WRITE.close_connection()

if __name__ == '__main__':
    CONFIG = getConf()
    TIME = TimeExecution()
    LOGGER = logging.getLogger('peak-revenues-advertising')
    DATE_FORMAT = """%(asctime)s,%(msecs)d %(levelname)-2s """
    INFO_FORMAT = """[%(filename)s:%(lineno)d] %(message)s"""
    LOG_FORMAT = DATE_FORMAT + INFO_FORMAT
    logging.basicConfig(format=LOG_FORMAT, level=logging.INFO)
    PARAMS = ReadParams(sys.argv)
    DATA_DIRECT_SALES = source_data_google_sheet('Venta Directa',
                                                 CONFIG)
    DATA_NETWORK_SALES = source_data_google_sheet('Redes',
                                                  CONFIG)
    write_data_dwh(PARAMS, CONFIG, DATA_DIRECT_SALES, DATA_NETWORK_SALES)
    TIME.get_time()
    LOGGER.info('Process ended successfully.')
