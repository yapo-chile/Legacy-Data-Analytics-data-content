# pylint: disable=no-member
# utf-8
import sys
import logging
from functools import reduce
import pandas as pd
from infraestructure.athena import Athena
from infraestructure.conf import getConf
from infraestructure.psql import Database
from utils.query import Query
from utils.read_params import ReadParams
from utils.time_execution import TimeExecution


# Query data from Pulse bucket
def source_data_pulse(params: ReadParams,
                      config: getConf):
    athena = Athena(conf=config.athenaConf)
    query = Query(config, params)
    data_athena = athena.get_data(query.query_base_pulse())
    athena.close_connection()
    return data_athena

# Query data from data warehouse
def source_data_dwh(params: ReadParams,
                    config: getConf):
    query = Query(config, params)
    db_source = Database(conf=config.DWConf)
    data_dwh = db_source.select_to_dict(query.query_base_postgresql_dw())
    db_source.close_connection()
    return data_dwh

# Query data from blocket DB
def source_data_blocket(params: ReadParams,
                        config: getConf):
    query = Query(config, params)
    db_source = Database(conf=config.blocketConf)
    data_blocket = db_source.select_to_dict( \
        query.query_base_postgresql_blocket())
    db_source.close_connection()
    return data_blocket

# Query data from data warehouse
def source_data_dwh_dau_xiti(params: ReadParams,
                             config: getConf):
    query = Query(config, params)
    db_source = Database(conf=config.DWConf)
    data_dwh = db_source.select_to_dict(query.get_traffic_xiti())
    db_source.close_connection()
    return data_dwh

# Query data from data warehouse
def source_data_dwh_leads_xiti(params: ReadParams,
                               config: getConf):
    query = Query(config, params)
    db_source = Database(conf=config.DWConf)
    data_dwh = db_source.select_to_dict(query.get_leads_xiti())
    db_source.close_connection()
    return data_dwh

# Query data from data warehouse
def source_data_dwh_traffic_pulse(params: ReadParams,
                                  config: getConf):
    query = Query(config, params)
    db_source = Database(conf=config.DWConf)
    data_dwh = db_source.select_to_dict(query.get_traffic_pulse())
    db_source.close_connection()
    return data_dwh

# Query data from data warehouse
def source_data_dwh_leads_pulse(params: ReadParams,
                                config: getConf):
    query = Query(config, params)
    db_source = Database(conf=config.DWConf)
    data_dwh = db_source.select_to_dict(query.get_unique_leads_pulse())
    db_source.close_connection()
    return data_dwh

# Query data from Pulse bucket
def source_data_pulse_leads_per_user(params: ReadParams,
                                     config: getConf):
    athena = Athena(conf=config.athenaConf)
    query = Query(config, params)
    data_athena = athena.get_data(query.get_leads_per_user())
    athena.close_connection()
    return data_athena

# Query data from Pulse bucket
def source_data_pulse_liquidity(params: ReadParams,
                                config: getConf):
    athena = Athena(conf=config.athenaConf)
    query = Query(config, params)
    data_athena = athena.get_data(query.get_liquidity())
    athena.close_connection()
    return data_athena

# Write data to data warehouse
def write_data_dwh_traffic(params: ReadParams,
                           config: getConf,
                           data_dwh: pd.DataFrame) -> None:
    query = Query(config, params)
    DB_WRITE = Database(conf=config.DWConf)
    DB_WRITE.execute_command(query.delete_base_traffic())
    DB_WRITE.insert_data_traffic(data_dwh)
    DB_WRITE.close_connection()

# Write data to data warehouse
def write_data_dwh_leads_per_user(params: ReadParams,
                                  config: getConf,
                                  data_dwh: pd.DataFrame) -> None:
    query = Query(config, params)
    DB_WRITE = Database(conf=config.DWConf)
    DB_WRITE.execute_command(query.delete_base_leads_per_user())
    DB_WRITE.insert_data_leads_per_user(data_dwh)
    DB_WRITE.close_connection()

# Write data to data warehouse
def write_data_dwh_liquidity(params: ReadParams,
                             config: getConf,
                             data_dwh: pd.DataFrame) -> None:
    query = Query(config, params)
    DB_WRITE = Database(conf=config.DWConf)
    DB_WRITE.execute_command(query.delete_base_liquidity())
    DB_WRITE.insert_data_liquidity(data_dwh)
    DB_WRITE.close_connection()

if __name__ == '__main__':
    CONFIG = getConf()
    TIME = TimeExecution()
    LOGGER = logging.getLogger('traffic-xiti')
    DATE_FORMAT = """%(asctime)s,%(msecs)d %(levelname)-2s """
    INFO_FORMAT = """[%(filename)s:%(lineno)d] %(message)s"""
    LOG_FORMAT = DATE_FORMAT + INFO_FORMAT
    logging.basicConfig(format=LOG_FORMAT, level=logging.INFO)
    PARAMS = ReadParams(sys.argv)

 ###################################################
 #                     EXTRACT                     #
 ###################################################

 #   DATA_DWH = source_data_dwh(PARAMS, CONFIG)
 #   print('DATA_DWH = ' + DATA_DWH)
 #   DATA_BLOCKET = source_data_blocket(PARAMS, CONFIG)
 #   print('DATA_BLOCKET = ' + DATA_BLOCKET)
 #   DATA_ATHENA = source_data_pulse(PARAMS, CONFIG)
 #   print('DATA_ATHENA = ' + DATA_ATHENA)
 ## DAU Xiti
    DATA_DAU_XITI = source_data_dwh_dau_xiti(PARAMS, CONFIG)
    print('DATA_DAU_XITI')
    print(DATA_DAU_XITI.head(20))
 ## Leads Xiti
    DATA_LEADS_XITI = source_data_dwh_leads_xiti(PARAMS, CONFIG)
    print('DATA_LEADS_XITI')
    print(DATA_LEADS_XITI.head(20))
 ## Traffic Pulse
    DATA_TRAFFIC_PULSE = source_data_dwh_traffic_pulse(PARAMS, CONFIG)
    print('DATA_TRAFFIC_PULSE')
    print(DATA_TRAFFIC_PULSE.head(20))
 ## Unique Leads Pulse
    DATA_LEADS_PULSE = source_data_dwh_leads_pulse(PARAMS, CONFIG)
    print('DATA_LEADS_PULSE')
    print(DATA_LEADS_PULSE.head(20))
## Unique Leads per user
    DATA_LEADS_PER_USER = source_data_pulse_leads_per_user(PARAMS, CONFIG)
    print('DATA_LEADS_PER_USER')
    print(DATA_LEADS_PER_USER.head(20))
## Unique Liquidity
    DATA_LIQUIDITY = source_data_pulse_liquidity(PARAMS, CONFIG)
    print('DATA_LIQUIDITY')
    print(DATA_LIQUIDITY.head(20))

 ###################################################
 #                   TRANSFORM                     #
 ###################################################

 # Merging all extraction traffic data
    DATAFRAMES = [DATA_DAU_XITI, DATA_LEADS_XITI, DATA_TRAFFIC_PULSE,
                  DATA_LEADS_PULSE]

    DF_TRAFFIC = reduce(lambda  left, right: \
        pd.merge(left, right, on=['timedate', 'vertical', 'platform'],
                 how='outer'), DATAFRAMES).fillna(0)

    DF_TRAFFIC[["dau_xiti", "visits_xiti", "leads_xiti",
                "dau_pulse", "visits_pulse", "browsers_pulse",
                "buyers_pulse", "unique_leads_pulse"]] = \
        DF_TRAFFIC[["dau_xiti", "visits_xiti", "leads_xiti",
                    "dau_pulse", "visits_pulse", "browsers_pulse",
                    "buyers_pulse", "unique_leads_pulse"]].astype(int)
    print('DF_TRAFFIC')
    print(DF_TRAFFIC.head(20))
 ###################################################
 #                     LOAD                        #
 ###################################################
    write_data_dwh_traffic(PARAMS, CONFIG, DF_TRAFFIC)
    print('DF_TRAFFIC inserted')
    write_data_dwh_leads_per_user(PARAMS, CONFIG, DATA_LEADS_PER_USER)
    print('DATA_LEADS_PER_USER inserted')
    write_data_dwh_liquidity(PARAMS, CONFIG, DATA_LIQUIDITY)
    print('DATA_LIQUIDITY inserted')
    TIME.get_time()
    LOGGER.info('Process ended successfully.')
