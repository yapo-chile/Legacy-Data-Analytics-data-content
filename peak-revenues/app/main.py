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

# Query data from dwh
def source_data_premium_product(params: ReadParams,
                                config: getConf) -> pd.DataFrame:
    dwh_read = Database(conf=config.db)
    query = Query()
    data_pp = dwh_read.select_to_dict(query.query_premium_products(params))
    dwh_read.close_connection()
    return data_pp

def source_data_packs_motors(params: ReadParams,
                             config: getConf) -> pd.DataFrame:
    dwh_read = Database(conf=config.db)
    query = Query()
    data_pre = dwh_read.select_to_dict(query.query_packs_motors(params))
    dwh_read.close_connection()
    return data_pre

def source_data_packs_real_estate(params: ReadParams,
                                  config: getConf) -> pd.DataFrame:
    dwh_read = Database(conf=config.db)
    query = Query()
    data_pre = dwh_read.select_to_dict(query.query_packs_real_estate(params))
    dwh_read.close_connection()
    return data_pre

def source_data_insertion_fee_motors(params: ReadParams,
                                     config: getConf) -> pd.DataFrame:
    dwh_read = Database(conf=config.db)
    query = Query()
    data_ifee = dwh_read.select_to_dict(query.\
                                        query_insertion_fee_motors(params))
    dwh_read.close_connection()
    return data_ifee

def source_data_insertion_fee_real_estate(params: ReadParams,
                                          config: getConf) -> pd.DataFrame:
    dwh_read = Database(conf=config.db)
    query = Query()
    data_ifee = dwh_read.select_to_dict(query.\
                                        query_insertion_fee_real_estate(params))
    dwh_read.close_connection()
    return data_ifee

def source_data_insertion_fee_jobs(params: ReadParams,
                                   config: getConf) -> pd.DataFrame:
    dwh_read = Database(conf=config.db)
    query = Query()
    data_ifee = dwh_read.select_to_dict(query.query_insertion_fee_jobs(params))
    dwh_read.close_connection()
    return data_ifee

# Write data to data warehouse
def write_data_dwh(params: ReadParams,
                   config: getConf,
                   data_pp: pd.DataFrame,
                   data_pre: pd.DataFrame,
                   data_pmo: pd.DataFrame,
                   data_ifee_m: pd.DataFrame,
                   data_ifee_re: pd.DataFrame,
                   data_ifee_job: pd.DataFrame,) -> None:
    query = Query()
    DB_WRITE = Database(conf=config.db)
    DB_WRITE.execute_command(query.truncate_table(params))
    DB_WRITE.insert_data(data_pp)
    DB_WRITE.insert_data(data_pre)
    DB_WRITE.insert_data(data_pmo)
    DB_WRITE.insert_data(data_ifee_m)
    DB_WRITE.insert_data(data_ifee_re)
    DB_WRITE.insert_data(data_ifee_job)
    DB_WRITE.close_connection()

if __name__ == '__main__':
    CONFIG = getConf()
    TIME = TimeExecution()
    LOGGER = logging.getLogger('data-pipeline-base')
    DATE_FORMAT = """%(asctime)s,%(msecs)d %(levelname)-2s """
    INFO_FORMAT = """[%(filename)s:%(lineno)d] %(message)s"""
    LOG_FORMAT = DATE_FORMAT + INFO_FORMAT
    logging.basicConfig(format=LOG_FORMAT, level=logging.INFO)
    PARAMS = ReadParams(sys.argv)
    DATA_PP = source_data_premium_product(PARAMS, CONFIG)
    DATA_PRE = source_data_packs_real_estate(PARAMS, CONFIG)
    DATA_PMO = source_data_packs_motors(PARAMS, CONFIG)
    DATA_IFEE_M = source_data_insertion_fee_motors(PARAMS, CONFIG)
    DATA_IFEE_RE = source_data_insertion_fee_real_estate(PARAMS, CONFIG)
    DATA_IFEE_JOB = source_data_insertion_fee_jobs(PARAMS, CONFIG)
    write_data_dwh(PARAMS,
                   CONFIG,
                   DATA_PP,
                   DATA_PRE,
                   DATA_PMO,
                   DATA_IFEE_M,
                   DATA_IFEE_RE,
                   DATA_IFEE_JOB)
    TIME.get_time()
    LOGGER.info('Process ended successfully.')
