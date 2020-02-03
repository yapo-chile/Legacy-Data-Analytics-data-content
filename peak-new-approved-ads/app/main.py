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

# Query data from data warehouse
def source_data_dwh(params: ReadParams,
                    config: getConf):
    query = Query()
    db_source = Database(conf=config.db)
    data_dwh = db_source.select_to_dict(query \
                                        .query_new_approved_ads(params))
    db_source.close_connection()
    return data_dwh

# Write data to data warehouse
def write_data_dwh(params: ReadParams,
                   config: getConf,
                   data_new_ads: pd.DataFrame) -> None:
    query = Query()
    DB_WRITE = Database(conf=config.db)
    DB_WRITE.execute_command(query.delete_new_approved_ads(params))
    DB_WRITE.insert_data(data_new_ads)
    DB_WRITE.close_connection()

if __name__ == '__main__':
    CONFIG = getConf()
    TIME = TimeExecution()
    LOGGER = logging.getLogger('peak-new-approved-ads')
    DATE_FORMAT = """%(asctime)s,%(msecs)d %(levelname)-2s """
    INFO_FORMAT = """[%(filename)s:%(lineno)d] %(message)s"""
    LOG_FORMAT = DATE_FORMAT + INFO_FORMAT
    logging.basicConfig(format=LOG_FORMAT, level=logging.INFO)
    PARAMS = ReadParams(sys.argv)
    DATA_NEW_ADS = source_data_dwh(PARAMS, CONFIG)
    write_data_dwh(PARAMS, CONFIG, DATA_NEW_ADS)
    TIME.get_time()
    LOGGER.info('Process ended successfully.')
    LOGGER.info('Base project.')
