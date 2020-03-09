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
    query = Query(config, params)
    db_source = Database(conf=config.db)
    data_dwh = db_source.select_to_dict(query \
                                        .query_leads_and_uniq_leads())
    db_source.close_connection()
    return data_dwh

# Write data to data warehouse
def write_data_dwh(params: ReadParams,
                   config: getConf,
                   data_dwh: pd.DataFrame) -> None:
    query = Query(config, params)
    DB_WRITE = Database(conf=config.db)
    DB_WRITE.execute_command(query.delete_leads_and_uniq_leads())
    DB_WRITE.insert_data(data_dwh)
    DB_WRITE.close_connection()

if __name__ == '__main__':
    CONFIG = getConf()
    TIME = TimeExecution()
    LOGGER = logging.getLogger('lead-and-unique-lead')
    DATE_FORMAT = """%(asctime)s,%(msecs)d %(levelname)-2s """
    INFO_FORMAT = """[%(filename)s:%(lineno)d] %(message)s"""
    LOG_FORMAT = DATE_FORMAT + INFO_FORMAT
    logging.basicConfig(format=LOG_FORMAT, level=logging.INFO)
    PARAMS = ReadParams(sys.argv)
    DATA_DWH = source_data_dwh(PARAMS, CONFIG)
    write_data_dwh(PARAMS, CONFIG, DATA_DWH)
    TIME.get_time()
    LOGGER.info('Process ended successfully.')
