# pylint: disable=no-member
# utf-8
import sys
import logging
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
def source_data_dwh_naa_vert_plat(params: ReadParams,
                                  config: getConf):
    query = Query(config, params)
    db_source = Database(conf=config.DWConf)
    data_dwh = db_source.select_to_dict(query.get_naa_vertical_platform())
    db_source.close_connection()
    return data_dwh

# Query data from data warehouse
def source_data_dwh_sel_vert_plat(params: ReadParams,
                                  config: getConf):
    query = Query(config, params)
    db_source = Database(conf=config.DWConf)
    data_dwh = db_source.select_to_dict(query.get_sellers_vertical_platform())
    db_source.close_connection()
    return data_dwh

# Query data from data warehouse
def source_data_dwh_sel_plat_all_yapo(params: ReadParams,
                                      config: getConf):
    query = Query(config, params)
    db_source = Database(conf=config.DWConf)
    data_dwh = db_source.select_to_dict(query.get_sellers_platform_all_yapo())
    db_source.close_connection()
    return data_dwh

# Query data from data warehouse
def source_data_dwh_sel_vert_all_yapo(params: ReadParams,
                                      config: getConf):
    query = Query(config, params)
    db_source = Database(conf=config.DWConf)
    data_dwh = db_source.select_to_dict(query.get_sellers_vertical_all_yapo())
    db_source.close_connection()
    return data_dwh

# Query data from data warehouse
def source_data_dwh_sel_vert_plat_all_yapo(params: ReadParams,
                                           config: getConf):
    query = Query(config, params)
    db_source = Database(conf=config.DWConf)
    data_dwh = db_source.select_to_dict(query.get_sellers_vertical_platform_all_yapo())
    db_source.close_connection()
    return data_dwh

# Query data from data warehouse
def source_data_dwh_nia_vert_plat(params: ReadParams,
                                  config: getConf):
    query = Query(config, params)
    db_source = Database(conf=config.DWConf)
    data_dwh = db_source.select_to_dict(query.get_nia_vertical_platform())
    db_source.close_connection()
    return data_dwh

# Write data to data warehouse
def write_data_dwh(params: ReadParams,
                   config: getConf,
                   data_dwh: pd.DataFrame) -> None:
    query = Query(config, params)
    DB_WRITE = Database(conf=config.DWConf)
    DB_WRITE.execute_command(query.delete_base())
    DB_WRITE.insert_data(data_dwh)
    DB_WRITE.close_connection()

if __name__ == '__main__':
    CONFIG = getConf()
    TIME = TimeExecution()
    LOGGER = logging.getLogger('peak-content-metrics')
    DATE_FORMAT = """%(asctime)s,%(msecs)d %(levelname)-2s """
    INFO_FORMAT = """[%(filename)s:%(lineno)d] %(message)s"""
    LOG_FORMAT = DATE_FORMAT + INFO_FORMAT
    logging.basicConfig(format=LOG_FORMAT, level=logging.INFO)
    PARAMS = ReadParams(sys.argv)

 ###################################################
 #                     EXTRACT                     #
 ###################################################
    #DATA_ATHENA = source_data_pulse(PARAMS, CONFIG)
    #DATA_DWH = source_data_dwh(PARAMS, CONFIG)

 ## Naa Vertical Platform
    DATA_NAA_VERT_PLAT = source_data_dwh_naa_vert_plat(PARAMS, CONFIG)
    LOGGER.info('DATA_NAA_VERT_PLAT extracted')
    LOGGER.info(DATA_NAA_VERT_PLAT.head(5))

 ## Sellers Vertical Platform
    DATA_SEL_VERT_PLAT = source_data_dwh_sel_vert_plat(PARAMS, CONFIG)
    LOGGER.info('DATA_SEL_VERT_PLAT extracted')
    LOGGER.info(DATA_SEL_VERT_PLAT.head(5))

 ## Sellers Platform All Yapo
    DATA_SEL_PLAT_ALL_YAPO = source_data_dwh_sel_plat_all_yapo(PARAMS, CONFIG)
    LOGGER.info('DATA_SEL_PLAT_ALL_YAPO extracted')
    LOGGER.info(DATA_SEL_PLAT_ALL_YAPO.head(5))

 ## Sellers Vertical All Yapo
    DATA_SEL_VERT_ALL_YAPO = source_data_dwh_sel_vert_all_yapo(PARAMS, CONFIG)
    LOGGER.info('DATA_SEL_VERT_ALL_YAPO extracted')
    LOGGER.info(DATA_SEL_VERT_ALL_YAPO.head(5))

 ## Sellers Vertical Platform All Yapo
    DATA_SEL_VERT_PLAT_ALL_YAPO = source_data_dwh_sel_vert_plat_all_yapo(PARAMS, CONFIG)
    LOGGER.info('DATA_SEL_VERT_PLAT_ALL_YAPO extracted')
    LOGGER.info(DATA_SEL_VERT_PLAT_ALL_YAPO.head(5))

 ## Nia Vertical Platform
    DATA_NIA_VERT_PLAT = source_data_dwh_nia_vert_plat(PARAMS, CONFIG)
    LOGGER.info('DATA_NIA_VERT_PLAT extracted')
    LOGGER.info(DATA_NIA_VERT_PLAT.head(5))

 ###################################################
 #                   TRANSFORM                     #
 ###################################################

 ## Creating Platform All Yapo (NAA)
    DF_NAA_PLAT_ALL_YAPO = DATA_NAA_VERT_PLAT.groupby(['approval_date', 'vertical']).\
        agg({'new_ads':'sum', 'naa_pri':'sum', 'naa_pro':'sum'}).reset_index()

    DF_NAA_PLAT_ALL_YAPO['platform'] = 'All Yapo'

    DF_NAA_PLAT_ALL_YAPO = DF_NAA_PLAT_ALL_YAPO[['approval_date', 'vertical', 'platform',
                                                 'new_ads', 'naa_pri', 'naa_pro']]
    LOGGER.info('DF_NAA_PLAT_ALL_YAPO transformed')

 ## Creating Vertical All Yapo (NAA)
    DF_NAA_VERT_ALL_YAPO = DATA_NAA_VERT_PLAT.groupby(['approval_date', 'platform']).\
        agg({'new_ads':'sum', 'naa_pri':'sum', 'naa_pro':'sum'}).reset_index()

    DF_NAA_VERT_ALL_YAPO['vertical'] = 'All Yapo'

    DF_NAA_VERT_ALL_YAPO = DF_NAA_VERT_ALL_YAPO[['approval_date', 'vertical', 'platform',
                                                 'new_ads', 'naa_pri', 'naa_pro']]
    LOGGER.info('DF_NAA_VERT_ALL_YAPO transformed')

 ## Creating Vertical and Platform All Yapo (NAA)
    DF_NAA_PLAT_VERT_ALL_YAPO = DATA_NAA_VERT_PLAT.groupby(['approval_date']).\
        agg({'new_ads':'sum', 'naa_pri':'sum', 'naa_pro':'sum'}).reset_index()

    DF_NAA_PLAT_VERT_ALL_YAPO['vertical'] = 'All Yapo'
    DF_NAA_PLAT_VERT_ALL_YAPO['platform'] = 'All Yapo'

    DF_NAA_PLAT_VERT_ALL_YAPO = DF_NAA_PLAT_VERT_ALL_YAPO[['approval_date', 'vertical',
                                                           'platform', 'new_ads', 'naa_pri',
                                                           'naa_pro']]
    LOGGER.info('DF_NAA_PLAT_VERT_ALL_YAPO transformed')

 ## Appending new rows to new approved ads metrics df
    DF_NAA_ADS = DATA_NAA_VERT_PLAT.append(DF_NAA_VERT_ALL_YAPO, ignore_index=True, sort=False)\
        .append(DF_NAA_PLAT_ALL_YAPO, ignore_index=True, sort=False)\
            .append(DF_NAA_PLAT_VERT_ALL_YAPO, ignore_index=True, sort=False)\
                .sort_values(['approval_date', 'platform', 'vertical'])\
                    .reset_index(drop=True)
    LOGGER.info('DF_NAA_ADS transformed')

 ## Appending new rows to sellers metrics df
    DF_SELLERS = DATA_SEL_VERT_PLAT.append(DATA_SEL_PLAT_ALL_YAPO, ignore_index=True, sort=False)\
        .append(DATA_SEL_VERT_ALL_YAPO, ignore_index=True, sort=False)\
            .append(DATA_SEL_VERT_PLAT_ALL_YAPO, ignore_index=True, sort=False)\
                .sort_values(['approval_date', 'platform', 'vertical'])\
                    .reset_index(drop=True)
    LOGGER.info('DF_SELLERS transformed')

 ## Creating Platform All Yapo (NIA)
    DF_NIA_PLAT_ALL_YAPO = DATA_NIA_VERT_PLAT.groupby(['creation_date', 'vertical']).\
        agg({'new_inserted_ads':'sum', 'nia_pri':'sum', 'nia_pro':'sum'}).reset_index()
    LOGGER.info('DF_NIA_PLAT_ALL_YAPO transformed')

 ## Creating Vertical All Yapo (NIA)
    DF_NIA_VERT_ALL_YAPO = DATA_NIA_VERT_PLAT.groupby(['creation_date', 'platform']).\
        agg({'new_inserted_ads':'sum', 'nia_pri':'sum', 'nia_pro':'sum'}).reset_index()

    DF_NIA_VERT_ALL_YAPO['vertical'] = 'All Yapo'

    DF_NIA_VERT_ALL_YAPO = DF_NIA_VERT_ALL_YAPO[['creation_date', 'vertical', 'platform',
                                                 'new_inserted_ads', 'nia_pri', 'nia_pro']]
    LOGGER.info('DF_NIA_VERT_ALL_YAPO transformed')

 ## Creating Vertical and Platform All Yapo (NIA)
    DF_NIA_PLAT_VERT_ALL_YAPO = DATA_NIA_VERT_PLAT.groupby(['creation_date']).\
        agg({'new_inserted_ads':'sum', 'nia_pri':'sum', 'nia_pro':'sum'}).reset_index()

    DF_NIA_PLAT_VERT_ALL_YAPO['vertical'] = 'All Yapo'
    DF_NIA_PLAT_VERT_ALL_YAPO['platform'] = 'All Yapo'

    DF_NIA_PLAT_VERT_ALL_YAPO = DF_NIA_PLAT_VERT_ALL_YAPO[['creation_date', 'vertical',
                                                           'platform', 'new_inserted_ads',
                                                           'nia_pri', 'nia_pro']]
    LOGGER.info('DF_NIA_PLAT_VERT_ALL_YAPO transformed')

 ## Appending new rows to new inserted ads metrics df
    DF_NIA_ADS = DATA_NIA_VERT_PLAT.append(DF_NIA_VERT_ALL_YAPO, ignore_index=True, sort=False)\
        .append(DF_NIA_PLAT_ALL_YAPO, ignore_index=True, sort=False)\
            .append(DF_NIA_PLAT_VERT_ALL_YAPO, ignore_index=True, sort=False)\
                .sort_values(['creation_date', 'platform', 'vertical'])\
                    .reset_index(drop=True)
    LOGGER.info('DF_NIA_ADS transformed')

 ## Merging all DF´s
    DF_NIA_ADS.rename(columns={'creation_date':'timedate'}, inplace=True)
    DF_NIA_ADS.set_index(['timedate', 'vertical', 'platform'], inplace=True)
    DF_SELLERS.rename(columns={'approval_date':'timedate'}, inplace=True)
    DF_SELLERS.set_index(['timedate', 'vertical', 'platform'], inplace=True)
    DF_NAA_ADS.rename(columns={'approval_date':'timedate'}, inplace=True)
    DF_NAA_ADS.set_index(['timedate', 'vertical', 'platform'], inplace=True)

    DF_CONTENT = DF_NAA_ADS.merge(DF_NIA_ADS, left_index=True, right_index=True)

    DF_CONTENT = DF_CONTENT.merge(DF_SELLERS, left_index=True, right_index=True)

    DF_CONTENT = DF_CONTENT.reset_index().sort_values(['timedate', 'platform', 'vertical']).\
        reset_index(drop=True)
    LOGGER.info('DF_CONTENT transformed')
 #  exit()
 ###################################################
 #                     LOAD                        #
 ###################################################
    write_data_dwh(PARAMS, CONFIG, DF_CONTENT)
    LOGGER.info('write_data_dwh inserted into final table')

    TIME.get_time()
    LOGGER.info('Process ended successfully.')
