# pylint: disable=no-member
# utf-8
import logging
from infraestructure.psql import Database
from utils.query import AdReplyQuery
from utils.read_params import ReadParams

CHUNCKED_BLOCKS = 100

class AdReply(AdReplyQuery):
    def __init__(self,
                 config,
                 params: ReadParams,
                 logger) -> None:
        self.config = config
        self.params = params
        self.logger = logger

    # Query data from data blocket
    @property
    def blocket_data_reply(self):
        return self.__blocket_data_reply

    @blocket_data_reply.setter
    def blocket_data_reply(self, config):
        db_source = Database(conf=config)
        blocket_data_reply = db_source.select_to_dict(self.blocket_ad_reply())
        db_source.close_connection()
        self.__blocket_data_reply = blocket_data_reply

    # Query data from data warehouse
    @property
    def data_buyers(self):
        return self.__data_buyers

    @data_buyers.setter
    def data_buyers(self, config):
        db_source = Database(conf=config)
        data_buyers_ = db_source.select_to_dict(self.get_ad_reply_stg())
        data_buyers_ = data_buyers_[data_buyers_.buyer_id_pk_aux == 0]
        data_buyers_.drop(
            ['buyer_id_pk_aux'],
            axis=1,
            inplace=True)
        db_source.close_connection()
        self.__data_buyers = data_buyers_

    # Query data to get newly inserted data from ad reply warehouse
    @property
    def ods_data_reply(self):
        return self.__ods_data_reply

    @ods_data_reply.setter
    def ods_data_reply(self, config):
        db_source = Database(conf=config)
        ods_data_reply = db_source.select_to_dict(self.dwh_ad_reply_rank())
        db_source.close_connection()
        self.__ods_data_reply = ods_data_reply

    @property
    def dwh_stg_data_reply(self):
        return self.__dwh_stg_data_reply

    @dwh_stg_data_reply.setter
    def dwh_stg_data_reply(self, config):
        db_source = Database(conf=config)
        data_reply = db_source.select_to_dict(self.ods_stg_ad_reply_comparation())
        db_source.close_connection()
        self.__dwh_stg_data_reply = data_reply

    def insert_to_stg(self):
        cleaned_data = self.blocket_data_reply
        astypes = {"mail_queue_id": "Int64",
                   "list_id": "Int64",
                   "rule_id": "Int64",
                   "ad_id": "Int64"}
        cleaned_data = cleaned_data.astype(astypes)
        dwh = Database(conf=self.config.db)
        self.logger.info("First records as evidence to STG")
        self.logger.info(cleaned_data.head())
        dwh.execute_command(self.clean_stg_ad_reply())
        dwh.insert_copy(cleaned_data, "stg", "ad_reply")

    def update_rank(self):

        def set_rank(x, ods):
            if x['buyer_id_fk'] in ods['buyer_id_fk'].tolist():
                x['rank'] = ods[ods['buyer_id_fk'] == x['buyer_id_fk']]['rank'].max() + 1
            return x

        cleaned_data = self.ods_data_reply
        
        self.logger.info("First records as evidence of updating ranks")
        self.logger.info(cleaned_data.head())

        buyers = cleaned_data['buyer_id_fk'].astype("str").unique().tolist()
        dwh = Database(conf=self.config.db)
        chunked = round(len(buyers) / CHUNCKED_BLOCKS)

        self.logger.info("Ods ad reply data retrieved")
        self.logger.info("Calculating ranks")
        self.logger.info(("buyers to find {}".format(len(buyers))))
        for obj in range(chunked):
            if buyers:
                ods = dwh.select_to_dict(self.dwh_ad_reply_by_id_buyer(
                    buyers[:CHUNCKED_BLOCKS]  
                ))
                self.logger.info((len(ods)))
                del buyers[:CHUNCKED_BLOCKS]
                astypes = {"buyer_id_fk": "Int64",
                        "rank": "Int64"}
                ods = ods.astype(astypes)
                cleaned_data.apply(set_rank, ods=ods, axis=1)
                del ods
                self.logger.info("processsed items {}".format(obj + CHUNCKED_BLOCKS))
        
        self.logger.info("Ranks calculated")
        self.logger.info("Filling new ranks as 1")
        cleaned_data["rank"].fillna(1, inplace = True)
        cleaned_data = cleaned_data.astype(astypes)
        
        dwh.execute_command(self.clean_ods_ad_reply_ranks(
            cleaned_data['ad_reply_id_pk'].astype("str").unique().tolist()))
        dwh.insert_copy(cleaned_data, "ods", "ad_reply")


    def insert_buyers_to_ods(self) -> None:
        db = Database(conf=self.config.db)
        db.insert_copy(
            df=self.data_buyers,
            schema='ods',
            table='buyer'
        )
        db.close_connection()

    def insert_to_ods(self):
        cleaned_data = self.dwh_stg_data_reply
        cleaned_data = cleaned_data[["buyer_id_fk",
                                     "ad_reply_id_nk",
                                     "ad_reply_creation_date",
                                     "email",
                                     "ad_id_fk",
                                     "insert_date"]]
        astypes = {"buyer_id_fk": "Int64",
                   "ad_reply_id_nk": "Int64",
                   "ad_id_fk": "Int64"}
        cleaned_data = cleaned_data.astype(astypes)
        dwh = Database(conf=self.config.db)
        dwh.execute_command(self.clean_ods_ad_reply())
        self.logger.info("First records as evidence to ODS")
        self.logger.info(cleaned_data.head())
        dwh.insert_copy(cleaned_data, "ods", "ad_reply")
        self.ods_data = cleaned_data


    def generate(self):
        self.blocket_data_reply = self.config.blocket
        print(self.blocket_data_reply.head())
        self.insert_to_stg()
        # Reading stg to perform ods buyers data
        self.logger.info('Starting ods_buyer step')
        #self.data_buyers = self.config.db
        #self.insert_buyers_to_ods()
        self.logger.info('Ending ods_buyer step')
        # Reading stg with ods all togeter
        self.dwh_stg_data_reply = self.config.db
        self.insert_to_ods()
        # Update rank values in ods
        self.ods_data_reply = self.config.db
        self.update_rank()
        self.logger.info("Ad Reply succesfully saved")
        return True
