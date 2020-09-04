# pylint: disable=no-member
# utf-8
import logging
from infraestructure.psql import Database
from utils.query import RevParamsQuery
from utils.read_params import ReadParams


class RevParams(RevParamsQuery):
    def __init__(self,
                 config,
                 params: ReadParams,
                 logger) -> None:
        self.config = config
        self.params = params
        self.logger = logger

    # Query data from data warehouse
    @property
    def blocket_data_rev_params(self):
        return self.__blocket_data_rev_params

    @blocket_data_rev_params.setter
    def blocket_data_rev_params(self, config):
        db_source = Database(conf=config)
        blocket_data_rev_params = db_source.select_to_dict(self.blocket_rev_params())
        db_source.close_connection()
        self.__blocket_data_rev_params = blocket_data_rev_params

    def insert_to_table(self):
        dwh = Database(conf=self.config.db)   
        dwh.execute_command(self.clean_rev_params())
        dwh.insert_copy(self.cleaned_data, "stg", "review_params")

    def generate(self):
        self.blocket_data_rev_params = self.config.blocket
        self.cleaned_data = self.blocket_data_rev_params
        self.cleaned_data["ad_id"] = self.cleaned_data["ad_id"].astype('Int64')
        self.logger.info("First records as evidence")
        self.logger.info(self.cleaned_data.head())
        self.insert_to_table()
        self.logger.info("Rev params succesfully saved")
        return True

            


