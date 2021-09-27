# pylint: disable=no-member
# utf-8
from infraestructure.psql import Database
from utils.query import TableauInsertingFeeQuery
from utils.read_params import ReadParams


class TableauInsertingFee(TableauInsertingFeeQuery):
    def __init__(self,
                 config,
                 params: ReadParams,
                 logger) -> None:
        self.config = config
        self.params = params
        self.logger = logger

    # Query data from data warehouse
    @property
    def dwh_inserting_fee(self):
        return self.__dwh_inserting_fee

    @dwh_inserting_fee.setter
    def dwh_inserting_fee(self, config):
        db_source = Database(conf=config)
        dwh_inserting_fee = db_source.select_to_dict(self.select_inferting_fee())
        db_source.close_connection()
        self.__dwh_inserting_fee = dwh_inserting_fee

    def insert_to_table(self):
        dwh = Database(conf=self.config.db)        
        dwh.insert_copy(self.cleaned_data, "dm_analysis", "temp_tableau_inserting_fee")

    def generate(self):
        self.dwh_inserting_fee = self.config.db
        self.cleaned_data = self.dwh_inserting_fee
        for column in ["month_id",
                       "qty_ads",
                       "qty_sellers",
                       "qty_fee",
                       "price_fee",
                       "qty_sellers_fee",
                       "ads_fee"]:
            self.cleaned_data[column] = self.cleaned_data[column].astype('Int64')

        self.logger.info("First records as evidence")
        self.logger.info(self.cleaned_data.head())
        self.insert_to_table()
        self.logger.info("Ad big seller params succesfully saved")
        return True



