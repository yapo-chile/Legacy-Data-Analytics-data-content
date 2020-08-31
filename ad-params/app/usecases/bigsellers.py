# pylint: disable=no-member
# utf-8
import logging
from infraestructure.psql import Database
from utils.query import AdParamsBigSellerQuery
from utils.read_params import ReadParams


class AdBigSellersParams(AdParamsBigSellerQuery):
    def __init__(self,
                 config,
                 params: ReadParams,
                 logger) -> None:
        self.config = config
        self.params = params
        self.logger = logger

    # Query data from data warehouse
    @property
    def blocket_data_big_seller(self):
        return self.__blocket_data_big_seller

    @blocket_data_big_seller.setter
    def blocket_data_big_seller(self, config):
        db_source = Database(conf=config)
        blocket_data_big_seller = db_source.select_to_dict(self.blocket_ad_params())
        db_source.close_connection()
        self.__blocket_data_big_seller = blocket_data_big_seller

    @property
    def dwh_data_big_seller(self):
        return self.__dwh_data_big_seller

    @dwh_data_big_seller.setter
    def dwh_data_big_seller(self, config):
        db_source = Database(conf=config)
        dwh_data_big_seller = db_source.select_to_dict(self.dwh_ad_params())
        db_source.close_connection()
        self.__dwh_data_big_seller = dwh_data_big_seller

    def insert_to_table(self):
        dwh = Database(conf=self.config.db)        
        dwh.insert_copy(self.cleaned_big_sellers, "stg", "big_sellers_detail")

    def generate(self):
        self.blocket_data_big_seller = self.config.blocket
        self.dwh_data_big_seller = self.config.db
        # Merging blocket and dwh dataset to check if they already
        # exists on final table
        merged_data = self.blocket_data_big_seller.\
            merge(self.dwh_data_big_seller.drop_duplicates(), on=['ad_id_nk'], 
                   how='left', indicator=True)
        merged_data = merged_data[merged_data['_merge'] == 'left_only']
        if merged_data.empty:
            self.logger.info("No new data to insert")
            return False
        merged_data = merged_data[merged_data.columns[~merged_data.columns.str.endswith('_y')]]
        merged_data.columns = merged_data.columns.str.replace(r'_x$', '')
        del merged_data['_merge']
        self.cleaned_big_sellers = merged_data
        for column in ["ad_id_nk",
                       "list_id"]:
            self.cleaned_big_sellers[column] = self.cleaned_big_sellers[column].astype('Int64')

        self.logger.info("First records as evidence")
        self.logger.info(self.cleaned_big_sellers.head())
        self.insert_to_table()
        self.logger.info("Ad big seller params succesfully saved")
        return True



