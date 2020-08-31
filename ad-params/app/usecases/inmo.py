# pylint: disable=no-member
# utf-8
import logging
from infraestructure.psql import Database
from utils.query import AdParamsInmoQuery
from utils.read_params import ReadParams


class AdInmoParams(AdParamsInmoQuery):
    def __init__(self,
                 config,
                 params: ReadParams,
                 logger) -> None:
        self.config = config
        self.params = params
        self.logger = logger

    # Query data from data warehouse
    @property
    def blocket_data_inmo(self):
        return self.__blocket_data_inmo

    @blocket_data_inmo.setter
    def blocket_data_inmo(self, config):
        db_source = Database(conf=config)
        blocket_data_inmo = db_source.select_to_dict(self.blocket_ad_params())
        db_source.close_connection()
        self.__blocket_data_inmo = blocket_data_inmo

    @property
    def dwh_data_inmo(self):
        return self.__dwh_data_inmo

    @dwh_data_inmo.setter
    def dwh_data_inmo(self, config):
        db_source = Database(conf=config)
        dwh_data_inmo = db_source.select_to_dict(self.dwh_ad_params())
        db_source.close_connection()
        self.__dwh_data_inmo = dwh_data_inmo

    def insert_to_table(self):
        dwh = Database(conf=self.config.db)        
        dwh.insert_copy(self.cleaned_inmo, "ods", "ads_inmo_params")

    def generate(self):
        self.blocket_data_inmo = self.config.blocket
        self.dwh_data_inmo = self.config.db
        # Merging blocket and dwh dataset to check if they already
        # exists on final table
        merged_data = self.blocket_data_inmo.\
            merge(self.dwh_data_inmo.drop_duplicates(), on=['ad_id_nk'], 
                   how='left', indicator=True)
        merged_data = merged_data[merged_data['_merge'] == 'left_only']
        if merged_data.empty:
            self.logger.info("No new data to insert")
            return False
        merged_data = merged_data[merged_data.columns[~merged_data.columns.str.endswith('_y')]]
        merged_data.columns = merged_data.columns.str.replace(r'_x$', '')
        del merged_data['_merge']
        self.cleaned_inmo = merged_data
        for column in ["ad_id_nk",
                        "bathrooms",
                        "rooms",
                        "meters",
                        "estate_type",
                        "new_realestate"]:
            self.cleaned_inmo[column] = self.cleaned_inmo[column].astype('Int64')

        self.logger.info("First records as evidence")
        self.logger.info(self.cleaned_inmo.head())
        self.insert_to_table()
        self.logger.info("Ad inmo params succesfully saved")
        return True

            


