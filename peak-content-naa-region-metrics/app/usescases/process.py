# pylint: disable=no-member
# utf-8
import logging
from infraestructure.psql import Database
from utils.query import Query
from utils.read_params import ReadParams


class Process():
    def __init__(self,
                 config,
                 params: ReadParams) -> None:
        self.config = config
        self.params = params

    # Query data from data warehouse
    @property
    def data_naa_vertical_platform(self):
        return self.__data_naa_vertical_platform

    @data_naa_vertical_platform.setter
    def data_naa_vertical_platform(self, config):
        query = Query(config, self.params)
        db_source = Database(conf=config)
        data_naa_vertical_platform = db_source.select_to_dict(query \
                                            .get_naa_vertical_platform())
        db_source.close_connection()

        # Creating Platform All Yapo
        platform_all_yapo = data_naa_vertical_platform \
            .groupby(['approval_date',
                      'vertical',
                      'region_name']).agg({'new_ads':'sum',
                                           'naa_pri':'sum',
                                           'naa_pro':'sum'}).reset_index()
        platform_all_yapo['platform'] = 'All Yapo'
        platform_all_yapo = platform_all_yapo[['approval_date',
                                               'vertical',
                                               'platform',
                                               'region_name',
                                               'new_ads',
                                               'naa_pri',
                                               'naa_pro']]

        # Creating Vertical All Yapo
        vertical_all_yapo = data_naa_vertical_platform \
            .groupby(['approval_date',
                      'platform',
                      'region_name']).agg({'new_ads':'sum',
                                           'naa_pri':'sum',
                                           'naa_pro':'sum'}).reset_index()
        vertical_all_yapo['vertical'] = 'All Yapo'
        vertical_all_yapo = vertical_all_yapo[['approval_date',
                                               'vertical',
                                               'platform',
                                               'region_name',
                                               'new_ads',
                                               'naa_pri',
                                               'naa_pro']]

        # Creating Region All Yapo
        region_all_yapo = data_naa_vertical_platform \
            .groupby(['approval_date',
                      'platform',
                      'vertical']).agg({'new_ads':'sum',
                                        'naa_pri':'sum',
                                        'naa_pro':'sum'}).reset_index()
        region_all_yapo['region_name'] = 'All Yapo'
        region_all_yapo = region_all_yapo[['approval_date',
                                           'vertical',
                                           'platform',
                                           'region_name',
                                           'new_ads',
                                           'naa_pri',
                                           'naa_pro']]

        # Creating Vertical and Platform All Yapo
        platform_vertical_all_yapo = data_naa_vertical_platform \
            .groupby(['approval_date',
                      'region_name']).agg({'new_ads':'sum',
                                           'naa_pri':'sum',
                                           'naa_pro':'sum'}).reset_index()
        platform_vertical_all_yapo['vertical'] = 'All Yapo'
        platform_vertical_all_yapo['platform'] = 'All Yapo'
        platform_vertical_all_yapo = platform_vertical_all_yapo \
            [['approval_date', 'vertical', 'platform', 'region_name',
              'new_ads', 'naa_pri', 'naa_pro']]

        # Creating Vertical and Region All Yapo
        region_vertical_all_yapo = data_naa_vertical_platform \
            .groupby(['approval_date',
                      'platform']).agg({'new_ads':'sum',
                                        'naa_pri':'sum',
                                        'naa_pro':'sum'}).reset_index()
        region_vertical_all_yapo['vertical'] = 'All Yapo'
        region_vertical_all_yapo['region_name'] = 'All Yapo'
        region_vertical_all_yapo = region_vertical_all_yapo[['approval_date',
                                                             'vertical',
                                                             'platform',
                                                             'region_name',
                                                             'new_ads',
                                                             'naa_pri',
                                                             'naa_pro']]

        # Creating Platform and Region All Yapo
        region_platform_all_yapo = data_naa_vertical_platform \
            .groupby(['approval_date',
                      'vertical']).agg({'new_ads':'sum',
                                        'naa_pri':'sum',
                                        'naa_pro':'sum'}).reset_index()
        region_platform_all_yapo['platform'] = 'All Yapo'
        region_platform_all_yapo['region_name'] = 'All Yapo'
        region_platform_all_yapo = region_platform_all_yapo[['approval_date',
                                                             'vertical',
                                                             'platform',
                                                             'region_name',
                                                             'new_ads',
                                                             'naa_pri',
                                                             'naa_pro']]

        # Creating Platform, Vertical and Region All Yapo
        region_platform_vertical_all_yapo = data_naa_vertical_platform \
            .groupby(['approval_date']).agg({'new_ads':'sum',
                                             'naa_pri':'sum',
                                             'naa_pro':'sum'}).reset_index()
        region_platform_vertical_all_yapo['platform'] = 'All Yapo'
        region_platform_vertical_all_yapo['vertical'] = 'All Yapo'
        region_platform_vertical_all_yapo['region_name'] = 'All Yapo'
        region_platform_vertical_all_yapo = \
            region_platform_vertical_all_yapo[['approval_date',
                                               'vertical',
                                               'platform',
                                               'region_name',
                                               'new_ads',
                                               'naa_pri',
                                               'naa_pro']]
        # Appending new rows to main df
        output_df = data_naa_vertical_platform \
            .append(vertical_all_yapo,
                    ignore_index=True,
                    sort=False) \
                        .append(platform_all_yapo,
                                ignore_index=True,
                                sort=False) \
                        .append(region_all_yapo,
                                ignore_index=True,
                                sort=False) \
                        .append(platform_vertical_all_yapo,
                                ignore_index=True,
                                sort=False) \
                        .append(region_vertical_all_yapo,
                                ignore_index=True,
                                sort=False) \
                        .append(region_platform_all_yapo,
                                ignore_index=True,
                                sort=False) \
                        .append(region_platform_vertical_all_yapo,
                                ignore_index=True,
                                sort=False) \
                        .sort_values(['approval_date',
                                      'platform',
                                      'vertical',
                                      'region_name']) \
                        .reset_index(drop=True)

        self.__data_naa_vertical_platform = output_df

    # Write data to data warehouse
    def save(self) -> None:
        query = Query(self.config, self.params)
        db = Database(conf=self.config.db)
        db.execute_command(query.delete_output_dw_table())
        for row in self.data_naa_vertical_platform.itertuples():
            data_row = [(row.approval_date, row.vertical,
                         row.platform, row.region_name,
                         row.new_ads, row.naa_pri, row.naa_pro)]
            db.insert_data(query.insert_output_to_dw(), data_row)
        logging.info('INSERT dm_analysis.temp_content_w_region COMMIT.')
        db.close_connection()

    def generate(self):
        self.data_naa_vertical_platform = self.config.db
        self.save()
