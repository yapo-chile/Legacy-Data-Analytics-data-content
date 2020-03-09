from infraestructure.conf import getConf
from utils.read_params import ReadParams


class Query:
    """
    Class that store all querys
    """
    def __init__(self,
                 conf: getConf,
                 params: ReadParams) -> None:
        self.params = params
        self.conf = conf

    def query_leads_and_uniq_leads(self) -> str:
        """
        Method return str with query
        """
        query = """
            select
                a.timedate,
                a.unique_leads::int,
                b.leads::int
            from
                (--a
                select
                    timedate::date,
                    sum(unique_leads)::int unique_leads
                from
                    dm_pulse.unique_leads
                where
                    platform = 'All Yapo'
                    and traffic_channel = 'All Yapo'
                    and vertical = 'Consumer Goods'
                    and main_category = 'All'
                    and timedate::date = '{0}'
                group by 1
                order by 1
                ) a
            left join
                (--b
                select
                    fecha::date timedate, 
                    sum(leads)::int leads
                from
                    ods.leads_daily ld
                left join
                    ods.category c
                on
                    ld.category_name = c.category_name and c.date_to >= current_timestamp
                where
                    c.category_id_nk::int in (3020,
                                              3040,
                                              3060,
                                              3080,
                                              4020,
                                              4040,
                                              4060,
                                              4080,
                                              5020,
                                              5040,
                                              5060,
                                              5160,
                                              6020,
                                              6060,
                                              6080,
                                              6100,
                                              6120,
                                              6140,
                                              6160,
                                              6180,
                                              8020,
                                              9020,
                                              9040,
                                              9060)
                    and fecha::date = '{0}'
                group by 1
                order by 1
                ) b
            using (timedate)
        """.format(self.params.get_date_from())
        return query

    def delete_leads_and_uniq_leads(self) -> str:
        """
        Method that returns events of the day
        """
        command = """
                    delete from """ + self.conf.db.table + """ where
                    timedate::date = 
                    '""" + self.params.get_date_from() + """'::date """

        return command
