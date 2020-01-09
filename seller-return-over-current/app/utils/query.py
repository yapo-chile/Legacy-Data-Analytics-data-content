from utils.read_params import ReadParams


class Query:
    """
    Class that store all querys
    """
    def seller_return_over_current(self, params: ReadParams) -> str:
        """
        Method return str with query
        """
        query = """
            select
                date '""" + params.get_date_from() + """"' dt_metric,
                (date '""" + params.get_date_from() + """"'
                    - interval '31' day)::date first_period_start,
                (date '""" + params.get_date_from() + """"'
                    - interval '2' day)::date first_period_end,
                (date '""" + params.get_date_from() + """"'
                    - interval '1' day)::date second_period,
                count(distinct seller_current_period)
                    sellers_current_period,
                count(distinct seller_past_period)
                    sellers_past_period,
                sum(case 
                    when seller_current_period is not null 
                        and seller_past_period is not null 
                            then 1 
                    else 0 end) sellers_both_periods
                from 
                (select 
                    distinct a.seller_id_fk seller_current_period
                from 
                    ods.ad a
                where 
                    a.approval_date::date = 
                    '""" + params.get_date_from() + """"'::date - integer '1'
                    and a.category_id_fk in (19,
                                                20,
                                                21,
                                                22,
                                                23,
                                                24,
                                                25,
                                                26,
                                                28,
                                                29,
                                                30,
                                                31,
                                                36,
                                                38,
                                                39,
                                                40,
                                                41,
                                                42,
                                                43,
                                                44,
                                                45,
                                                46,
                                                50,
                                                37)
                )z
                full join
                (select 
                    distinct a.seller_id_fk seller_past_period 
                from 
                    ods.ad a 
                where 
                a.approval_date::date between 
                '""" + params.get_date_from() + """"'::date - integer '31'
                and '""" + params.get_date_from() + """"'::date - integer '2'
                and a.category_id_fk in (19,
                                         20,
                                         21,
                                         22,
                                         23,
                                         24,
                                         25,
                                         26,
                                         28,
                                         29,
                                         30,
                                         31,
                                         36,
                                         38,
                                         39,
                                         40,
                                         41,
                                         42,
                                         43,
                                         44,
                                         45,
                                         46,
                                         50,
                                         37)
                )y 
                on z.seller_current_period=y.seller_past_period
        """
        return query

    def delete_data(self, params: ReadParams) -> str:
        """
        Method that returns events of the day
        """
        command = """
                delete from dm_peak.seller_return_over_current
                where
                dt_metric::date = '""" + params.get_date_from() + """'::date """
        return command
