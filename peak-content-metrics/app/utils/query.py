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

    def query_base_postgresql_blocket(self) -> str:
        """
        Method return str with query
        """
        queryBlocket = """
                select '2'
            """
        return queryBlocket

    def query_base_pulse(self) -> str:
        """
        Method return str with query
        """
        queryAthena = """
        select '3'
        """
        return queryAthena

    def query_base_postgresql_dw(self) -> str:
        """
        Method return str with query
        """
        queryDW = """
                select '1'
            """
        return queryDW

    def get_naa_vertical_platform(self) -> str:
        """
        Method return str with query
        """
        queryDW = """
        select
            approval_date::date,
            vertical,
            platform_name platform,
            sum(new_ads) new_ads,
            sum(case when seller_pri_pro = 'PRI' then new_ads else 0 end) naa_pri,
            sum(case when seller_pri_pro = 'PRO' then new_ads else 0 end) naa_pro
        from
            (--aa
            select
                approval_date::date as approval_date,
                case
                    when category_id_fk in (47,48) then 'Real Estate'
                    when category_id_fk in (49) then 'Holiday Rental'
                    when category_id_fk in (7,8,9,10,11,12) then 'Motor'
                    when category_id_fk in (32) then 'Jobs'
                    when category_id_fk in (33,34,35) then 'Professional Services'
                    when category_id_fk in (28,29,30,31,37,38,39,50,40,41,42,43,19,20,21,22,23,24,25,26,36,44,45,46) then 'Consumer Goods'
                    else 'Undefined'
                end as vertical,
                case 
                    when pl.platform_name = 'Unknown' then 'Web'
                    when pl.platform_name = 'M Site' then 'MSite'
                    when pl.platform_name = 'NGA Android' then 'AndroidApp'
                    when pl.platform_name = 'NGA Ios' then 'iOSApp'
                    else pl.platform_name 
                end platform_name,
                case 
                    when spd.seller_id_fk is null then 'PRI' 
                    else 'PRO' 
                end seller_pri_pro,
                count(distinct(ad_id_pk)) as new_ads
            from
                ods.ad
                left join ods.seller_pro_details spd using(seller_id_fk, category_id_fk)
                left join ods.platform pl on pl.platform_id_pk=ad.platform_id_fk
            where 
                approval_date::date = '{0}'
                and action_type not in ('import')
            group by 1,2,3,4
            --QUERY DE BIG SELLERS A DW
            union all 
            select 
                bs.list_time::date as approval_date,
                case
                    when category_id_fk in (47,48) then 'Real Estate'
                    when category_id_fk in (49) then 'Holiday Rental'
                    when category_id_fk in (7,8,9,10,11,12) then 'Motor'
                    when category_id_fk in (32) then 'Jobs'
                    when category_id_fk in (33,34,35) then 'Professional Services'
                    when category_id_fk in (28,29,30,31,37,38,39,50,40,41,42,43,19,20,21,22,23,24,25,26,36,44,45,46) then 'Consumer Goods'
                    else 'Undefined'
                end as vertical,
                'Web' as platform_name,
                'PRO' seller_pri_pro,
                count(distinct(a.ad_id_pk)) as new_ads
            from
                ods.ad a
                left join ods.seller_pro_details spd using(seller_id_fk, category_id_fk)
                left join ods.platform pl on pl.platform_id_pk=a.platform_id_fk
                inner join stg.big_sellers_detail bs on bs.ad_id_nk::int = a.ad_id_nk
            where
                bs.list_time::date  = '{0}'
            group by 1,2,3,4
            )aa
        group by 1,2,3
        order by 2,1,3
        """.format(self.params.get_date_from())
        return queryDW

    def get_sellers_vertical_platform(self) -> str:
        """
        Method return str with query
        """
        queryDW = """
        select
            approval_date::date,
            vertical,
            platform_name platform,
            sum(sellers) sellers,
            sum(case when seller_pri_pro = 'PRI' then sellers else 0 end) sellers_pri,
            sum(case when seller_pri_pro = 'PRO' then sellers else 0 end) sellers_pro
        from
            (--aa
            select
                approval_date::date as approval_date,
                case
                    when category_id_fk in (47,48) then 'Real Estate'
                    when category_id_fk in (49) then 'Holiday Rental'
                    when category_id_fk in (7,8,9,10,11,12) then 'Motor'
                    when category_id_fk in (32) then 'Jobs'
                    when category_id_fk in (33,34,35) then 'Professional Services'
                    when category_id_fk in (28,29,30,31,37,38,39,50,40,41,42,43,19,20,21,22,23,24,25,26,36,44,45,46) then 'Consumer Goods'
                    else 'Undefined'
                end as vertical,
                case 
                    when pl.platform_name = 'Unknown' then 'Web'
                    when pl.platform_name = 'M Site' then 'MSite'
                    when pl.platform_name = 'NGA Android' then 'AndroidApp'
                    when pl.platform_name = 'NGA Ios' then 'iOSApp'
                    else pl.platform_name 
                end platform_name,
                case 
                    when spd.seller_id_fk is null then 'PRI' 
                    else 'PRO' 
                end seller_pri_pro,
                count(distinct(ad.seller_id_fk)) as sellers
            from
                ods.ad
                left join ods.seller_pro_details spd using(seller_id_fk, category_id_fk)
                left join ods.platform pl on pl.platform_id_pk=ad.platform_id_fk
            where
                approval_date::date  = '{0}'
                and action_type not in ('import')
            group by 1,2,3,4
            --QUERY DE BIG SELLERS A DW
            union all 
            select 
                bs.list_time::date as approval_date,
                case
                    when category_id_fk in (47,48) then 'Real Estate'
                    when category_id_fk in (49) then 'Holiday Rental'
                    when category_id_fk in (7,8,9,10,11,12) then 'Motor'
                    when category_id_fk in (32) then 'Jobs'
                    when category_id_fk in (33,34,35) then 'Professional Services'
                    when category_id_fk in (28,29,30,31,37,38,39,50,40,41,42,43,19,20,21,22,23,24,25,26,36,44,45,46) then 'Consumer Goods'
                    else 'Undefined'
                end as vertical,
                'Web' as platform_name,
                'PRO' seller_pri_pro,
                count(distinct(a.seller_id_fk)) as sellers
            from
                ods.ad a
                left join ods.seller_pro_details spd using(seller_id_fk, category_id_fk)
                left join ods.platform pl on pl.platform_id_pk=a.platform_id_fk
                inner join stg.big_sellers_detail bs on bs.ad_id_nk::int = a.ad_id_nk
            where
                bs.list_time::date  = '{0}'
            group by 1,2,3,4
            )aa
        group by 1,2,3
        order by 2,1,3
        """.format(self.params.get_date_from())
        return queryDW

    def get_sellers_platform_all_yapo(self) -> str:
        """
        Method return str with query
        """
        queryDW = """
        select
            approval_date::date,
            vertical,
            'All Yapo' platform,
            sum(sellers) sellers,
            sum(case when seller_pri_pro = 'PRI' then sellers else 0 end) sellers_pri,
            sum(case when seller_pri_pro = 'PRO' then sellers else 0 end) sellers_pro
        from
            (--aa
            select
                approval_date::date as approval_date,
                case
                    when category_id_fk in (47,48) then 'Real Estate'
                    when category_id_fk in (49) then 'Holiday Rental'
                    when category_id_fk in (7,8,9,10,11,12) then 'Motor'
                    when category_id_fk in (32) then 'Jobs'
                    when category_id_fk in (33,34,35) then 'Professional Services'
                    when category_id_fk in (28,29,30,31,37,38,39,50,40,41,42,43,19,20,21,22,23,24,25,26,36,44,45,46) then 'Consumer Goods'
                    else 'Undefined'
                end as vertical,
                case 
                    when spd.seller_id_fk is null then 'PRI' 
                    else 'PRO' 
                end seller_pri_pro,
                count(distinct(ad.seller_id_fk)) as sellers
            from
                ods.ad 
                left join ods.seller_pro_details spd using(seller_id_fk, category_id_fk)
                left join ods.platform pl on pl.platform_id_pk=ad.platform_id_fk
            where
                approval_date::date = '{0}'
                and action_type not in ('import')
            group by 1,2,3
            --QUERY DE BIG SELLERS A DW
            union all 
            select 
                bs.list_time::date as approval_date,
                case
                    when category_id_fk in (47,48) then 'Real Estate'
                    when category_id_fk in (49) then 'Holiday Rental'
                    when category_id_fk in (7,8,9,10,11,12) then 'Motor'
                    when category_id_fk in (32) then 'Jobs'
                    when category_id_fk in (33,34,35) then 'Professional Services'
                    when category_id_fk in (28,29,30,31,37,38,39,50,40,41,42,43,19,20,21,22,23,24,25,26,36,44,45,46) then 'Consumer Goods'
                else 'Undefined'
                end as vertical,
                'PRO' seller_pri_pro,
                count(distinct(a.seller_id_fk)) as sellers
            from
                ods.ad a
                left join ods.seller_pro_details spd using(seller_id_fk, category_id_fk)
                left join ods.platform pl on pl.platform_id_pk=a.platform_id_fk
                inner join stg.big_sellers_detail bs on bs.ad_id_nk::int = a.ad_id_nk
            where
                bs.list_time::date = '{0}'
            group by 1,2,3
            )aa
        group by 1,2,3
        order by 2,1,3
        """.format(self.params.get_date_from())
        return queryDW

    def get_sellers_vertical_all_yapo(self) -> str:
        """
        Method return str with query
        """
        queryDW = """
        select
            approval_date::date,
            'All Yapo' vertical,
            platform_name platform,
            sum(sellers) sellers,
            sum(case when seller_pri_pro = 'PRI' then sellers else 0 end) sellers_pri,
            sum(case when seller_pri_pro = 'PRO' then sellers else 0 end) sellers_pro
        from
            (--aa
            select
                approval_date::date as approval_date,
                case 
                    when pl.platform_name = 'Unknown' then 'Web'
                    when pl.platform_name = 'M Site' then 'MSite'
                    when pl.platform_name = 'NGA Android' then 'AndroidApp'
                    when pl.platform_name = 'NGA Ios' then 'iOSApp'
                    else pl.platform_name 
                end platform_name,
                case 
                    when spd.seller_id_fk is null then 'PRI' 
                    else 'PRO' 
                end seller_pri_pro,
                count(distinct(ad.seller_id_fk)) as sellers
            from
                ods.ad
                left join ods.seller_pro_details spd using(seller_id_fk, category_id_fk)
                left join ods.platform pl on pl.platform_id_pk=ad.platform_id_fk
            where
                approval_date::date = '{0}'
                and action_type not in ('import')
            group by 1,2,3
            --QUERY DE BIG SELLERS A DW
            union all 
            select 
                bs.list_time::date as approval_date,
                'Web' as platform_name,
                'PRO' seller_pri_pro,
                count(distinct(a.seller_id_fk)) as sellers
            from
                ods.ad a
                left join ods.seller_pro_details spd using(seller_id_fk, category_id_fk)
                left join ods.platform pl on pl.platform_id_pk=a.platform_id_fk
                inner join stg.big_sellers_detail bs on bs.ad_id_nk::int = a.ad_id_nk
            where
                bs.list_time::date = '{0}'
            group by 1,2,3
            )aa
        group by 1,2,3
        order by 2,1,3
        """.format(self.params.get_date_from())
        return queryDW

    def get_sellers_vertical_platform_all_yapo(self) -> str:
        """
        Method return str with query
        """
        queryDW = """
        select
            approval_date::date,
            'All Yapo' vertical,
            'All Yapo' platform,
            sum(sellers) sellers,
            sum(case when seller_pri_pro = 'PRI' then sellers else 0 end) sellers_pri,
            sum(case when seller_pri_pro = 'PRO' then sellers else 0 end) sellers_pro
        from
            (--aa
            select
                approval_date::date as approval_date,
                case 
                    when spd.seller_id_fk is null then 'PRI' 
                    else 'PRO' 
                end seller_pri_pro,
                count(distinct(ad.seller_id_fk)) as sellers
            from
                ods.ad
                left join ods.seller_pro_details spd using(seller_id_fk, category_id_fk)
                left join ods.platform pl on pl.platform_id_pk=ad.platform_id_fk
            where
                approval_date::date = '{0}'
                and action_type not in ('import')
            group by 1,2
            --QUERY DE BIG SELLERS A DW
            union all
            select 
                bs.list_time::date as approval_date,
                'PRO' seller_pri_pro,
                count(distinct(a.seller_id_fk)) as sellers
            from
                ods.ad a
                left join ods.seller_pro_details spd using(seller_id_fk, category_id_fk)
                left join ods.platform pl on pl.platform_id_pk=a.platform_id_fk
                inner join stg.big_sellers_detail bs on bs.ad_id_nk::int = a.ad_id_nk
            where
                bs.list_time::date = '{0}'
            group by 1,2
            )aa
        group by 1,2,3
        order by 2,1,3
        """.format(self.params.get_date_from())
        return queryDW

    def get_nia_vertical_platform(self) -> str:
        """
        Method return str with query
        """
        queryDW = """
        select
            creation_date::date,
            vertical,
            platform_name platform,
            sum(new_ads) new_inserted_ads,
            sum(case when seller_pri_pro = 'PRI' then new_ads else 0 end) nia_pri,
            sum(case when seller_pri_pro = 'PRO' then new_ads else 0 end) nia_pro
        from
            (--aa
            select
                creation_date::date as creation_date,
                case
                    when category_id_fk in (47,48) then 'Real Estate'
                    when category_id_fk in (49) then 'Holiday Rental'
                    when category_id_fk in (7,8,9,10,11,12) then 'Motor'
                    when category_id_fk in (32) then 'Jobs'
                    when category_id_fk in (33,34,35) then 'Professional Services'
                    when category_id_fk in (28,29,30,31,37,38,39,50,40,41,42,43,19,20,21,22,23,24,25,26,36,44,45,46) then 'Consumer Goods'
                    else 'Undefined'
                end as vertical,
                case 
                    when pl.platform_name = 'Unknown' then 'Web'
                    when pl.platform_name = 'M Site' then 'MSite'
                    when pl.platform_name = 'NGA Android' then 'AndroidApp'
                    when pl.platform_name = 'NGA Ios' then 'iOSApp'
                    else pl.platform_name 
                end platform_name,
                case 
                    when spd.seller_id_fk is null then 'PRI' 
                    else 'PRO' 
                end seller_pri_pro,
                count(distinct(ad_id_pk)) as new_ads
            from
                ods.ad
                left join ods.seller_pro_details spd using(seller_id_fk, category_id_fk)
                left join ods.platform pl on pl.platform_id_pk=ad.platform_id_fk
            where
                creation_date::date = '{0}'
                and action_type not in ('import')
            group by 1,2,3,4
            --QUERY DE BIG SELLERS A DW
            union all 
            select 
                bs.list_time::date as creation_date,
                case
                    when category_id_fk in (47,48) then 'Real Estate'
                    when category_id_fk in (49) then 'Holiday Rental'
                    when category_id_fk in (7,8,9,10,11,12) then 'Motor'
                    when category_id_fk in (32) then 'Jobs'
                    when category_id_fk in (33,34,35) then 'Professional Services'
                    when category_id_fk in (28,29,30,31,37,38,39,50,40,41,42,43,19,20,21,22,23,24,25,26,36,44,45,46) then 'Consumer Goods'
                    else 'Undefined'
                end as vertical,
                'Web' as platform_name,
                'PRO' seller_pri_pro,
                count(distinct(a.ad_id_pk)) as new_ads
            from
                ods.ad a
                left join ods.seller_pro_details spd using(seller_id_fk, category_id_fk)
                left join ods.platform pl on pl.platform_id_pk=a.platform_id_fk
                inner join stg.big_sellers_detail bs on bs.ad_id_nk::int = a.ad_id_nk
            where
                bs.list_time::date = '{0}'
            group by 1,2,3,4
            )aa
        group by 1,2,3
        order by 2,1,3
        """.format(self.params.get_date_from())
        return queryDW

    def delete_base(self) -> str:
        """
        Method that returns events of the day
        """
        command = """
                    delete from dm_peak.content where 
                    timedate::date = 
                    '""" + self.params.get_date_from() + """'::date """

        return command
