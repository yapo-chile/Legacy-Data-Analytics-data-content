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

    def get_traffic_xiti(self) -> str:
        """
        Method return str with query
        """
        queryDW = """
        select 
            time_date::date timedate, 
            case when category_id_nk in (1260) then 'Holiday Rental'
                 when category_id_nk in (7020) then 'Jobs'
                 when category_id_nk in (7040,7060,7080) then 'Professional Services'
                 when category_id_nk in (1220,1240) then 'Real Estate'
            end as vertical,
            case when site = 535162 then 'Web'
                 when site = 535499 then 'MSite'
                 when site = 557231 then 'AndroidApp'
                 when site = 557229 then 'iOSApp'
            end platform,
            sum(visitors)::int dau_xiti,
            sum(visits)::int visits_xiti
        from 
            stg.fact_day_category_xiti f
            left join
            stg.dim_category_xiti_correlation cxc on f.main_category = cxc.main_category_xiti and f.category = cxc.category_xiti
            left join
            stg.dim_sites_xiti ds on ds.site_id = f.site
        where 
            site in (535162,535499,557229,557231) and category_id_nk in (1220,1240,1260,7020,7040,7060,7080)
            and time_date::date = '{0}'
        group by 1,2,3
        union all
        select 
            time_date::date timedate, 
            case when category_id_nk in (1260) then 'Holiday Rental'
                 when category_id_nk in (7020) then 'Jobs'
                 when category_id_nk in (7040,7060,7080) then 'Professional Services'
                 when category_id_nk in (1220,1240) then 'Real Estate'
            end as vertical,
            'All Yapo' platform,
            sum(visitors)::int dau_xiti,
            sum(visits)::int visits_xiti
        from 
            stg.fact_day_category_xiti f
            left join
            stg.dim_category_xiti_correlation cxc on f.main_category = cxc.main_category_xiti and f.category = cxc.category_xiti
            left join
            stg.dim_sites_xiti ds on ds.site_id = f.site
        where 
            site in (535162,535499,557229,557231) and category_id_nk in (1220,1240,1260,7020,7040,7060,7080)
            and time_date::date = '{0}'
        group by 1,2,3
        union all
        select 
            time_date::date timedate,
            case when main_category in ('Vehículos','Vehicles') then 'Motor'
                 when main_category in ('Computadores & electrónica','Computers & electronics','Computers and electronics','Fashion, footwear, beauty & health', 'Moda, belleza y salud','Moda, calzado, Belleza y','Moda, calzado, belleza y salud','Futura mamá, bebés y niños','Pregnancy, babies & children','Hogar','Home & personal items','Home and personal items','Leisure sports and hobby','Leisure, sports & hobby','Tiempo libre','others','Otros') then 'Consumer Goods'
            end vertical,
            case when site = 535162 then 'Web'
                 when site = 535499 then 'MSite'
                 when site = 557231 then 'AndroidApp'
                 when site = 557229 then 'iOSApp'
            end platform,
            sum(visitors)::int dau_xiti,
            sum(visits)::int visits_xiti
        from 
            stg.fact_day_category_main_xiti
        where 
            site in (535162,535499,557231,557229)
            and main_category in ('Vehículos','Vehicles','Computadores & electrónica','Computers & electronics','Computers and electronics','Fashion, footwear, beauty & health', 'Moda, belleza y salud','Moda, calzado, Belleza y','Moda, calzado, belleza y salud','Futura mamá, bebés y niños','Pregnancy, babies & children','Hogar','Home & personal items','Home and personal items','Leisure sports and hobby','Leisure, sports & hobby','Tiempo libre','others','Otros')
            and time_date::date = '{0}'
        group by 1,2,3
        union all
        select 
            time_date::date timedate,
            case when main_category in ('Vehículos','Vehicles') then 'Motor'
                 when main_category in ('Computadores & electrónica','Computers & electronics','Computers and electronics','Fashion, footwear, beauty & health', 'Moda, belleza y salud','Moda, calzado, Belleza y','Moda, calzado, belleza y salud','Futura mamá, bebés y niños','Pregnancy, babies & children','Hogar','Home & personal items','Home and personal items','Leisure sports and hobby','Leisure, sports & hobby','Tiempo libre','others','Otros') then 'Consumer Goods'
            end vertical,
            'All Yapo' platform,
            sum(visitors)::int dau_xiti,
            sum(visits)::int visits_xiti
        from 
            stg.fact_day_category_main_xiti
        where 
            site in (535162,535499,557231,557229)
            and main_category in ('Vehículos','Vehicles','Computadores & electrónica','Computers & electronics','Computers and electronics','Fashion, footwear, beauty & health', 'Moda, belleza y salud','Moda, calzado, Belleza y','Moda, calzado, belleza y salud','Futura mamá, bebés y niños','Pregnancy, babies & children','Hogar','Home & personal items','Home and personal items','Leisure sports and hobby','Leisure, sports & hobby','Tiempo libre','others','Otros')
            and time_date::date = '{0}'
        group by 1,2,3
        union all
        select 
            fecha::date timedate, 
            'All Yapo' vertical,
            case when site = 535162 then 'Web'
                 when site = 535499 then 'MSite'
                 when site = 557231 then 'AndroidApp'
                 when site = 557229 then 'iOSApp'
            end platform,
            sum(visitors)::int dau_xiti,
            sum(visits)::int visits_xiti
        from
            ods.fact_day_xiti
        where
            site in (535162,535499,557231,557229)
            and fecha::date = '{0}'
        group by 1,2,3
        union all
        select 
            fecha::date timedate, 
            'All Yapo' vertical,
            'All Yapo' platform,
            sum(visitors)::int dau_xiti,
            sum(visits)::int visits_xiti
        from
            ods.fact_day_xiti
        where
            site in (535162,535499,557231,557229)
            and fecha::date = '{0}'
        group by 1,2,3
        order by 1,2,3
        """.format(self.params.get_date_from())
        return queryDW

    def get_leads_xiti(self) -> str:
        """
        Method return str with query
        """
        queryDW = """
        select
            fecha::date timedate,
            case when c.category_id_nk in (2020,2040,2060,2080,2100,2120) then 'Motor'
                 when c.category_id_nk in (1260) then 'Holiday Rental'
                 when c.category_id_nk in (7020) then 'Jobs'
                 when c.category_id_nk in (3020,3040,3060,3080,4020,4040,4060,4080,5020,5040,5060,5160,6020,6060,6080,6100,6120,6140,6160,6180,8020,9020,9040,9060) then 'Consumer Goods'
                 when c.category_id_nk in (7040,7060,7080) then 'Professional Services'
                 when c.category_id_nk in (1220,1240) then 'Real Estate'
            end as vertical,
            case when a.site = 'Desktop v2' then 'Web'
                 when a.site = 'Mobile v2' then 'MSite'
                 when a.site = 'NGA Android App' then 'AndroidApp'
                 when a.site = 'NGA Ios App' then 'iOSApp'
            end platform,
            sum(leads)::int leads_xiti
        from
            ods.leads_daily a
            left join
            ods.category c on a.category_name = c.category_name and c.date_to >= current_timestamp
        where
            fecha::date = '{0}'
            and c.category_id_nk in (1220,1240,1260,2020,2040,2060,2080,2100,2120,3020,3040,3060,3080,4020,4040,4060,4080,5020,5040,5060,5160,6020,6060,6080,6100,6120,6140,6160,6180,7020,7040,7060,7080,8020,9020,9040,9060)
        group by 1,2,3
        union all
        select
            fecha::date timedate,
            case when c.category_id_nk in (2020,2040,2060,2080,2100,2120) then 'Motor'
                 when c.category_id_nk in (1260) then 'Holiday Rental'
                 when c.category_id_nk in (7020) then 'Jobs'
                 when c.category_id_nk in (3020,3040,3060,3080,4020,4040,4060,4080,5020,5040,5060,5160,6020,6060,6080,6100,6120,6140,6160,6180,8020,9020,9040,9060) then 'Consumer Goods'
                 when c.category_id_nk in (7040,7060,7080) then 'Professional Services'
                 when c.category_id_nk in (1220,1240) then 'Real Estate'
            end as vertical,
            'All Yapo' platform,
            sum(leads)::int leads_xiti
        from
            ods.leads_daily a
            left join
            ods.category c on a.category_name = c.category_name and c.date_to >= current_timestamp
        where
            fecha::date = '{0}'
            and c.category_id_nk in (1220,1240,1260,2020,2040,2060,2080,2100,2120,3020,3040,3060,3080,4020,4040,4060,4080,5020,5040,5060,5160,6020,6060,6080,6100,6120,6140,6160,6180,7020,7040,7060,7080,8020,9020,9040,9060)
        group by 1,2,3
        union all
        select
            fecha::date timedate,
            'All Yapo' vertical,
            case when a.site = 'Desktop v2' then 'Web'
                 when a.site = 'Mobile v2' then 'MSite'
                 when a.site = 'NGA Android App' then 'AndroidApp'
                 when a.site = 'NGA Ios App' then 'iOSApp'
            end platform,
            sum(leads)::int leads_xiti
        from
            ods.leads_daily a
        left join
        ods.category c on a.category_name = c.category_name and c.date_to >= current_timestamp
        where
            fecha::date = '{0}'
        group by 1,2,3
        union all
        select
            fecha::date timedate,
            'All Yapo' vertical,
            'All Yapo' platform,
            sum(leads)::int leads_xiti
        from
            ods.leads_daily a
        left join
            ods.category c on a.category_name = c.category_name and c.date_to >= current_timestamp
        where
            fecha::date = '{0}'
        group by 1,2,3
        order by 1,2,3
        """.format(self.params.get_date_from())
        return queryDW

    def get_traffic_pulse(self) -> str:
        """
        Method return str with query
        """
        queryDW = """
        select
            timedate::date,
            vertical,
            platform,
            sum(active_users)::int dau_pulse,
            sum(sessions)::int visits_pulse,
            sum(active_users_that_do_adviews)::int browsers_pulse,
            sum(active_users_that_do_leads)::int buyers_pulse
        from
            dm_pulse.traffic_metrics
        where
            traffic_channel = 'All Yapo' and main_category = 'All'
            and vertical != 'Undefined' and platform != ''
            and timedate::date = '{0}'
        group by 1,2,3
        order by 1,2,3
        """.format(self.params.get_date_from())
        return queryDW

    def get_unique_leads_pulse(self) -> str:
        """
        Method return str with query
        """
        queryDW = """
        select
            timedate::date,
            vertical,
            platform,
            sum(unique_leads)::int unique_leads_pulse
        from
            dm_pulse.unique_leads
        where
            traffic_channel = 'All Yapo' and main_category = 'All'
            and vertical != 'Undefined' and platform != ''
            and timedate::date = '{0}'
        group by 1,2,3
        order by 1,2,3
        """.format(self.params.get_date_from())
        return queryDW

    def get_leads_per_user(self) -> str:
        """
        Method return str with query
        """
        queryAthena = """
        select
            timedate, 
            vertical,
            platform,
            avg(leads) leads_per_user,
            avg(ads_with_lead) ads_with_lead_per_user
        from
            (
            select 
                cast(date_parse(cast(year as varchar) || '-' || cast(month as varchar) || '-' || cast(day as varchar),'%Y-%c-%e') as date) timedate,
                'All Yapo' vertical,
                'All Yapo' platform,
                environment_id,
                count(distinct event_id) leads,
                count(distinct ad_id) ads_with_lead
            from
                yapocl_databox.insights_events_behavioral_fact_layer_365d
            where
                event_type in ('Call','SMS','Send')
                and cast(date_parse(cast(year as varchar) || '-' || cast(month as varchar) || '-' || cast(day as varchar),'%Y-%c-%e') as date) = date '{0}'
            group by 1,2,3,4
            union all
            select 
                cast(date_parse(cast(year as varchar) || '-' || cast(month as varchar) || '-' || cast(day as varchar),'%Y-%c-%e') as date) timedate,
                'All Yapo' vertical,
                case when (device_type = 'desktop' and (object_url like '%www2.yapo.cl%' or object_url like '%//yapo.cl%')) or product_type = 'Web' or object_url like '%www.yapo.cl%' or (device_type = 'desktop' and product_type = 'unknown') then 'Web'
                     when ((device_type = 'mobile' or device_type = 'tablet') and (object_url like '%www2.yapo.cl%' or object_url like '%//yapo.cl%')) or product_type = 'M-Site' or object_url like '%m.yapo.cl%' or ((device_type = 'mobile' or device_type = 'tablet') and product_type = 'unknown') then 'MSite'
                     when ((device_type = 'mobile' or device_type = 'tablet') and object_url is not null and product_type = 'AndroidApp') or product_type = 'AndroidApp' then 'AndroidApp'
                     when ((device_type = 'mobile' or device_type = 'tablet') and object_url is not null and product_type = 'iOSApp') or product_type = 'iOSApp' or product_type = 'iPadApp' then 'iOSApp'
                end platform,
                environment_id,
                count(distinct event_id) leads,
                count(distinct ad_id) ads_with_lead
            from
                yapocl_databox.insights_events_behavioral_fact_layer_365d
            where
                event_type in ('Call','SMS','Send')
                and cast(date_parse(cast(year as varchar) || '-' || cast(month as varchar) || '-' || cast(day as varchar),'%Y-%c-%e') as date) = date '{0}'
            group by 1,2,3,4
            union all
            select 
                cast(date_parse(cast(year as varchar) || '-' || cast(month as varchar) || '-' || cast(day as varchar),'%Y-%c-%e') as date) timedate,
                case when local_main_category in ('vehiculos','vehículos','veh�culos') then 'Motor' 
                     when local_category_level1 in ('arriendo de temporada') and local_main_category in ('inmuebles') then 'Holiday Rental' 
                     when local_category_level1 in ('ofertas de empleo') and local_main_category in ('servicios','servicios negocios y empleo','servicios, negocios y empleo','servicios,negocios y empleo') then 'Jobs'
                     when local_main_category in ('computadores & electrónica','computadore & electronica','computadores & electr�nica','computadores y electronica','futura mamá','futura mam�','futura mama bebes y ninos','futura mamá bebés y niños','futura mamá, bebés y niños','futura mamá,bebés y niños','futura mam� beb�s y ni�os','futura mam�,beb�s y ni�os','hogar','moda','moda calzado belleza y salud','moda, calzado, belleza y salud','moda,calzado,belleza y salud','otros','otros productos','other','tiempo libre') then 'Consumer Goods'
                     when local_category_level1 in ('busco empleo','servicios','negocios,maquinaria y construccion','negocios maquinaria y construccion','negocios maquinaria y construcción','negocios maquinaria y construcci髇','negocios maquinaria y construcci�n','negocios, maquinaria y construcción') and local_main_category in ('servicios','servicios negocios y empleo','servicios, negocios y empleo','servicios,negocios y empleo') then 'Professional Services'
                     when local_category_level1 in ('arrendar','arriendo','comprar') and local_main_category in ('inmuebles') then 'Real Estate'
                     when local_main_category in ('unknown','undefined') then 'Undefined'
                     else 'Undefined'
                end vertical,
                case when (device_type = 'desktop' and (object_url like '%www2.yapo.cl%' or object_url like '%//yapo.cl%')) or product_type = 'Web' or object_url like '%www.yapo.cl%' or (device_type = 'desktop' and product_type = 'unknown') then 'Web'
                     when ((device_type = 'mobile' or device_type = 'tablet') and (object_url like '%www2.yapo.cl%' or object_url like '%//yapo.cl%')) or product_type = 'M-Site' or object_url like '%m.yapo.cl%' or ((device_type = 'mobile' or device_type = 'tablet') and product_type = 'unknown') then 'MSite'
                     when ((device_type = 'mobile' or device_type = 'tablet') and object_url is not null and product_type = 'AndroidApp') or product_type = 'AndroidApp' then 'AndroidApp'
                     when ((device_type = 'mobile' or device_type = 'tablet') and object_url is not null and product_type = 'iOSApp') or product_type = 'iOSApp' or product_type = 'iPadApp' then 'iOSApp'
                end platform,
                environment_id,
                count(distinct event_id) leads,
                count(distinct ad_id) ads_with_lead
            from
                yapocl_databox.insights_events_behavioral_fact_layer_365d
            where
                event_type in ('Call','SMS','Send')
                and cast(date_parse(cast(year as varchar) || '-' || cast(month as varchar) || '-' || cast(day as varchar),'%Y-%c-%e') as date) = date '{0}'
            group by 1,2,3,4
            union all
            select 
                cast(date_parse(cast(year as varchar) || '-' || cast(month as varchar) || '-' || cast(day as varchar),'%Y-%c-%e') as date) timedate,
                case when local_main_category in ('vehiculos','vehículos','veh�culos') then 'Motor' 
                     when local_category_level1 in ('arriendo de temporada') and local_main_category in ('inmuebles') then 'Holiday Rental' 
                     when local_category_level1 in ('ofertas de empleo') and local_main_category in ('servicios','servicios negocios y empleo','servicios, negocios y empleo','servicios,negocios y empleo') then 'Jobs'
                     when local_main_category in ('computadores & electrónica','computadore & electronica','computadores & electr�nica','computadores y electronica','futura mamá','futura mam�','futura mama bebes y ninos','futura mamá bebés y niños','futura mamá, bebés y niños','futura mamá,bebés y niños','futura mam� beb�s y ni�os','futura mam�,beb�s y ni�os','hogar','moda','moda calzado belleza y salud','moda, calzado, belleza y salud','moda,calzado,belleza y salud','otros','otros productos','other','tiempo libre') then 'Consumer Goods'
                     when local_category_level1 in ('busco empleo','servicios','negocios,maquinaria y construccion','negocios maquinaria y construccion','negocios maquinaria y construcción','negocios maquinaria y construcci髇','negocios maquinaria y construcci�n','negocios, maquinaria y construcción') and local_main_category in ('servicios','servicios negocios y empleo','servicios, negocios y empleo','servicios,negocios y empleo') then 'Professional Services'
                     when local_category_level1 in ('arrendar','arriendo','comprar') and local_main_category in ('inmuebles') then 'Real Estate'
                     when local_main_category in ('unknown','undefined') then 'Undefined'
                     else 'Undefined'
                end vertical,
                'All Yapo' platform,
                environment_id,
                count(distinct event_id) leads,
                count(distinct ad_id) ads_with_lead
            from
                yapocl_databox.insights_events_behavioral_fact_layer_365d
            where
                event_type in ('Call','SMS','Send')
                and cast(date_parse(cast(year as varchar) || '-' || cast(month as varchar) || '-' || cast(day as varchar),'%Y-%c-%e') as date) = date '{0}'
            group by 1,2,3,4
            ) a
        group by 1,2,3
        order by 1,2,3
        """.format(self.params.get_date_from())
        return queryAthena

    def get_liquidity(self) -> str:
        """
        Method return str with query
        """
        queryAthena = """
        select 
            insertion_date timedate,
            vertical,
            sum(case when ad_lead = 'ad_with_lead_7days' then 1 else 0 end) ad_with_lead_7days
        from
            (
            select 
                ad_id,
                vertical,
                insertion_date,
                case when days_first_lead <7 then 'ad_with_lead_7days' 
                     else 'ad_without_lead_7days' 
                end ad_lead
            from 
                (
                select
                    e.ad_id,
                    case when e.local_main_category in ('vehiculos','vehículos','veh�culos') then 'Motor' 
                         when e.local_category_level1 in ('arriendo de temporada') and e.local_main_category in ('inmuebles') then 'Holiday Rental' 
                         when e.local_category_level1 in ('ofertas de empleo') and e.local_main_category in ('servicios','servicios negocios y empleo','servicios, negocios y empleo','servicios,negocios y empleo') then 'Jobs'
                         when e.local_main_category in ('computadores & electrónica','computadore & electronica','computadores & electr�nica','computadores y electronica','futura mamá','futura mam�','futura mama bebes y ninos','futura mamá bebés y niños','futura mamá, bebés y niños','futura mamá,bebés y niños','futura mam� beb�s y ni�os','futura mam�,beb�s y ni�os','hogar','moda','moda calzado belleza y salud','moda, calzado, belleza y salud','moda,calzado,belleza y salud','otros','otros productos','other','tiempo libre') then 'Consumer Goods'
                         when e.local_category_level1 in ('busco empleo','servicios','negocios,maquinaria y construccion','negocios maquinaria y construccion','negocios maquinaria y construcción','negocios maquinaria y construcci髇','negocios maquinaria y construcci�n','negocios, maquinaria y construcción') and e.local_main_category in ('servicios','servicios negocios y empleo','servicios, negocios y empleo','servicios,negocios y empleo') then 'Professional Services'
                         when e.local_category_level1 in ('arrendar','arriendo','comprar') and e.local_main_category in ('inmuebles') then 'Real Estate'
                         when e.local_main_category in ('unknown','undefined') then 'Undefined'
                         else 'Undefined'
                    end vertical,
                    cast(date_parse(cast(a.year as varchar) || '-' || cast(a.month as varchar) || '-' || cast(a.day as varchar),'%Y-%c-%e') as date) insertion_date,
                    date_diff ('day', cast(date_parse(cast(a.year as varchar) || '-' || cast(a.month as varchar) || '-' || cast(a.day as varchar),'%Y-%c-%e') as date) , min(cast(date_parse(cast(a.year as varchar) || '-' || cast(a.month as varchar) || '-' || cast(a.day as varchar),'%Y-%c-%e') as date)) ) days_first_lead
                from
                    yapocl_databox.insights_events_behavioral_fact_layer_365d e
                    left join
                    (
                    select *
                    from
                        yapocl_databox.insights_events_content_fact_layer_365d
                    where
                        object_type = 'ClassifiedAd' and event_name='Ad published'
                    ) a 
                    using(ad_id)
                where
                    e.event_type in ('Call','SMS','Send', 'Show')
                    and cast(date_parse(cast(a.year as varchar) || '-' || cast(a.month as varchar) || '-' || cast(a.day as varchar),'%Y-%c-%e') as date) = date '{0}'
                group by 1,2,3
                union all
                select
                    e.ad_id,
                    'All Yapo' vertical,
                    cast(date_parse(cast(a.year as varchar) || '-' || cast(a.month as varchar) || '-' || cast(a.day as varchar),'%Y-%c-%e') as date) insertion_date,
                    date_diff ('day', cast(date_parse(cast(a.year as varchar) || '-' || cast(a.month as varchar) || '-' || cast(a.day as varchar),'%Y-%c-%e') as date) , min(cast(date_parse(cast(a.year as varchar) || '-' || cast(a.month as varchar) || '-' || cast(a.day as varchar),'%Y-%c-%e') as date)) ) days_first_lead
                from
                    yapocl_databox.insights_events_behavioral_fact_layer_365d e
                    left join
                    (
                    select *
                    from
                        yapocl_databox.insights_events_content_fact_layer_365d
                    where
                        object_type = 'ClassifiedAd' and event_name='Ad published'
                    ) a 
                    using(ad_id)
                where
                    e.event_type in ('Call','SMS','Send', 'Show')
                    and cast(date_parse(cast(a.year as varchar) || '-' || cast(a.month as varchar) || '-' || cast(a.day as varchar),'%Y-%c-%e') as date) = date '{0}'
                group by 1,2,3
                ) z
            ) y
        group by 1,2
        order by 1,2
        """.format(self.params.get_date_from())
        return queryAthena

    def delete_base_traffic(self) -> str:
        """
        Method that returns events of the day
        """
        command = """
                    delete from dm_analysis.temp_traffic where 
                    timedate::date = 
                    '""" + self.params.get_date_from() + """'::date """
        return command

    def delete_base_leads_per_user(self) -> str:
        """
        Method that returns events of the day
        """
        command = """
                    delete from dm_analysis.temp_leads_per_user where 
                    timedate::date = 
                    '""" + self.params.get_date_from() + """'::date """
        return command

    def delete_base_liquidity(self) -> str:
        """
        Method that returns events of the day
        """
        command = """
                    delete from dm_analysis.temp_liquidity where 
                    timedate::date = 
                    '""" + self.params.get_date_from() + """'::date """
        return command
