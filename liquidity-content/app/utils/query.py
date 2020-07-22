class AdsQuery():
    """
    Class that store Ads Queries
    """
    def __init__(self, date):
        self.date = date

    def get_ads(self) -> str:
        """
        Method return str with query
        """
        query = """select
            a.list_id_nk::varchar as ad_id,
            a.approval_date::date::varchar(10) as approval_date,
            a.deletion_date::date::varchar(10) as deletion_date,
            cm.category_main_name as main_category,
            c.category_name as category,
            case
                when a.region_id_fk = 1 then 'Arica & Parinacota'
                when a.region_id_fk = 2 then 'Tarapacá'
                when a.region_id_fk = 3 then 'Antofagasta'
                when a.region_id_fk = 4 then 'Atacama'
                when a.region_id_fk = 5 then 'Coquimbo'
                when a.region_id_fk = 6 then 'Valparaíso'
                when a.region_id_fk = 7 then 'O\\'Higgins'
                when a.region_id_fk = 8 then 'Maule'
                when a.region_id_fk = 9 then 'Biobío'
                when a.region_id_fk = 10 then 'Araucanía'
                when a.region_id_fk = 11 then 'Los Ríos'
                when a.region_id_fk = 12 then 'Los Lagos'
                when a.region_id_fk = 13 then 'Aisén'
                when a.region_id_fk = 14 then 'Magallanes & Antártica'
                when a.region_id_fk = 15 then 'Región Metropolitana'
                else 'undefined'
            end as region,
            case
                when a.platform_id_fk = 1 then 'desktop'
                when a.platform_id_fk = 2 then 'msite'
                when a.platform_id_fk = 7 then 'ios'
                when a.platform_id_fk = 8 then 'android'
                else 'desktop'
            end as platform,
            case
                when a.ad_type_id_fk = 1 then 'Sell'
                when a.ad_type_id_fk = 2 then 'Buy'
                when a.ad_type_id_fk = 3 then 'Rent'
                when a.ad_type_id_fk = 4 then 'Let'
                else 'undefined'
            end as ad_type,
            s.seller_id_nk as lister,
            case
                when spd.seller_id_fk is not null then 'pro'
                else 'pri'
            end as pri_pro,
            case
                when a.reason_removed_detail_id_fk = 1 and a.reason_removed_id_fk = 0 then 'Unknown'
                when a.reason_removed_detail_id_fk = 1 and a.reason_removed_id_fk = 1 then 'User deleted'
                when a.reason_removed_detail_id_fk = 1 and a.reason_removed_id_fk = 2 then 'Admin deleted'
                when a.reason_removed_detail_id_fk = 1 and a.reason_removed_id_fk = 3 then 'Expired'
                when a.reason_removed_detail_id_fk = 1 and a.reason_removed_id_fk = 4 then 'Refused'
                when a.reason_removed_detail_id_fk = 1 and a.reason_removed_id_fk = 5 then 'Auto deleted'
                else 'undefined'
                end as reason_removed
        from ods.ad a
            left join ods.seller_pro_details spd on spd.category_id_fk = a.category_id_fk 
                                                    and spd.seller_id_fk = a.seller_id_fk
            left join ods.category c on a.category_id_fk = c.category_id_pk 
                                        and c.date_to > current_timestamp
            left join ods.category_main cm on c.category_main_id_fk = cm.category_main_id_pk 
                                        and cm.date_to > current_timestamp
            left join ods.seller s on s.seller_id_pk = a.seller_id_fk
        where 1=1
            and a.list_id_nk is not null
            and (a.approval_date::date = '{date}' or a.deletion_date::date = '{date}')
        union all
        select 
            bsd.list_id::varchar as ad_id,
            bsd.list_time::date::varchar(10) as approval_date,
            a.deletion_date::date::varchar(10) as deletion_date,
            cm.category_main_name as main_category,
            c.category_name as category,
            case
                when a.region_id_fk = 1 then 'Arica & Parinacota'
                when a.region_id_fk = 2 then 'Tarapacá'
                when a.region_id_fk = 3 then 'Antofagasta'
                when a.region_id_fk = 4 then 'Atacama'
                when a.region_id_fk = 5 then 'Coquimbo'
                when a.region_id_fk = 6 then 'Valparaíso'
                when a.region_id_fk = 7 then 'O\\'Higgins'
                when a.region_id_fk = 8 then 'Maule'
                when a.region_id_fk = 9 then 'Biobío'
                when a.region_id_fk = 10 then 'Araucanía'
                when a.region_id_fk = 11 then 'Los Ríos'
                when a.region_id_fk = 12 then 'Los Lagos'
                when a.region_id_fk = 13 then 'Aisén'
                when a.region_id_fk = 14 then 'Magallanes & Antártica'
                when a.region_id_fk = 15 then 'Región Metropolitana'
                else 'undefined'
            end as region,
            'desktop' as platform,
            case
                when a.ad_type_id_fk = 1 then 'Sell'
                when a.ad_type_id_fk = 2 then 'Buy'
                when a.ad_type_id_fk = 3 then 'Rent'
                when a.ad_type_id_fk = 4 then 'Let'
                else 'undefined'
            end as ad_type,
            s.seller_id_nk as lister,
            'pro' as pri_pro,
            case
                when a.reason_removed_detail_id_fk = 1 and a.reason_removed_id_fk = 0 then 'Unknown'
                when a.reason_removed_detail_id_fk = 1 and a.reason_removed_id_fk = 1 then 'User deleted'
                when a.reason_removed_detail_id_fk = 1 and a.reason_removed_id_fk = 2 then 'Admin deleted'
                when a.reason_removed_detail_id_fk = 1 and a.reason_removed_id_fk = 3 then 'Expired'
                when a.reason_removed_detail_id_fk = 1 and a.reason_removed_id_fk = 4 then 'Refused'
                when a.reason_removed_detail_id_fk = 1 and a.reason_removed_id_fk = 5 then 'Auto deleted'
                else 'undefined'
            end as reason_removed
        from stg.big_sellers_detail bsd
            left join ods.ad a on bsd.ad_id_nk::int = a.ad_id_nk
            left join ods.category c on a.category_id_fk = c.category_id_pk and c.date_to > current_timestamp
            left join ods.category_main cm on c.category_main_id_fk = cm.category_main_id_pk and cm.date_to > current_timestamp
            left join ods.seller s on s.seller_id_pk = a.seller_id_fk
        where 1=1
            and (bsd.list_time::date = '{date}' or a.deletion_date::date = '{date}')
        """.format(date=self.date)
        return query
        