# pylint: disable=no-member
# utf-8


class TableauInsertingFeeQuery:
    def clean_inserting_fee(self) -> str:
        return """delete from dm_analysis.temp_tableau_inserting_fee 
                    where date_id::date between '{}'::date 
                    and '{}'::date""".format(self.params.get_date_from(),
                       self.params.get_date_to())

    def select_inferting_fee(self) -> str:
        """
        Method return str with query
        """
        query = """
            select 
                month_id,
                date_id,
                aa.platform_name,
                aa.category_name,
                aa.user_type,
                aa.qty_ads,
                aa.qty_sellers,
                fee.qty_pp as qty_fee,
                (fee.price_pp/1.19)::int as price_fee,
                fee.qty_sellers as qty_sellers_fee,
                fee.qty_ads as ads_fee
            from
                ods.dim_calendar dc
            left join
                (--aa
                select
                    a.approval_date::date,
                    case
                        when a.platform_id_fk in (1) then 'desktop'
                        when a.platform_id_fk in (2) then 'msite'
                        when a.platform_id_fk in (5,7) then 'ios'
                        when a.platform_id_fk in (6,8,3) then 'android'
                        else 'msite' end as platform_name,
                    c.category_name,
                    case
                        when spj.seller_id_fk is null then 'Private'
                        else 'Professional' end as user_type,
                    count(distinct a.ad_id_pk) as qty_ads,
                    count(distinct a.seller_id_fk) as qty_sellers
                from
                    ods.ad a
                left join
                    (--seller pro
                    select 
                        sp.seller_id_fk,
                        sp.category_id_fk
                    from
                        ods.seller_pro_details sp
                    where
                        category_id_fk in (32,7,8,47,48)
                    )spj on spj.seller_id_fk = a.seller_id_fk and spj.category_id_fk = a.category_id_fk
                left join
                    ods.category c on c.category_id_pk = a.category_id_fk
                where
                    a.category_id_fk in (32,7,8,47,48)
                    and approval_date::date >= '{DATE_FROM}'
                group by 1,2,3,4
                )aa on aa.approval_date::date = dc.date_id::date 
            left join
                (--qty_IF and $_IF and ads_insertion_fee and sellers_IF
                select 
                    po.payment_date::date,
                    case
                        when po.payment_platform = 'unknown' then 'msite'
                        else po.payment_platform
                        end as payment_platform,
                    c.category_name,
                    'Professional' as user_type,
                    count(distinct po.product_order_id_pk) as qty_pp,
                    coalesce(sum(po.price)) as price_pp,
                    count(distinct po.user_id_nk) as qty_sellers,
                    count(distinct po.ad_id_fk) as qty_ads
                from
                    ods.product_order po 
                inner join
                    ods.ad a on a.ad_id_pk = po.ad_id_fk and a.category_id_fk in (32,7,8,47,48) and a.approval_date::date >= '2016-09-01'
                --quizas quitar la categoria una vez exista el producto ya que solo existe en jobs
                left join
                    ods.category c on c.category_id_pk = a.category_id_fk
                where 
                    po.product_id_fk in (21,22,23,421)
                    and po.payment_date::date >= '{DATE_FROM}'
                    and po.status in ('sent','paid','confirmed','failed')
                    and (po.payment_method <> 'credits' or po.payment_method is null)
                group by 1,2,3,4
                union all
                select 
                    poi.payment_date::date,
                    'ios' as payment_platform,
                    c.category_name,
                    'Professional' as user_type,
                    count(distinct poi.product_order_nk) as qty_pp,
                    coalesce(sum(poi.price_clp)) as price_pp,
                    count(distinct poi.user_id_nk) as qty_sellers,
                    count(distinct poi.ad_id_nk) as qty_ads
                from
                    ods.product_order_ios poi 
                inner join
                    ods.ad a on a.ad_id_nk = poi.ad_id_nk and a.category_id_fk in (32,7,8,47,48) and a.approval_date::date >= '2016-09-01'
                --quizas quitar la categoria una vez exista el producto ya que solo existe en jobs
                left join
                    ods.category c on c.category_id_pk = a.category_id_fk
                where 
                    poi.product_id_nk in (60,61,62,1080) and poi.payment_date::date >= '{DATE_FROM}' and poi.status in ('sent','paid','confirmed','failed')
                group by 1,2,3,4
                )fee on fee.payment_date::date = dc.date_id and fee.payment_platform = aa.platform_name and fee.user_type = aa.user_type and fee.category_name = aa.category_name
            where dc.date_id between '{DATE_FROM}' and '{DATE_TO}'
            """.format(DATE_FROM=self.params.get_date_from(),
                       DATE_TO=self.params.get_date_to())
        return query

