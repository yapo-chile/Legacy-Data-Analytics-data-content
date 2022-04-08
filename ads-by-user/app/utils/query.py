# pylint: disable=no-member
# utf-8


class Queries():
    def get_data(self):
        return """select
                date_time::date as date_time,
                email,
                user_id,
                category,
                sum(nof_ads) as nof_ads,
                sum(nof_sold_ads) as nof_sold_ads
            from (
                select
                    *
                from (
                    select
                        a.deletion_date::date as date_time,
                        s.seller_id_nk as email,
                        s.seller_id_blocket_nk::int as user_id,
                        c.category_id_nk::int as category,
                        0 as nof_ads,
                        count(distinct a.ad_id_pk) as nof_sold_ads
                    from
                        ods.ad a
                    left join
                        ods.seller s on a.seller_id_fk = s.seller_id_pk
                    left join
                        ods.category c on a.category_id_fk = c.category_id_pk and c.date_to > current_timestamp 
                    left join
                        ods.reason_removed_detail rd on a.reason_removed_detail_id_fk = rd.reason_removed_detail_id_pk 
                    where
                        a.deletion_date::date between '{START_DATE}'::date and '{END_DATE}'::date
                        and rd.reason_removed_detail_id_pk in ( 1 )
                    group by 1, 2, 3, 4
                    order by 3,4 desc ) t_sold_ads
                    union all 
                    select
                        *
                    from (
                    select 
                        CASE
                            WHEN action_type = 'import' THEN list_time::date
                            ELSE approval_date::date 
                            END AS date_time,
                        email,
                        user_id,
                        category,
                        count(distinct ad_id_pk) as nof_ads, 
                        0 as nof_sold_ads
                    from (
                        select 
                            a.approval_date::date ,
                            list_time::date,
                            s.seller_id_nk as email,
                            s.seller_id_blocket_nk::int as user_id,
                            c.category_id_nk::int as category,
                            a.ad_id_pk,
                            a.deletion_date,
                            action_type
                        from
                            ods.ad a 
                        LEFT JOIN
                            ods.seller s on a.seller_id_fk = s.seller_id_pk 
                        LEFT JOIN 
                            ods.category c on a.category_id_fk = c.category_id_pk and c.date_to > current_timestamp 
                        LEFT JOIN 
                            stg.big_sellers_detail AS bsd ON a.ad_id_nk = bsd.ad_id_nk
                        where
                            ((a.approval_date::date between '{START_DATE}'::date and '{END_DATE}'::date) or (list_time::date between '{START_DATE}'::date and '{END_DATE}'::date))) z
                        group by 1, 2, 3,4 
                        order by 2,3,4 desc) t_ads 
                    ) union_ads
                group by 1, 2, 3, 4 
                order by 3, 4 desc""".format(START_DATE=self.params.start_date,
                                             END_DATE=self.params.end_date)

    def clean_dwh_table(self):
        return """delete from temp.ads_by_user
                    where date_time between '{}'::date and '{}'::date""".format(self.params.start_date,
                                                                                self.params.end_date)

    def clean_statistics_table(self):
       return """delete from public.ads_by_user2
                    where date_time between '{}'::date and '{}'::date""".format(self.params.start_date,
                                                                                self.params.end_date)