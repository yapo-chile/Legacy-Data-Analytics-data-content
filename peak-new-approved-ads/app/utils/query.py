from utils.read_params import ReadParams


class Query:
    """
    Class that store all querys
    """
    def query_new_approved_ads(self, params: ReadParams) -> str:
        """
        Method return str with query
        """
        query = """
                select
                approval_date,
                vertical,
                sum(new_ads) as naa
                from
                (--aa
                    select
                    approval_date::date as approval_date,
                    case
                        when category_id_fk in (47,48) then 'Real estate'
                        when category_id_fk in (49) then 'Holiday rental'
                        when category_id_fk in (7,8,9,10,11,12) then 'Motor'
                        when category_id_fk in (32) then 'Jobs'
                        when category_id_fk in (33,34,35)
                            then 'Professional services'
                        when category_id_fk in 
                        (28,29,30,31,37,38,39,50,40,41,42,43,19,20,21,22,23,24,25,26,36,44,45,46) 
                            then 'Consumer Goods'
                        else 'Undefined'
                    end as vertical,
                    count(distinct(ad_id_pk)) as new_ads
                    from
                        ods.ad
                    where
                        approval_date::date 
                        between 
                        '""" + params.get_date_from() + """'::date
                        and '""" + params.get_date_to() + """'::date
                    group by
                        1,2
                    --QUERY DE BIG SELLERS A DW
                    union all select 
                        bs.list_time::date as approval_date,
                        case
                            when category_id_fk in (47,48) then 'Real estate'
                            when category_id_fk in (49) then 'Holiday rental'
                            when category_id_fk in (7,8,9,10,11,12)
                                then 'Motor'
                            when category_id_fk in (32)
                                then 'Jobs'
                            when category_id_fk in (33,34,35)
                                then 'Professional services'
                            when category_id_fk in 
                            (28,29,30,31,37,38,39,50,40,41,42,43,19,20,21,22,23,24,25,26,36,44,45,46)
                                then 'Consumer Goods'
                            else 'Undefined'
                        end as vertical,
                        count(distinct(a.ad_id_pk)) as new_ads
                    from
                        ods.ad a 
                    inner join
                        stg.big_sellers_detail bs
                        on bs.ad_id_nk::int = a.ad_id_nk
                    where
                        bs.list_time::date between
                        '""" + params.get_date_from() + """'::date
                        and '""" + params.get_date_to() + """'::date
                    group by
                        1,2
                    )aa
                    group by
                        1,2	
            """
        return query

    def delete_new_approved_ads(self, params: ReadParams) -> str:
        """
        Method that returns events of the day
        """
        command = """
                    delete from dm_peak.new_approved_ads where 
                    approval_date::date between
                    '""" + params.get_date_from() + """'::date and
                    '""" + params.get_date_to() + """'::date """

        return command
