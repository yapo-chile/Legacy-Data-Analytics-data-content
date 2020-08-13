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

    def query_get_data(self) -> str:
        """
        Method return str with query
        """
        query = """
            select
                x2.review_time,
                x2.queue,
                (ads_5 *1.0 / total_ads)*100 as "< 5 min",
                (ads_5_15 *1.0 / total_ads)*100 as "(5:00 - 14:59)min",
                (ads_15_30 *1.0 / total_ads)*100 as "(15:00 - 29:59)min",
                (ads_30_45 *1.0 / total_ads)*100 as "(30:00 - 44:59)min",
                (ads_45_60 *1.0 / total_ads)*100 as "(45:00 - 59:59)min",
                (ads_1_13 *1.0 / total_ads)*100 as "(1:00 - 1:30)hrs",
                (ads_13_2 *1.0 / total_ads)*100 as "(1:31 - 2:00)hrs",
                (ads_2_3 *1.0 / total_ads)*100 as "(2:01 - 3:00)hrs",
                (ads_3_4 *1.0 / total_ads)*100 as "(3:01 - 4:00)hrs",
                (ads_4 *1.0 / total_ads)*100 as "> 4 hrs",
                ads_5 as "avisos - < 5 min",
                (ads_5_15 ) as "avisos -(5:00 - 14:59)min",
                (ads_15_30 ) as "avisos -(15:00 - 29:59)min",
                (ads_30_45 ) as "avisos -(30:00 - 44:59)min",
                (ads_45_60 ) as "avisos -(45:00 - 59:59)min",
                (ads_1_13) as "avisos -(1:00 - 1:30)hrs",
                (ads_13_2 ) as "avisos -(1:31 - 2:00)hrs",
                (ads_2_3 ) as "avisos -(2:01 - 3:00)hrs",
                (ads_3_4 ) as "avisos -(3:01 - 4:00)hrs",
                (ads_4) as "avisos -> 4 hrs"
            from (--x2
                select
                    x1.review_time,
                    x1.queue,
                    sum(case when orden_rango = 1 then ads_revisados end) as ads_5,
                    sum(case when orden_rango = 2 then ads_revisados end) as ads_5_15,
                    sum(case when orden_rango = 3 then ads_revisados end) as ads_15_30,
                    sum(case when orden_rango = 4 then ads_revisados end) as ads_30_45,
                    sum(case when orden_rango = 5 then ads_revisados end) as ads_45_60,
                    sum(case when orden_rango = 6 then ads_revisados end) as ads_1_13,
                    sum(case when orden_rango = 7 then ads_revisados end) as ads_13_2,
                    sum(case when orden_rango = 8 then ads_revisados end) as ads_2_3,
                    sum(case when orden_rango = 9 then ads_revisados end) as ads_3_4,
                    sum(case when orden_rango = 10 then ads_revisados end) as ads_4,
                    sum(ads_revisados) as total_ads
                from (--x1
                    select
                        review_time::date,
                        tipo_accion,
                        grupo_revision,
                        queue,
                        case
                            when (tpo_creation_exit_min_real < 5) then '< 5 min'
                            when (tpo_creation_exit_min_real >= 5 and tpo_creation_exit_min_real < 15) then '(5:00 - 14:59)min'
                            when (tpo_creation_exit_min_real >= 15 and tpo_creation_exit_min_real < 30) then '(15:00 - 29:59)min'
                            when (tpo_creation_exit_min_real >= 30 and tpo_creation_exit_min_real < 45) then '(30:00 - 44:59)min'
                            when (tpo_creation_exit_min_real >= 45 and tpo_creation_exit_min_real < 60) then '(45:00 - 59:59)min'
                            when (tpo_creation_exit_min_real >= 60 and tpo_creation_exit_min_real <= 90) then '(1:00 - 1:30)hrs'
                            when (tpo_creation_exit_min_real > 90 and tpo_creation_exit_min_real <= 120) then '(1:31 - 2:00)hrs'
                            when (tpo_creation_exit_min_real > 120 and tpo_creation_exit_min_real <= 180) then '(2:01 - 3:00)hrs'
                            when (tpo_creation_exit_min_real > 180 and tpo_creation_exit_min_real <= 240) then '(3:01 - 4:00)hrs'
                            when (tpo_creation_exit_min_real > 240) then '> 4 hrs'
                        end as rango_tiempo_revision,
                        case
                            when (tpo_creation_exit_min_real < 5) then 1
                            when (tpo_creation_exit_min_real >= 5 and tpo_creation_exit_min_real < 15) then 2
                            when (tpo_creation_exit_min_real >= 15 and tpo_creation_exit_min_real < 30) then 3
                            when (tpo_creation_exit_min_real >= 30 and tpo_creation_exit_min_real < 45) then 4
                            when (tpo_creation_exit_min_real >= 45 and tpo_creation_exit_min_real < 60) then 5
                            when (tpo_creation_exit_min_real >= 60 and tpo_creation_exit_min_real <= 90) then 6
                            when (tpo_creation_exit_min_real > 90 and tpo_creation_exit_min_real <= 120) then 7
                            when (tpo_creation_exit_min_real > 120 and tpo_creation_exit_min_real <= 180) then 8
                            when (tpo_creation_exit_min_real > 180 and tpo_creation_exit_min_real <= 240) then 9
                            when (tpo_creation_exit_min_real > 240) then 10
                        end as orden_rango,
                        count(*) as ads_revisados
                    from (--zz1
                        select
                            m.*,
                            (date_part('day', time_stamp_exit_real - time_stamp_creation) * 24 + date_part('hour', time_stamp_exit_real - time_stamp_creation))*60
                            + date_part ('minute', time_stamp_exit_real - time_stamp_creation) as tpo_creation_exit_min_real
                        from (--m
                            select
                                t.ad_id,
                                t.action_type,
                                t.queue,
                                t.review_time,
                                t.time_stamp_creation,
                                t.time_stamp_exit,
                                t.admin_id,
                                t.admin_fullname,
                                t.grupo_revision,
                                t.category,
                                t.action,
                                t.action_id,
                                (date_part('day', time_stamp_exit - time_stamp_creation) * 24 + date_part('hour', time_stamp_exit - time_stamp_creation))*60
                                + date_part ('minute', time_stamp_exit - time_stamp_creation) as tpo_creation_exit_min,
                                case when (time_stamp_creation = time_stamp_creation_lag) then null else
                                (date_part('day', time_stamp_exit - review_time) * 24 + date_part('hour', time_stamp_exit - review_time))*60
                                + date_part ('minute', time_stamp_exit - review_time) end as tpo_review_exit_min,
                                (date_part('day', review_time - time_stamp_creation) * 24 + date_part('hour', review_time - time_stamp_creation))*60
                                + date_part ('minute', review_time - time_stamp_creation) as tpo_creation_review_min,
                                (case when (time_stamp_creation = time_stamp_creation_lag) then 'calidad'
                                    when (action_type in ('adminedit', 'post_refusal')) then 'calidad'
                                    when (action_type_2 in ('adminedit', 'post_refusal')) then 'calidad'
                                    when (action_type_2 = 'disable' and t.action = 'refused' and action_type = 'status_change') then 'calidad'
                                    when (action_type_2 = 'remove_gallery' and t.time_stamp_exit is null and admin_id = 141) then 'calidad'
                                    when (action_type_2 in ('adminedit', 'post_refusal')) then 'calidad'
                                    when (time_stamp_creation_lag is null and action_type_2 is null and action_type_3 = 'post_refusal') then 'calidad'
                                    when (action_type='bump' and action_type_2='bump' and action_type_3='bump' and action='refused') then 'calidad'
                                    else 'revision' end) as tipo_accion,
                                t.action_type_2,
                                coalesce(t.action_type_2, t.action_type) as real_action_type,
                                case when time_stamp_exit is null then review_time else time_stamp_exit end as time_stamp_exit_real
                            from (--t
                                select
                                    r.ad_id,
                                    r.action_type,
                                    r.queue,
                                    r.review_time,
                                    acts."timestamp" as time_stamp_creation,
                                    lag(acts."timestamp",1) over(partition by r.ad_id, r.action_id order by r.ad_id, r.action_id, acts."timestamp") as time_stamp_creation_lag,
                                    aa.locked_until as time_stamp_exit,
                                    r.admin_id,
                                    am.fullname as admin_fullname,
                                    case
                                        when upper(am.fullname) like '%BESEDO%' then 'Besedo'
                                        when am.fullname is null then null
                                        else 'Yapo.cl' end as grupo_revision,
                                    r.category,
                                    r.action,
                                    r.action_id,
                                    acs.action_type as action_type_2,
                                    aa2.action_type as action_type_3
                                from review_log r
                                left join (--aa
                                    select
                                        ad_id,
                                        action_id,
                                        locked_until
                                    from ad_actions aa
                                    union all
                                    select
                                        ad_id,
                                        action_id,
                                        locked_until
                                    from blocket_{current_year}.ad_actions aa
                                    union all
                                    select
                                        ad_id,
                                        action_id,
                                        locked_until
                                    from blocket_{last_year}.ad_actions aa
                                )aa	on r.ad_id = aa.ad_id and r.action_id = aa.action_id
                                left join
                                    (--aa2
                                    select
                                        ad_id,
                                        action_id,
                                        locked_until,
                                        aa.action_type
                                    from ad_actions aa
                                    union all
                                    select
                                        ad_id,
                                        action_id,
                                        locked_until,
                                        aa.action_type
                                    from blocket_{current_year}.ad_actions aa
                                    union all
                                    select
                                        ad_id,
                                        action_id,
                                        locked_until,
                                        aa.action_type
                                    from blocket_{last_year}.ad_actions aa
                                    )aa2 on r.ad_id = aa2.ad_id and r.action_id = aa2.action_id-1
                                left join admins am on am.admin_id = r.admin_id
                                left join (--acts
                                    select
                                        acts."timestamp",
                                        ad_id,
                                        action_id,
                                        acts.transition
                                    from
                                        action_states acts
                                    where acts."timestamp"::date between '{date_from}'::date-2 and '{date_from}'::date
                                    union all
                                    select
                                        acts."timestamp",
                                        ad_id,
                                        action_id,
                                        acts.transition
                                    from blocket_{current_year}.action_states acts
                                    where acts."timestamp"::date between '{date_from}'::date-2 and '{date_from}'::date
                                    union all
                                    select
                                        acts."timestamp",
                                        ad_id,
                                        action_id,
                                        acts.transition
                                    from blocket_{last_year}.action_states acts
                                    where acts."timestamp"::date between '{date_from}'::date-2 and '{date_from}'::date
                                    )acts on acts.transition = 'initial' and acts.ad_id = r.ad_id and acts.action_id = r.action_id
                                left join (--acs
                                    select
                                        acs.ad_id,
                                        acs."timestamp",
                                        aac.action_type
                                    from (--acs
                                        select
                                            acts.ad_id,
                                            acts."timestamp",
                                            min(acts.action_id) as action_id
                                        from action_states acts
                                        where acts."timestamp"::date between '{date_from}'::date-2 and '{date_from}'::date
                                        group by 1,2
                                        union all
                                        select
                                            acts.ad_id,
                                            acts."timestamp",
                                            min(acts.action_id) as action_id
                                        from blocket_{current_year}.action_states acts
                                        where acts."timestamp"::date between '{date_from}'::date-2 and '{date_from}'::date
                                        group by 1,2

                                        union all

                                        select
                                            acts.ad_id,
                                            acts."timestamp",
                                            min(acts.action_id) as action_id
                                        from blocket_{last_year}.action_states acts
                                        where acts."timestamp"::date between '{date_from}'::date-2 and '{date_from}'::date
                                        group by 1,2
                                        )acs
                                    inner join ad_actions aac on aac.ad_id = acs.ad_id and aac.action_id = acs.action_id
                                    )acs on acs.ad_id = r.ad_id and acs."timestamp" = r.review_time
                                where 1=1
                                    and r.review_time::date = '{date_from}'::date
                                    and r.queue in ('normal', 'all', 'difficult', 'duplicated', 'edit', 'medium','duplicated_active')
                                )t
                            where grupo_revision = 'Besedo'
                        )m
                    where tipo_accion = 'revision'
                    )zz1
                group by 1,2,3,4,5,6
                )x1
            group by 1,2
            ) as x2;
            """.format(current_year=self.params.current_year,
                       last_year=self.params.last_year,
                       date_from=self.params.date_from)
        return query

    def query_get_data_reviews(self)->str:
        """
        Method return str with query
        """
        query = """
            select 
                ad_id,
                action_type,
                queue,
                review_time,
                date_from as review_time_date,
                admin_id,
                category,
                action, 
                action_id
            from review_log
            where review_time::date = '{date_from}'::date
            order by ad_id, action_id asc
        """.format(date_from=self.params.date_from)
        return query
