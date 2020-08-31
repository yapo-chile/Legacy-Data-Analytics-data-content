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

    def query_get_data_reviews(self)->str:
        """
        Method return str with query
        """
        return """
            select
                rl.ad_id,
                rl.action_type,
                rl.queue,
                rl.review_time,
                '{date_from}' as review_time_date,
                rl.admin_id,
                rl.category,
                rl.action,
                rl.action_id,
                aa.locked_until as time_stamp_exit,
                aa2.action_type as action_type_3,
                admin.admin_fullname,
                admin.grupo_revision,
                acts."timestamp" as time_stamp_creation,
                acts2.action_type_2
            from review_log rl
            left join (
                select  ad_id,
                        action_id,
                        locked_until
                from ad_actions
                union all
                select  ad_id,
                        action_id,
                        locked_until
                from blocket_{current_year}.ad_actions
                union all
                select  ad_id,
                        action_id,
                        locked_until
                from blocket_{last_year}.ad_actions
            ) aa on rl.ad_id = aa.ad_id and rl.action_id = aa.action_id
            left join (
                select  ad_id,
                        action_id,
                        action_type
                from ad_actions
                union all
                select  ad_id,
                        action_id,
                        action_type
                from blocket_{current_year}.ad_actions
                union all
                select  ad_id,
                        action_id,
                        action_type
                from blocket_{last_year}.ad_actions
            ) aa2 on rl.ad_id = aa2.ad_id and rl.action_id = aa2.action_id-1
            left join (
                select
                    fullname as admin_fullname,
                    case
                        when upper(fullname) like '%BESEDO%' then 'Besedo'
                        when fullname is null then null
                        else 'Yapo.cl'
                    end as grupo_revision,
                    admin_id
                from admins
            ) admin on admin.admin_id = rl.admin_id
            left join (
                select "timestamp",
                        ad_id,
                        action_id
                from action_states
                where 1=1
                    and "timestamp"::date between '{date_from}'::date-2 and '{date_from}'::date
                    and transition = 'initial'
                union all
                select  "timestamp",
                        ad_id,
                        action_id
                from blocket_{current_year}.action_states
                where 1=1
                    and "timestamp"::date between '{date_from}'::date-2 and '{date_from}'::date
                    and transition = 'initial'
                union all
                select  "timestamp",
                        ad_id,
                        action_id
                from blocket_{last_year}.action_states acts
                where 1=1
                    and "timestamp"::date between '{date_from}'::date-2 and '{date_from}'::date
                    and transition = 'initial'
            ) acts on acts.ad_id = rl.ad_id and acts.action_id = rl.action_id
            left join (
                select  acs2.ad_id,
                        acs2."timestamp",
                        aac.action_type as action_type_2
                from (
                    select  ad_id,
                            "timestamp",
                            min(action_id) as action_id
                    from action_states
                    where "timestamp"::date between '{date_from}'::date-2 and '{date_from}'::date
                    group by 1,2
                    union all
                    select  ad_id,
                            "timestamp",
                            min(action_id) as action_id
                    from blocket_{current_year}.action_states
                    where "timestamp"::date between '{date_from}'::date-2 and '{date_from}'::date
                    group by 1,2
                    union all
                    select  ad_id,
                            "timestamp",
                            min(action_id) as action_id
                    from blocket_{last_year}.action_states
                    where "timestamp"::date between '{date_from}'::date-2 and '{date_from}'::date
                    group by 1,2
                    ) acs2
                inner join ad_actions aac on aac.ad_id = acs2.ad_id and aac.action_id = acs2.action_id
            ) acts2 on acts2.ad_id = rl.ad_id and acts."timestamp" = acts2."timestamp"
            where 1=1
                and review_time::date = '{date_from}'::date
            order by rl.ad_id, rl.action_id, acts."timestamp" asc
        """.format(current_year=self.params.current_year,
                    last_year=self.params.last_year,
                    date_from=self.params.date_from)
