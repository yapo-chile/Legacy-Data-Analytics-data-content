from infraestructure.conf import getConf
from utils.read_params import ReadParams


class QueryP1:
    """
    Class that store all querys
    """
    def __init__(self,
                 conf: getConf,
                 params: ReadParams) -> None:
        self.params = params
        self.conf = conf

    def get_data_review_ads(self) -> str:
        """
        Method return str with query
        """
        return """
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
                case    when upper(am.fullname) like '%BESEDO%' then 'Besedo'
                        when am.fullname is null then null
                        else 'Yapo.cl' end as grupo_revision,
                r.category,
                r.action,
                r.action_id,
                acs.action_type as action_type_2,
                aa2.action_type as action_type_3
            from review_log r
            left join (--aa
                select  ad_id,
                        action_id,
                        locked_until
                from ad_actions aa 
                union all
                select  ad_id,
                        action_id,
                        locked_until
                from blocket_{current_year}.ad_actions aa 
                union all
                select  ad_id,
                        action_id,
                        locked_until
                from blocket_{last_year}.ad_actions aa 
            )aa	on r.ad_id = aa.ad_id and r.action_id = aa.action_id
            left join (--aa2
                select  ad_id,
                        action_id,
                        locked_until,
                        aa.action_type
                from ad_actions aa 
                union all
                select  ad_id,
                        action_id,
                        locked_until,
                        aa.action_type
                from blocket_{current_year}.ad_actions aa 
                union all
                select  ad_id,
                        action_id,
                        locked_until,
                        aa.action_type
                from blocket_{last_year}.ad_actions aa 
            )aa2 on r.ad_id = aa2.ad_id and r.action_id = aa2.action_id-1
            left join admins am on am.admin_id = r.admin_id
            left join (--acts
                select  acts."timestamp",
                        ad_id,
                        action_id,
                        acts.transition
                from action_states acts 
                where acts."timestamp"::date between '{date_from}'::date-2 and '{date_from}'::date
                union all	
                select  acts."timestamp",
                        ad_id,
                        action_id,
                        acts.transition
                from blocket_{current_year}.action_states acts 
                where acts."timestamp"::date between '{date_from}'::date-2 and '{date_from}'::date
                union all	
                select  acts."timestamp",
                        ad_id,
                        action_id,
                        acts.transition
                from blocket_{last_year}.action_states acts 
                where acts."timestamp"::date between '{date_from}'::date-2 and '{date_from}'::date
            )acts on acts.transition = 'initial' and acts.ad_id = r.ad_id and acts.action_id = r.action_id
            left join (--acs
                select  acs.ad_id,
                        acs."timestamp",
                        aac.action_type
                from (--acs	
                    select  acts.ad_id,
                            acts."timestamp",
                            min(acts.action_id) as action_id
                    from action_states acts
                    where acts."timestamp"::date between '{date_from}'::date-2 and '{date_from}'::date
                    group by 1,2
                    union all
                    select  acts.ad_id,
                            acts."timestamp",
                            min(acts.action_id) as action_id
                    from blocket_{current_year}.action_states acts
                    where acts."timestamp"::date between '{date_from}'::date-2 and '{date_from}'::date
                    group by 1,2
                    union all
                    select  acts.ad_id,
                            acts."timestamp",
                            min(acts.action_id) as action_id
                    from blocket_{last_year}.action_states acts
                    where acts."timestamp"::date between '{date_from}'::date-2 and '{date_from}'::date
                    group by 1,2 
                ) acs 
                inner join ad_actions aac on aac.ad_id = acs.ad_id and aac.action_id = acs.action_id
            )acs on acs.ad_id = r.ad_id and acs."timestamp" = r.review_time
            where 1=1
                and r.review_time::date = '{date_from}'::date
            order by r.ad_id, r.action_id, acts."timestamp" asc;
        """.format(current_year=self.params.current_year,
                    last_year=self.params.last_year,
                    date_from=self.params.date_from)
         
