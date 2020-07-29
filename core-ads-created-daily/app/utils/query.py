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

    def get_data_ads_created_daily(self) -> str:
        """
        Method return str with query
        """
        queryBlocket = """
                    select
                        z.ad_id,
                        z.list_id,
                        z.user_id,
                        z.account_id,
                        z.email,
                        z.platform_id_nk,
                        z.creation_date,
                        z.approval_date,
                        z.deletion_date,
                        z.category,
                        z.region,
                        z.type,
                        z.company_ad,
                        z.price,
                        z.reason_removed_id_nk,
                        z.reason_removed_detail_id_nk,
                        z.action_type,
                        z.communes_id_nk,
                        z.phone,
                        z.body,
                        z.subject,
                        z.user_name
                    from (
                        select 
                        distinct aa.ad_id, 
                        a.list_id,
                        a.user_id, 
                        ac.account_id,
                        u.email,
                        ap."value" as platform_id_nk,
                        case
                            when (acts.action_id = 1 and acts.state = 'reg' and aa.action_type in ('edit', 'new', 'import')) then acts.timestamp 
                            else null
                        end creation_date,
                        case
                            when (acts.state = 'accepted' and aa.action_type = 'new') then  acts.timestamp
                            else null
                        end approval_date,
                        case
                            when (acts.state in('deleted', 'refused') and aa.action_type in ('delete', 'post_refusal')) then  acts.timestamp
                            else null
                        end deletion_date,
                        a.category, 
                        a.region,
                        a.type,
                        a.company_ad,
                        a.price,
                        case
                            when (acts.state in('deleted', 'refused') and aa.action_type in ('delete', 'post_refusal')) then acts.transition 
                            else null
                        end reason_removed_id_nk,
                        case
                            when (acts.state in('deleted', 'refused') and aa.action_type in ('delete', 'post_refusal')) then rp."value"
                            else null
                        end reason_removed_detail_id_nk,
                        aa.action_type,
                        dp."value"::int as communes_id_nk,
                        a.phone, 
                        a.body,
                        a.subject,
                        a.name	as user_name
                        from 
                            public.action_states as acts
                        left join
                            public.ad_actions as aa on (aa.ad_id = acts.ad_id and aa.action_id = acts.action_id) 
                        left join 
                            public.ads as a on aa.ad_id = a.ad_id
                        left join 
                            public.action_params as ap on (ap.ad_id = aa.ad_id and ap.action_id = aa.action_id and ap.name = 'source')
                        left join 
                            public.users as u on u.user_id = a.user_id
                        left join 
                            public.accounts as ac on ac.user_id = u.user_id
                        left join
                            public.ad_params as dp on (dp.ad_id = a.ad_id and dp.name = 'communes')
                        left join 
                            public.action_params as rp on (rp.ad_id = aa.ad_id and rp.action_id = aa.action_id and rp.name = 'deletion_reason') 
                        where 
                            acts.state in ('reg', 'accepted', 'deleted', 'refused')
                            and acts.timestamp between '{0} 00:00:00' and '{1} 23:59:59'
                        union all
                        select 
                            distinct aa.ad_id, 
                            a.list_id,
                            a.user_id, 
                            ac.account_id,
                            u.email,
                            ap."value" as platform_id_nk,
                            case
                                when (acts.action_id = 1 and acts.state = 'reg' and aa.action_type in ('edit', 'new', 'import')) then acts.timestamp 
                                else null
                            end creation_date,
                            case
                                when  (acts.state = 'accepted' and aa.action_type = 'new') then  acts.timestamp
                                else null
                            end approval_date,
                            case
                                when (acts.state in('deleted', 'refused') and aa.action_type in ('delete', 'post_refusal')) then  acts.timestamp
                                else null
                            end deletion_date,
                            a.category, 
                            a.region,
                            a.type,
                            a.company_ad,
                            a.price,
                            case
                                when (acts.state in('deleted', 'refused') and aa.action_type in ('delete', 'post_refusal')) then acts.transition 
                                else null
                            end reason_removed_id_nk,
                            case
                                when (acts.state in('deleted', 'refused') and aa.action_type in ('delete', 'post_refusal')) then rp."value"
                                else null
                            end reason_removed_detail_id_nk,
                            aa.action_type,
                            dp."value"::int as communes_id_nk,
                            a.phone, 
                            a.body,
                            a.subject,
                            a.name as user_name	
                        from 
                            blocket_{2}.action_states as acts
                        left join
                            blocket_{2}.ad_actions as aa on (aa.ad_id = acts.ad_id and aa.action_id = acts.action_id) 
                        left join 
                            blocket_{2}.ads as a on aa.ad_id = a.ad_id
                        left join 
                            blocket_{2}.action_params as ap on (ap.ad_id = aa.ad_id and ap.action_id = aa.action_id and ap.name = 'source')
                        left join 
                            public.users as u on u.user_id = a.user_id
                        left join 
                            public.accounts as ac on ac.user_id = u.user_id
                        left join
                            blocket_{2}.ad_params as dp on (dp.ad_id = a.ad_id and dp.name = 'communes')
                        left join 
                            blocket_{2}.action_params as rp on (rp.ad_id = aa.ad_id and rp.action_id = aa.action_id and rp.name = 'deletion_reason')
                        where 
                            acts.state in ('reg', 'accepted', 'deleted', 'refused')
                            and acts.timestamp between '{0} 00:00:00' and '{1} 23:59:59'
                        union all
                        select 
                            distinct aa.ad_id, 
                            a.list_id,
                            a.user_id, 
                            ac.account_id,
                            u.email,
                            ap."value" as platform_id_nk,
                            case
                                when (acts.action_id = 1 and acts.state = 'reg' and aa.action_type in ('edit', 'new', 'import')) then acts.timestamp 
                                else null
                            end creation_date,
                            case
                                when  (acts.state = 'accepted' and aa.action_type = 'new') then  acts.timestamp
                                else null
                            end approval_date,
                            case
                                when (acts.state in('deleted', 'refused') and aa.action_type in ('delete', 'post_refusal')) then  acts.timestamp
                                else null
                            end deletion_date,
                            a.category, 
                            a.region,
                            a.type,
                            a.company_ad,
                            a.price,
                            case
                                when (acts.state in('deleted', 'refused') and aa.action_type in ('delete', 'post_refusal')) then acts.transition 
                                else null
                            end reason_removed_id_nk,
                            case
                                when (acts.state in('deleted', 'refused') and aa.action_type in ('delete', 'post_refusal')) then rp."value"
                                else null
                            end reason_removed_detail_id_nk,
                            aa.action_type,
                            dp."value"::int as communes_id_nk,
                            a.phone, 
                            a.body,
                            a.subject,
                            a.name as user_name	
                        from 
                            blocket_{3}.action_states as acts
                        left join
                            blocket_{3}.ad_actions as aa on (aa.ad_id = acts.ad_id and aa.action_id = acts.action_id) 
                        left join 
                            blocket_{3}.ads as a on aa.ad_id = a.ad_id
                        left join 
                            blocket_{3}.action_params as ap on (ap.ad_id = aa.ad_id and ap.action_id = aa.action_id and ap.name = 'source')
                        left join 
                            public.users as u on u.user_id = a.user_id
                        left join 
                            public.accounts as ac on ac.user_id = u.user_id
                        left join
                            blocket_{3}.ad_params as dp on (dp.ad_id = a.ad_id and dp.name = 'communes')
                        left join 
                            blocket_{3}.action_params as rp on (rp.ad_id = aa.ad_id and rp.action_id = aa.action_id and rp.name = 'deletion_reason')
                        where 
                            acts.state in ('reg', 'accepted', 'deleted', 'refused')
                            and acts.timestamp between '{0} 00:00:00' and '{1} 23:59:59'
                        ) z
                    where 
                    z.action_type in ('edit', 'new', 'import','delete', 'post_refusal')
                    and (z.creation_date is not null or z.approval_date is not null or z.deletion_date is not null)
            """.format(self.params.get_date_from(),
                       self.params.get_date_to(),
                       self.params.get_current_year(),
                       self.params.get_last_year())
        return queryBlocket

    def insert_output_to_dw(self) -> str:
        """
        Method return str with query
        """
        query = """
                INSERT INTO dm_analysis.temp_stg_ads
                            (ad_id,
                            list_id,
                            user_id,
                            account_id,
                            email,
                            platform_id_nk,
                            creation_date,
                            approval_date,
                            deletion_date,
                            category, 
                            region,
                            type,
                            company_ad, 
                            price,
                            reason_removed_id_nk,
                            reason_removed_detail_id_nk,
                            action_type,
                            communes_id_nk,
                            phone,
                            body,
                            subject,
                            user_name)
                VALUES %s;"""
        return query

    def delete_output_dw_table(self) -> str:
        """
        Method that returns events of the day
        """
        command = """
                    truncate table dm_analysis.temp_stg_ads 
                """
        return command
