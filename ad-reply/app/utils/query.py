# pylint: disable=no-member
# utf-8


class AdReplyQuery:

    def dwh_ad_reply_rank(self) -> str:
        return """SELECT
            ad_reply_id_pk
            ,ad_reply_id_nk 
            ,buyer_id_fk
            FROM ods.ad_reply
            where rank is null and ad_reply_id_nk is not null and ad_id_fk >0
            order by ad_reply_creation_date, ad_reply_id_nk;
        """

    def dwh_ad_reply_by_id_buyer(self, ids) -> str:
        return """select * from ods.ad_reply
            where buyer_id_fk in ({}) and rank is not null""".format(",".join(ids))

    def clean_stg_ad_reply(self) -> str:
        return """truncate stg.ad_reply;"""

    def clean_ods_ad_reply(self) -> str:
        """
        Cleans yesterday's data to avoid duplicate in insert
        """
        return """delete from ods.ad_reply
            where ad_reply_creation_date 
                between '{DATE_FROM}' and '{DATE_TO}';""".format(
                       DATE_FROM=self.params.get_date_from(),
                       DATE_TO=self.params.get_date_to())

    def clean_ods_ad_reply_ranks(self, ids) -> str:
        """
        Cleans yesterday's data to avoid duplicate in insert
        """
        return """delete from ods.ad_reply
            where rank is null
            and ad_id_fk > 0 
            and ad_reply_id_pk in ({})""".format(",".join(ids))

    def ods_stg_ad_reply_comparation(self) -> str:
        return """SELECT
                ar.sender_email as buyer_id_nk,
                b.buyer_id_pk as buyer_id_fk
                , min(ar.mail_queue_id) as ad_reply_id_nk
                , ar.added_at as ad_reply_creation_date
                , ar.sender_email as email
                , ar.list_id as list_id_nk
                , now() as insert_date
                , a.ad_id_pk as ad_id_fk
                FROM stg.ad_reply ar
                inner join ods.ad a on ar.ad_id = a.ad_id_nk
                inner join ods.buyer b on  b.buyer_id_nk = ar.sender_email
                group by sender_email,b.buyer_id_pk ,added_at, list_id, a.ad_id_pk
                order by added_at
            """

    def blocket_ad_reply(self) -> str:
        """
        Method return str with query
        """
        query = """
            SELECT
                mq.mail_queue_id
                ,mq."state"
                ,mq.template_name
                ,mq.added_at
                ,mq.remote_addr
                ,lower(mq.sender_email) as sender_email
                ,mq.sender_name
                ,mq.receipient_email
                ,mq.receipient_name
                ,mq.subject
                ,mq.body
                ,mq.list_id
                ,mq.rule_id
                ,mq.reason
                ,mq.sender_phone
                ,a.ad_id as ad_id
            FROM 
                blocket_{CURRENT_YEAR}.mail_queue mq
            inner join
                blocket_{CURRENT_YEAR}.ads as a on a.list_id = mq.list_id
            WHERE 
                added_at between '{DATE_FROM} 00:00:00' and '{DATE_TO} 23:59:59'
            union all
            SELECT
                mq.mail_queue_id
                ,mq."state"
                ,mq.template_name
                ,mq.added_at
                ,mq.remote_addr
                ,lower(mq.sender_email) as sender_email
                ,mq.sender_name
                ,mq.receipient_email
                ,mq.receipient_name
                ,mq.subject
                ,mq.body
                ,mq.list_id
                ,mq.rule_id
                ,mq.reason
                ,mq.sender_phone
                ,b.ad_id as ad_id
            FROM
                blocket_{CURRENT_YEAR}.mail_queue mq
            inner join
                public.ads as b on b.list_id = mq.list_id
            WHERE 
	            added_at between '{DATE_FROM} 00:00:00' and '{DATE_TO} 23:59:59'
            """.format(DATE_FROM=self.params.get_date_from(),
                       DATE_TO=self.params.get_date_to(),
                       CURRENT_YEAR=self.params.get_current_year())
        return query
<<<<<<< HEAD
=======

    def get_ad_reply_stg(self) -> str:
        query = """
            SELECT  ad.buyer_id_nk,
                    ad.email,
                    coalesce(b.buyer_id_pk, 0) as buyer_id_pk_aux,
                    ad.buyer_creation_date ,
                    ad.insert_date
            FROM (  SELECT sender_email as buyer_id_nk,
                        sender_email as email,
                        min(added_at) as buyer_creation_date ,
                        now() as insert_date
                    FROM stg.ad_reply , ods.ad
                    WHERE list_id = ad.list_id_nk
                    GROUP BY sender_email) ad
            LEFT JOIN ods.buyer b on ad.email = b.buyer_id_nk;"""
        return query
>>>>>>> [feat/ad-reply]: ad reply etl job
