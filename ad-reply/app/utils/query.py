# pylint: disable=no-member
# utf-8


class AdReplyQuery:

    def dwh_ad_reply(self, ids) -> str:
        return "select * from ods.ad_reply where buyer_id_fk in ({})".format(ids)

    def clean_ods_ad_reply(self) -> str:
        """
        Cleans yesterday's data to avoid duplicate in insert
        """
        return """delete from ods.ad_reply
            where ad_reply_creation_date 
                between '{DATE_FROM}' and '{DATE_TO}';""".format(
                       DATE_FROM=self.params.get_date_from(),
                       DATE_TO=self.params.get_date_to())

    def ods_stg_ad_reply_comparation(self) -> str:
        return """SELECT
                    sender_email as buyer_id_nk	
                    , min(mail_queue_id) as ad_reply_id_nk
                    , added_at as ad_reply_creation_date
                    , sender_email as email
                    , list_id as list_id_nk
                    , now() as insert_date
                    , ad_id as ad_id_nk
                    FROM stg.ad_reply
                        ,ods.ad
                    where ad_reply.ad_id = ad.ad_id_nk
                    group by sender_email, added_at, list_id, ad_id
                    order by added_at"""

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
                added_at between '{DATE_FROM}' and '{DATE_TO}'
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
	            added_at between '{DATE_FROM}' and '{DATE_TO}'
            """.format(DATE_FROM=self.params.get_date_from(),
                       DATE_TO=self.params.get_date_to(),
                       CURRENT_YEAR=self.params.get_current_year())
        return query

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
