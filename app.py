from curses.ascii import alt
from datetime import date, datetime, timedelta
from urllib.error import URLError
import pandas as pd
import numpy as np
import streamlit as st
from streamlit_option_menu import option_menu
import psycopg2
from functools import wraps
import hmac
import boto3
import json

client = boto3.client(
    "secretsmanager",
    region_name=st.secrets["AWS_DEFAULT_REGION"],
    aws_access_key_id=st.secrets["AWS_ACCESS_KEY_ID"],
    aws_secret_access_key=st.secrets["AWS_SECRET_ACCESS_KEY"]
)

def get_secret(secret_name):
    # Retrieve the secret value
    response = client.get_secret_value(SecretId=secret_name)
    return json.loads(response["SecretString"])

# Replace 'your-secret-name' with the actual secret name in AWS Secrets Manager
secret = get_secret("G-streamlit-KAT")
db = secret["db"]
name = secret["name"]
passw = secret["passw"]
server = secret["server"]
port = secret["port"]
stripe_key = secret["stripe"]


st.set_page_config( page_title = "Ads Dashboard",
    page_icon=":bar_chart:",
    layout="wide",
    initial_sidebar_state="expanded")

# Configure Streamlit to handle larger datasets
# This increases the message size limit to handle large dataframes
st.config.set_option('server.maxMessageSize', 500)

# st.toast('Successfully connected to the database!!', icon='😍')

st.write("Successfully connected to the database!")

def redshift_connection(dbname, user, password, host, port):
    def decorator(func):
        @wraps(func)
        def wrapper(*args, **kwargs):
            try:

                connection = psycopg2.connect(
                    dbname=dbname,
                    user=user,
                    password=password,
                    host=host,
                    port=port
                )

                cursor = connection.cursor()

                print("Connected to Redshift!")

                result = func(*args, connection=connection, cursor=cursor, **kwargs)

                cursor.close()
                connection.close()

                print("Disconnected from Redshift!")

                return result

            except Exception as e:
                print(f"Error: {e}")
                return None

        return wrapper

    return decorator

# query = '''
# SELECT buid,bu.name as business_name,bu.email as email,a.ad_account_id,b.currency,
# (sevend_spends) as "7d_spends",
# (current_month_spends) as current_month_spends,
# (thirtyd_spends) as "30d_spends",
# (lifetime_spends) as lifetime_spends,
# case when flag='Others' then 'Active' else COALESCE(flag,'Active') end as status,disable_date,disable_reason,
# sum(case when ad_status = 'APPROVED' then 1 else 0 end) as total_ads,
# sum(case when ad_status = 'DISAPPROVED' then 1 else 0 end) as disapproved_ads,
# sum(case when (ad_status = 'APPROVED' and date(a.created_at)>=current_date-7) then 1 else 0 end) as total_ads_last7days,
# sum(case when (ad_status = 'DISAPPROVED' and date(edited_at)>=current_date-7) then 1 else 0 end) as disapproved_ads_last7days,
# sum(case when (ad_status = 'APPROVED' and date(a.created_at)=current_date-1) then 1 else 0 end) as total_ads_yesterday,
# sum(case when (ad_status = 'DISAPPROVED' and date(edited_at)=current_date-1) then 1 else 0 end) as disapproved_ads_yesterday
# from
# (
# SELECT a.ad_account_id,a.ad_id,ad_status,effective_status,edited_at,ad_review_feedback,a.created_at
#  FROM
# (
# SELECT a.ad_account_id,ad_id,ad_status,effective_status,edited_at,a.created_at,ad_review_feedback
#  FROM
# (
# select fad.ad_account_id,ad_id, 
# case when effective_status ='DISAPPROVED' then 'DISAPPROVED' else 'APPROVED' end as ad_status,effective_status,
#  date(fad.edited_at) as edited_at, date(fad.created_date) as created_at,
# row_number() over(PARTITION by ad_id order by date(fad.edited_at) desc) as rw,ad_review_feedback
# from zocket_global.fb_ads_details_v3 fad
# join zocket_global.fb_child_ad_accounts fcaa on fad.ad_account_id = fcaa.ad_account_id
# )a
# where rw=1
# ) a
# ) a
# left JOIN
# (
#     select ad_account_id,
# SUM(CASE WHEN date(dt) > CURRENT_DATE - INTERVAL '7 DAY' THEN spend::float ELSE 0 END) AS "sevend_spends",
# SUM(CASE WHEN date(dt) > CURRENT_DATE - INTERVAL '30 DAY' THEN spend::float ELSE 0 END) AS "thirtyd_spends",
# SUM( CASE WHEN dt >= DATE_TRUNC('month', CURRENT_DATE) THEN spend::float ELSE 0 END) AS current_month_spends,
# sum(spend) as lifetime_spends from 
# (SELECT  ad_account_id,date(date_start) as dt,max(spend)spend
#     from 
#     (
#     select ad_account_id,date(date_start) as date_start,spend,'public' from ad_account_spends 
#     union ALL
#     select ad_account_id,date(date_start) as date_start,spend,'global' from zocket_global.ad_account_spends
#     )aas
# 	group by 1,2
#     ORDER by 2
#    )aas
#     --    where ad_account_id='act_621862387097953'

# group by 1
# order by 4 desc
# )s on a.ad_account_id = s.ad_account_id
# left join (
#     select ad_account_id,buid,created_at,prev_date,currency
# from
# (
#     select faa.ad_account_id,app_business_id,buid,faa.created_at,currency,
#     row_number() over(PARTITION by faa.ad_account_id order by date(faa.created_at) desc) as rw,
#      coalesce(date(lag(faa.created_at,1) over(partition by faa.ad_account_id order by date(faa.created_at) desc)),date('2099-12-31')) as prev_date

#      from zocket_global.fb_child_ad_accounts faa
#      left join zocket_global.fb_child_business_managers fcbm on faa.app_business_manager_id=fcbm.id
# left join 
#     (SELECT
#     id ,name,brand_name,json_extract_path_text(json_extract_array_element_text(business_user_ids, 0), 'role') AS role,
#     json_extract_path_text(json_extract_array_element_text(business_user_ids, 0), 'business_user_id') AS buid
# FROM
#     zocket_global.business_profile
# WHERE
#     json_extract_path_text(json_extract_array_element_text(business_user_ids, 0), 'role') = 'owner' )bp on fcbm.app_business_id=bp.id
# )
# where rw=1
# )b on 
# a.ad_account_id =b.ad_account_id
# left join zocket_global.business_users bu on b.buid=bu.id
# left JOIN (
#     select ad_account_id,disable_date,flag,disable_reason
#     FROM
#     (
#     SELECT ad_account_id,disable_date,flag,disable_reason,row_number() over(PARTITION by ad_account_id order by coalesce(date(disable_date),date('2000-01-01') )desc ) as rw
#     FROM
#     (
#     SELECT euid,ad_account_id,
# case when flag = 'Reactivated' then reactivation_date
# when flag = 'Disabled' then dt end as disable_date,
# case when flag = 'Reactivated' then dt end as reactivation_date
# ,flag
# ,currency
# ,name as ad_account_name,bm_name,
# case when disable_reason = 0 then 'NONE'
# when disable_reason = 1 then  'ADS_INTEGRITY_POLICY'
# when disable_reason = 2 then  'ADS_IP_REVIEW'
# when disable_reason = 3 then  'RISK_PAYMENT'
# when disable_reason = 4 then  'GRAY_ACCOUNT_SHUT_DOWN'
# when disable_reason = 5 then  'ADS_AFC_REVIEW'
# when disable_reason = 6 then  'BUSINESS_INTEGRITY_RAR'
# when disable_reason = 7 then  'PERMANENT_CLOSE'
# when disable_reason = 8 then  'UNUSED_RESELLER_ACCOUNT'
# when disable_reason = 9 then  'UNUSED_ACCOUNT'
# when disable_reason = 10 then  'UMBRELLA_AD_ACCOUNT'
# when disable_reason = 11 then  'BUSINESS_MANAGER_INTEGRITY_POLICY'
# when disable_reason = 12 then  'MISREPRESENTED_AD_ACCOUNT'
# when disable_reason = 13 then  'AOAB_DESHARE_LEGAL_ENTITY'
# when disable_reason = 14 then  'CTX_THREAD_REVIEW'
# when disable_reason = 15 then  'COMPROMISED_AD_ACCOUNT' end as disable_reason
# FROM
# (
# SELECT *,case when rw = 1 and prev_status !=1 and account_status = 1 then 'Reactivated' 
#             when rw = 1 and account_status != 1 then 'Disabled' else 'Others'
#             end as flag, case when rw = 1 and prev_status !=1 and account_status = 1 then prev_dt end as reactivation_date

# FROM
# (
# select coalesce(eu.euid,cast(bp.buid as int)) as euid,COALESCE(b.name,d.name)as name,a.ad_account_id,a.account_status,disable_reason,dateadd('minute',330,a.created_at) as dt,
# COALESCE(b.currency,d.currency)as currency,
# COALESCE(c.name,e.name)as bm_name,
#  row_number() over(partition by a.ad_account_id order by a.created_at desc) as rw,
#  lag(a.account_status,1) over(PARTITION by a.ad_account_id order by dateadd('minute',330,a.created_at)) as prev_status,
#  lag(dateadd('minute',330,a.created_at),1) over(PARTITION by a.ad_account_id order by dateadd('minute',330,a.created_at)) as prev_dt
# -- from "dev"."public"."ad_account_webhook" a
# from "dev"."z_b"."ad_account_webhook" a
# left join fb_ad_accounts b on a.ad_account_id = b.ad_account_id
# left join fb_business_managers c on c.id = b.app_business_manager_id
# left join zocket_global.fb_child_ad_accounts d on a.ad_account_id = d.ad_account_id
# left join zocket_global.fb_child_business_managers e on e.id = d.app_business_manager_id
# left join 
#     (SELECT
#     id ,name,brand_name,json_extract_path_text(json_extract_array_element_text(business_user_ids, 0), 'role') AS role,
#     json_extract_path_text(json_extract_array_element_text(business_user_ids, 0), 'business_user_id') AS buid
# FROM
#     zocket_global.business_profile
# WHERE
#     json_extract_path_text(json_extract_array_element_text(business_user_ids, 0), 'role') = 'owner' )bp on e.app_business_id=bp.id
# left join enterprise_users eu on c.app_business_id=eu.euid
# -- where a.ad_account_id='act_635291785907397'
# order by 3
# )
# -- where ad_account_id='act_635291785907397'
# order by ad_account_id
# )
#     )
#     )
#     where rw=1 
# )da on a.ad_account_id=da.ad_account_id
# -- where a.ad_account_id='act_621862387097953'
# group by 1,2,3,4,5,6,7,8,9,10,11,12
#     '''

# yesterday_query = '''SELECT buid,bu.name as business_name,bu.email as email,a.ad_account_id,b.currency,
# (sevend_spends) as "7d_spends",
# (current_month_spends) as current_month_spends,
# (thirtyd_spends) as "30d_spends",
# (lifetime_spends) as lifetime_spends,
# case when flag='Others' then 'Active' else COALESCE(flag,'Active') end as status,disable_date,disable_reason,
# sum(case when ad_status = 'APPROVED' then 1 else 0 end) as total_ads,
# sum(case when ad_status = 'DISAPPROVED' then 1 else 0 end) as disapproved_ads,
# sum(case when (ad_status = 'APPROVED' and date(a.created_at)>=current_date-7) then 1 else 0 end) as total_ads_last7days,
# sum(case when (ad_status = 'DISAPPROVED' and date(edited_at)>=current_date-7) then 1 else 0 end) as disapproved_ads_last7days,
# sum(case when (ad_status = 'APPROVED' and date(a.created_at)=current_date-1) then 1 else 0 end) as total_ads_yesterday,
# sum(case when (ad_status = 'DISAPPROVED' and date(edited_at)=current_date-1) then 1 else 0 end) as disapproved_ads_yesterday
# from
# (
# SELECT a.ad_account_id,a.ad_id,ad_status,effective_status,edited_at,ad_review_feedback,a.created_at
#  FROM
# (
# SELECT a.ad_account_id,ad_id,ad_status,effective_status,edited_at,a.created_at,ad_review_feedback
#  FROM
# (
# select fad.ad_account_id,ad_id, 
# case when effective_status ='DISAPPROVED' then 'DISAPPROVED' else 'APPROVED' end as ad_status,effective_status,
#  date(fad.edited_at) as edited_at, date(fad.created_date) as created_at,
# row_number() over(PARTITION by ad_id order by date(fad.edited_at) desc) as rw,ad_review_feedback
# from zocket_global.fb_ads_details_v3 fad
# join zocket_global.fb_child_ad_accounts fcaa on fad.ad_account_id = fcaa.ad_account_id
# where date(fad.edited_at)=current_date-1
# )a
# where rw=1
# ) a
# ) a
# left JOIN
# (
#     select ad_account_id,
# SUM(CASE WHEN date(dt) > CURRENT_DATE - INTERVAL '7 DAY' THEN spend::float ELSE 0 END) AS "sevend_spends",
# SUM(CASE WHEN date(dt) > CURRENT_DATE - INTERVAL '30 DAY' THEN spend::float ELSE 0 END) AS "thirtyd_spends",
# SUM( CASE WHEN dt >= DATE_TRUNC('month', CURRENT_DATE) THEN spend::float ELSE 0 END) AS current_month_spends,
# sum(spend) as lifetime_spends from 
# (SELECT  ad_account_id,date(date_start) as dt,max(spend)spend
#     from 
#     (
#     select ad_account_id,date(date_start) as date_start,spend,'public' from ad_account_spends 
#     union ALL
#     select ad_account_id,date(date_start) as date_start,spend,'global' from zocket_global.ad_account_spends
#     )aas
# 	group by 1,2
#     ORDER by 2
#    )aas
#     --    where ad_account_id='act_621862387097953'

# group by 1
# order by 4 desc
# )s on a.ad_account_id = s.ad_account_id
# left join (
#     select ad_account_id,buid,created_at,prev_date,currency
# from
# (
#     select faa.ad_account_id,app_business_id,buid,faa.created_at,currency,
#     row_number() over(PARTITION by faa.ad_account_id order by date(faa.created_at) desc) as rw,
#      coalesce(date(lag(faa.created_at,1) over(partition by faa.ad_account_id order by date(faa.created_at) desc)),date('2099-12-31')) as prev_date

#      from zocket_global.fb_child_ad_accounts faa
#      left join zocket_global.fb_child_business_managers fcbm on faa.app_business_manager_id=fcbm.id
# left join 
#     (SELECT
#     id ,name,brand_name,json_extract_path_text(json_extract_array_element_text(business_user_ids, 0), 'role') AS role,
#     json_extract_path_text(json_extract_array_element_text(business_user_ids, 0), 'business_user_id') AS buid
# FROM
#     zocket_global.business_profile
# WHERE
#     json_extract_path_text(json_extract_array_element_text(business_user_ids, 0), 'role') = 'owner' )bp on fcbm.app_business_id=bp.id
# )
# where rw=1
# )b on 
# a.ad_account_id =b.ad_account_id
# left join zocket_global.business_users bu on b.buid=bu.id
# left JOIN (
#     select ad_account_id,disable_date,flag,disable_reason
#     FROM
#     (
#     SELECT ad_account_id,disable_date,flag,disable_reason,row_number() over(PARTITION by ad_account_id order by coalesce(date(disable_date),date('2000-01-01') )desc ) as rw
#     FROM
#     (
#     SELECT euid,ad_account_id,
# case when flag = 'Reactivated' then reactivation_date
# when flag = 'Disabled' then dt end as disable_date,
# case when flag = 'Reactivated' then dt end as reactivation_date
# ,flag
# ,currency
# ,name as ad_account_name,bm_name,
# case when disable_reason = 0 then 'NONE'
# when disable_reason = 1 then  'ADS_INTEGRITY_POLICY'
# when disable_reason = 2 then  'ADS_IP_REVIEW'
# when disable_reason = 3 then  'RISK_PAYMENT'
# when disable_reason = 4 then  'GRAY_ACCOUNT_SHUT_DOWN'
# when disable_reason = 5 then  'ADS_AFC_REVIEW'
# when disable_reason = 6 then  'BUSINESS_INTEGRITY_RAR'
# when disable_reason = 7 then  'PERMANENT_CLOSE'
# when disable_reason = 8 then  'UNUSED_RESELLER_ACCOUNT'
# when disable_reason = 9 then  'UNUSED_ACCOUNT'
# when disable_reason = 10 then  'UMBRELLA_AD_ACCOUNT'
# when disable_reason = 11 then  'BUSINESS_MANAGER_INTEGRITY_POLICY'
# when disable_reason = 12 then  'MISREPRESENTED_AD_ACCOUNT'
# when disable_reason = 13 then  'AOAB_DESHARE_LEGAL_ENTITY'
# when disable_reason = 14 then  'CTX_THREAD_REVIEW'
# when disable_reason = 15 then  'COMPROMISED_AD_ACCOUNT' end as disable_reason
# FROM
# (
# SELECT *,case when rw = 1 and prev_status !=1 and account_status = 1 then 'Reactivated' 
#             when rw = 1 and account_status != 1 then 'Disabled' else 'Others'
#             end as flag, case when rw = 1 and prev_status !=1 and account_status = 1 then prev_dt end as reactivation_date

# FROM
# (
# select coalesce(eu.euid,cast(bp.buid as int)) as euid,COALESCE(b.name,d.name)as name,a.ad_account_id,a.account_status,disable_reason,dateadd('minute',330,a.created_at) as dt,
# COALESCE(b.currency,d.currency)as currency,
# COALESCE(c.name,e.name)as bm_name,
#  row_number() over(partition by a.ad_account_id order by a.created_at desc) as rw,
#  lag(a.account_status,1) over(PARTITION by a.ad_account_id order by dateadd('minute',330,a.created_at)) as prev_status,
#  lag(dateadd('minute',330,a.created_at),1) over(PARTITION by a.ad_account_id order by dateadd('minute',330,a.created_at)) as prev_dt
# -- from "dev"."public"."ad_account_webhook" a
# from "dev"."z_b"."ad_account_webhook" a
# left join fb_ad_accounts b on a.ad_account_id = b.ad_account_id
# left join fb_business_managers c on c.id = b.app_business_manager_id
# left join zocket_global.fb_child_ad_accounts d on a.ad_account_id = d.ad_account_id
# left join zocket_global.fb_child_business_managers e on e.id = d.app_business_manager_id
# left join 
#     (SELECT
#     id ,name,brand_name,json_extract_path_text(json_extract_array_element_text(business_user_ids, 0), 'role') AS role,
#     json_extract_path_text(json_extract_array_element_text(business_user_ids, 0), 'business_user_id') AS buid
# FROM
#     zocket_global.business_profile
# WHERE
#     json_extract_path_text(json_extract_array_element_text(business_user_ids, 0), 'role') = 'owner' )bp on e.app_business_id=bp.id
# left join enterprise_users eu on c.app_business_id=eu.euid
# -- where a.ad_account_id='act_635291785907397'
# order by 3
# )
# -- where ad_account_id='act_635291785907397'
# order by ad_account_id
# )
#     )
#     )
#     where rw=1 
# )da on a.ad_account_id=da.ad_account_id
# -- where a.ad_account_id='act_621862387097953'
# group by 1,2,3,4,5,6,7,8,9,10,11,12
# '''


ads_data_query = '''

SELECT a.ad_account_id,a.ad_id,ad_status,effective_status,a.created_at,edited_at as status_change_date,error_type,error_description
 FROM
(
SELECT a.ad_account_id,ad_id,ad_status,effective_status,edited_at,a.created_at,ad_review_feedback,error_description,error_type
 FROM
(SELECT 
  fad.ad_account_id,
  ad_id,
  CASE 
    WHEN effective_status = 'DISAPPROVED' THEN 'DISAPPROVED' 
    ELSE 'APPROVED' 
  END AS ad_status,
  effective_status,
  DATE(fad.edited_at) AS edited_at,
  DATE(fad.created_date) AS created_at,

  -- Remove curly braces safely
  SPLIT_PART(
    REPLACE(REPLACE(JSON_EXTRACT_PATH_TEXT(ad_review_feedback, 'global'), '{', ''), '}', ''),
    '=',
    1
  ) AS error_type,

  LTRIM(
    SPLIT_PART(
      REPLACE(REPLACE(JSON_EXTRACT_PATH_TEXT(ad_review_feedback, 'global'), '{', ''), '}', ''),
      '=',
      2
    )
  ) AS error_description,

  ROW_NUMBER() OVER (PARTITION BY ad_id ORDER BY DATE(fad.edited_at) DESC) AS rw,
  ad_review_feedback

FROM zocket_global.fb_ads_details_v3 fad
JOIN zocket_global.fb_child_ad_accounts fcaa 
  ON fad.ad_account_id = fcaa.ad_account_id
)a
where rw=1
) a
where status_change_date>='2025-01-01'
'''

# @st.cache_data(ttl=36400)  # 86400 seconds = 24 hours
@redshift_connection(db,name,passw,server,port)
def execute_query(connection, cursor,query):

    cursor.execute(query)
    column_names = [desc[0] for desc in cursor.description]
    result = pd.DataFrame(cursor.fetchall(), columns=column_names)

    return result

# df = execute_query(query=query)
# df = execute_query(query=query)
# df_yesterday = execute_query(query=yesterday_query)
df_ads = execute_query(query=ads_data_query)

# Function to generate Facebook Ads Manager URL
def generate_ad_link(ad_account_id, ad_id):
    """
    Generate Facebook Ads Manager URL for a specific ad.
    Removes 'act_' prefix from ad_account_id and uses the ad_id directly.
    """
    # Remove 'act_' prefix if it exists
    clean_account_id = ad_account_id.replace('act_', '') if ad_account_id.startswith('act_') else ad_account_id
    
    # Generate the URL
    url = f"https://adsmanager.facebook.com/adsmanager/manage/ads/edit/standalone?act={clean_account_id}&columns=name%2Cdelivery%2Crecommendations_guidance%2Ccampaign_name%2Cbid%2Cbudget%2Clast_significant_edit%2Cattribution_setting%2Cresults%2Creach%2Cimpressions%2Ccost_per_result%2Cquality_score_organic%2Cquality_score_ectr%2Cquality_score_ecvr%2Cspend%2Cend_time%2Cschedule%2Ccpm%2Cpurchase_roas%3Aomni_purchase%2Cfrequency%2Cactions%3Aomni_purchase%2Ccreated_time&attribution_windows=default&filter_set=CAMPAIGN_DELIVERY_STATUS-STRING_SET%1EIN%1E[%22active%22%2C%22draft%22%2C%22pending%22%2C%22inactive%22%2C%22error%22%2C%22deleted%22%2C%22completed%22%2C%22off%22]%1DCAMPAIGN_GROUP_DELIVERY_STATUS-STRING_SET%1EIN%1E[%22active%22%2C%22draft%22%2C%22pending%22%2C%22inactive%22%2C%22error%22%2C%22deleted%22%2C%22completed%22%2C%22off%22]%1DADGROUP_DELIVERY_STATUS-STRING_SET%1EIN%1E[%22active%22%2C%22draft%22%2C%22pending%22%2C%22inactive%22%2C%22error%22%2C%22deleted%22%2C%22completed%22%2C%22off%22]&selected_ad_ids={ad_id}&sort=created_time~0&current_step=0&ads_manager_write_regions=true&nav_source=no_referrer#"
    return url

# Add ad_link column to df_ads
if not df_ads.empty and 'ad_account_id' in df_ads.columns and 'ad_id' in df_ads.columns:
    df_ads['ad_link'] = df_ads.apply(lambda row: generate_ad_link(row['ad_account_id'], row['ad_id']), axis=1)

# df['Disapproved_Percentage'] = df['disapproved_ads'] / df['total_ads']
# df['Disapproved_Percentage'] = df['Disapproved_Percentage'].fillna(0)
# df['Disapproved_Percentage'] = df['Disapproved_Percentage'].apply(lambda x: "0%" if pd.isna(x) or np.isinf(x) or x == 0 else f"{round(x*100):.0f}%")


# df['7d_Disapproved_Percentage'] = df['disapproved_ads_last7days'] / df['total_ads_last7days']
# df['7d_Disapproved_Percentage'] = df['7d_Disapproved_Percentage'].fillna(0)
# df['7d_Disapproved_Percentage'] = df['7d_Disapproved_Percentage'].apply(lambda x: "0%" if pd.isna(x) or np.isinf(x) or x == 0 else f"{round(x*100):.0f}%")


# df_yesterday['Disapproved_Percentage'] = df_yesterday['disapproved_ads'] / df_yesterday['total_ads']
# df_yesterday['Disapproved_Percentage'] = df_yesterday['Disapproved_Percentage'].fillna(0)
# df_yesterday['Disapproved_Percentage'] = df_yesterday['Disapproved_Percentage'].apply(lambda x: "0%" if pd.isna(x) or np.isinf(x) or x == 0 else f"{round(x*100):.0f}%")

# st.title("Disapproved Ads Stats")


# st.title("Yesterday Disapproved Ads Stats")


# st.dataframe(df_yesterday[['buid','business_name','email','ad_account_id','currency','status','disable_date','disable_reason','total_ads','disapproved_ads','Disapproved_Percentage','7d_spends','current_month_spends','30d_spends','lifetime_spends']],use_container_width=True)



# st.title("Overall Disapproved Ads Stats")

# st.dataframe(df[['buid','business_name','email','ad_account_id','currency','status','disable_date','disable_reason','total_ads','disapproved_ads','Disapproved_Percentage','total_ads_last7days','disapproved_ads_last7days','7d_Disapproved_Percentage','total_ads_yesterday','disapproved_ads_yesterday','7d_spends','current_month_spends','30d_spends','lifetime_spends']],use_container_width=True)

# st.dataframe(df_yesterday,use_container_width=True)

# Create navigation sidebar
with st.sidebar:
    st.title("📊 Ads Dashboard")
    page = st.selectbox(
        "Navigate to:",
        ["Main Stats", "Raw Dump"],
        index=0
    )

# Create filter widgets for the ads data
st.subheader("Ads Data Filters")

# Arrange filters in 2 columns
col1, col2 = st.columns(2)

with col1:
    # Ad Account Filter
    ad_account_id = st.text_input("Enter Ad Account ID", placeholder="e.g., act_123456789")
    
    # Date Filter (for 'created_at' ideally, as that's usually present)
    date_min = df_ads['created_at'].min() if 'created_at' in df_ads.columns else None
    date_max = df_ads['created_at'].max() if 'created_at' in df_ads.columns else None
    if date_min is not None and date_max is not None:
        date_range = st.date_input(
            "Created At Date Range",
            value=(date_min, date_max),
            min_value=date_min,
            max_value=date_max
        )
    else:
        date_range = None
    
    # Main Status Filter (formerly Ad Status)
    ad_status_options = df_ads['ad_status'].dropna().unique() if 'ad_status' in df_ads.columns else []
    selected_ad_status = st.multiselect("Main Status", options=ad_status_options)

with col2:
    # Status Change Date Filter
    edited_min = df_ads['status_change_date'].min() if 'status_change_date' in df_ads.columns else None
    edited_max = df_ads['status_change_date'].max() if 'status_change_date' in df_ads.columns else None
    if edited_min is not None and edited_max is not None:
        edited_range = st.date_input(
            "Status Change Date Range",
            value=(edited_min, edited_max),
            min_value=edited_min,
            max_value=edited_max,
            key="status_change_date_range"
        )
    else:
        edited_range = None
    
    # Sub Status Filter (formerly Effective Status)
    effective_status_options = df_ads['effective_status'].dropna().unique() if 'effective_status' in df_ads.columns else []
    selected_effective_status = st.multiselect("Sub Status", options=effective_status_options)
    
    # Error Type Filter
    error_type_options = df_ads['error_type'].dropna().unique() if 'error_type' in df_ads.columns else []
    selected_error_type = st.multiselect("Error Type", options=error_type_options)

# Apply filters
filtered_df_ads = df_ads.copy()

# Ad Account ID validation and filtering
if ad_account_id and 'ad_account_id' in df_ads.columns:
    # Check if the entered ad account ID exists in the data
    available_accounts = df_ads['ad_account_id'].dropna().unique()
    if ad_account_id not in available_accounts:
        st.error(f"Ad Account ID '{ad_account_id}' does not exist. Please check the account ID and try again.")
        st.info(f"Available account IDs: {', '.join(available_accounts[:10])}{'...' if len(available_accounts) > 10 else ''}")
        filtered_df_ads = pd.DataFrame()  # Empty dataframe to show no results
    else:
        filtered_df_ads = filtered_df_ads[filtered_df_ads['ad_account_id'] == ad_account_id]

if date_range and 'created_at' in df_ads.columns and not filtered_df_ads.empty:
    start, end = date_range
    filtered_df_ads = filtered_df_ads[
        (filtered_df_ads['created_at'] >= start) &
        (filtered_df_ads['created_at'] <= end)
    ]

if edited_range and 'status_change_date' in df_ads.columns and not filtered_df_ads.empty:
    start, end = edited_range
    filtered_df_ads = filtered_df_ads[
        (filtered_df_ads['status_change_date'] >= start) &
        (filtered_df_ads['status_change_date'] <= end)
    ]

if selected_ad_status and 'ad_status' in df_ads.columns and not filtered_df_ads.empty:
    filtered_df_ads = filtered_df_ads[filtered_df_ads['ad_status'].isin(selected_ad_status)]

if selected_effective_status and 'effective_status' in df_ads.columns and not filtered_df_ads.empty:
    filtered_df_ads = filtered_df_ads[filtered_df_ads['effective_status'].isin(selected_effective_status)]

if selected_error_type and 'error_type' in df_ads.columns and not filtered_df_ads.empty:
    filtered_df_ads = filtered_df_ads[filtered_df_ads['error_type'].isin(selected_error_type)]

# Main Stats Section
if page == "Main Stats":
    st.title("📊 Main Stats")
    
    # Overview Section (Independent of filters)
    st.subheader("📊 Overview")

    if not df_ads.empty:
        # Calculate date ranges
        today = pd.Timestamp.now().date()
        yesterday = today - pd.Timedelta(days=1)
        thirty_days_ago = today - pd.Timedelta(days=30)
        current_month_start = pd.Timestamp.now().replace(day=1).date()
        
        # Convert date columns to datetime for comparison
        df_ads_copy = df_ads.copy()
        if 'created_at' in df_ads_copy.columns:
            df_ads_copy['created_at'] = pd.to_datetime(df_ads_copy['created_at'])
        if 'status_change_date' in df_ads_copy.columns:
            df_ads_copy['status_change_date'] = pd.to_datetime(df_ads_copy['status_change_date'])
        
        # Calculate metrics
        total_ads = len(df_ads)
        total_rejected = len(df_ads[df_ads['ad_status'] == 'DISAPPROVED'])
        
        # Yesterday metrics
        yesterday_ads = len(df_ads_copy[
            (df_ads_copy['created_at'].dt.date == yesterday) 
        ])
        yesterday_rejected = len(df_ads_copy[
            (df_ads_copy['ad_status'] == 'DISAPPROVED') &
             (df_ads_copy['status_change_date'].dt.date == yesterday)
        ])
        
        # Current month metrics
        current_month_ads = len(df_ads_copy[
            df_ads_copy['created_at'].dt.date >= current_month_start
        ])
        current_month_rejected = len(df_ads_copy[
            (df_ads_copy['ad_status'] == 'DISAPPROVED') &
            (df_ads_copy['status_change_date'].dt.date >= current_month_start)
        ])
        
        # Last 30 days metrics
        last_30_days_ads = len(df_ads_copy[
            df_ads_copy['created_at'].dt.date >= thirty_days_ago
        ])
        last_30_days_rejected = len(df_ads_copy[
            (df_ads_copy['ad_status'] == 'DISAPPROVED') &
            (df_ads_copy['status_change_date'].dt.date >= thirty_days_ago)
        ])
        
        # Top rejected account yesterday
        yesterday_rejected_by_account = df_ads_copy[
            (df_ads_copy['ad_status'] == 'DISAPPROVED') &
             (df_ads_copy['status_change_date'].dt.date == yesterday)
        ].groupby('ad_account_id').size().sort_values(ascending=False)
        
        top_rejected_account_yesterday = yesterday_rejected_by_account.index[0] if len(yesterday_rejected_by_account) > 0 else "None"
        top_rejected_count_yesterday = yesterday_rejected_by_account.iloc[0] if len(yesterday_rejected_by_account) > 0 else 0
        
        # Display metrics in columns
        col1, col2, col3, col4 = st.columns(4)
        
        with col1:
            st.metric("Total Ads", total_ads)
            st.metric("Total Rejected", total_rejected)
        
        with col2:
            st.metric("Yesterday Ads", yesterday_ads)
            st.metric("Yesterday Rejected", yesterday_rejected)
        
        with col3:
            st.metric("Current Month Ads", current_month_ads)
            st.metric("Current Month Rejected", current_month_rejected)
        
        with col4:
            st.metric("Last 30 Days Ads", last_30_days_ads)
            st.metric("Last 30 Days Rejected", last_30_days_rejected)
        
        # Top rejected account info
        st.info(f"🔴 **Top Rejected Account Yesterday:** {top_rejected_account_yesterday} ({top_rejected_count_yesterday} rejections)")
        
    else:
        st.warning("No data available to display overview.")

    # Filtered Overview Section (shows stats for filtered data)
    # Check if any filters are applied (excluding default date ranges)
    filters_applied = (
        ad_account_id or 
        selected_ad_status or 
        selected_effective_status or 
        selected_error_type or
        (date_range and date_range != (date_min, date_max)) or
        (edited_range and edited_range != (edited_min, edited_max))
    )

    if filters_applied and not filtered_df_ads.empty:
        st.subheader("🔍 Filtered Overview")
        
        # Calculate date ranges
        today = pd.Timestamp.now().date()
        yesterday = today - pd.Timedelta(days=1)
        thirty_days_ago = today - pd.Timedelta(days=30)
        current_month_start = pd.Timestamp.now().replace(day=1).date()
        
        # Convert date columns to datetime for comparison
        filtered_df_ads_copy = filtered_df_ads.copy()
        if 'created_at' in filtered_df_ads_copy.columns:
            filtered_df_ads_copy['created_at'] = pd.to_datetime(filtered_df_ads_copy['created_at'])
        if 'status_change_date' in filtered_df_ads_copy.columns:
            filtered_df_ads_copy['status_change_date'] = pd.to_datetime(filtered_df_ads_copy['status_change_date'])
        
        # Calculate filtered metrics
        filtered_total_ads = len(filtered_df_ads)
        filtered_total_rejected = len(filtered_df_ads[filtered_df_ads['ad_status'] == 'DISAPPROVED'])
        
        # Yesterday metrics for filtered data
        filtered_yesterday_ads = len(filtered_df_ads_copy[
            (filtered_df_ads_copy['created_at'].dt.date == yesterday)
        ])
        filtered_yesterday_rejected = len(filtered_df_ads_copy[
            (filtered_df_ads_copy['ad_status'] == 'DISAPPROVED') &
             (filtered_df_ads_copy['status_change_date'].dt.date == yesterday)
        ])
        
        # Current month metrics for filtered data
        filtered_current_month_ads = len(filtered_df_ads_copy[
            filtered_df_ads_copy['created_at'].dt.date >= current_month_start
        ])
        filtered_current_month_rejected = len(filtered_df_ads_copy[
            (filtered_df_ads_copy['ad_status'] == 'DISAPPROVED') &
            (filtered_df_ads_copy['status_change_date'].dt.date >= current_month_start)
        ])
        
        # Last 30 days metrics for filtered data
        filtered_last_30_days_ads = len(filtered_df_ads_copy[
            filtered_df_ads_copy['created_at'].dt.date >= thirty_days_ago
        ])
        filtered_last_30_days_rejected = len(filtered_df_ads_copy[
            (filtered_df_ads_copy['ad_status'] == 'DISAPPROVED') &
            (filtered_df_ads_copy['status_change_date'].dt.date >= thirty_days_ago)
        ])
        
        # Top rejected account yesterday for filtered data
        filtered_yesterday_rejected_by_account = filtered_df_ads_copy[
            (filtered_df_ads_copy['ad_status'] == 'DISAPPROVED') &
             (filtered_df_ads_copy['status_change_date'].dt.date == yesterday)
        ].groupby('ad_account_id').size().sort_values(ascending=False)
        
        filtered_top_rejected_account_yesterday = filtered_yesterday_rejected_by_account.index[0] if len(filtered_yesterday_rejected_by_account) > 0 else "None"
        filtered_top_rejected_count_yesterday = filtered_yesterday_rejected_by_account.iloc[0] if len(filtered_yesterday_rejected_by_account) > 0 else 0
        
        # Display filtered metrics in columns
        col1, col2, col3, col4 = st.columns(4)
        
        with col1:
            st.metric("Filtered Total Ads", filtered_total_ads)
            st.metric("Filtered Total Rejected", filtered_total_rejected)
        
        with col2:
            st.metric("Filtered Yesterday Ads", filtered_yesterday_ads)
            st.metric("Filtered Yesterday Rejected", filtered_yesterday_rejected)
        
        with col3:
            st.metric("Filtered Current Month Ads", filtered_current_month_ads)
            st.metric("Filtered Current Month Rejected", filtered_current_month_rejected)
        
        with col4:
            st.metric("Filtered Last 30 Days Ads", filtered_last_30_days_ads)
            st.metric("Filtered Last 30 Days Rejected", filtered_last_30_days_rejected)
        
        # Top rejected account info for filtered data
        st.info(f"🔴 **Top Rejected Account Yesterday (Filtered):** {filtered_top_rejected_account_yesterday} ({filtered_top_rejected_count_yesterday} rejections)")
        
        # Show percentage of filtered data vs total data
        if not df_ads.empty:
            total_ads_count = len(df_ads)
            filtered_percentage = (filtered_total_ads / total_ads_count) * 100 if total_ads_count > 0 else 0
            st.success(f"📊 **Filtered data represents {filtered_percentage:.1f}% of total ads**")

    # Grouped Tables Section
    st.subheader("📈 Grouped Analysis")
    
    # Arrange tables side by side using Streamlit columns
    col1, col2 = st.columns(2)

    with col1:
        # Grouped data by status_change_date of disapproved ads, sorted by status_change_date desc
        grouped_created_at = (
            filtered_df_ads[filtered_df_ads['ad_status'] == 'DISAPPROVED']
            .groupby(['status_change_date'])
            .agg(no_of_ads=('ad_id', 'count'))
            .sort_index(ascending=False)
        )
        st.write("Disapproved Ads Count by Status Change Date")
        st.dataframe(grouped_created_at, use_container_width=True)

    with col2:
        # Grouped data by status_change_date and error_type of disapproved ads, sorted by status_change_date desc
        grouped_created_at_error = (
            filtered_df_ads[filtered_df_ads['ad_status'] == 'DISAPPROVED']
            .groupby(['status_change_date', 'error_type'])
            .agg(no_of_ads=('ad_id', 'count'))
            .sort_index(level='status_change_date', ascending=False)
        )
        st.write("Disapproved Ads Count by Status Change Date and Error Type")
        st.dataframe(grouped_created_at_error, use_container_width=True)

    # Arrange grouped tables by ad_account_id and (ad_account_id, error_type) side by side
    col3, col4 = st.columns(2)

    with col3:
        # Grouped data by ad_account_id of disapproved ads
        grouped_df_ads_account = (
            filtered_df_ads[filtered_df_ads['ad_status'] == 'DISAPPROVED']
            .groupby(['ad_account_id'])
            .agg(no_of_ads=('ad_id', 'count'))
        )
        st.write("Disapproved Ads Count by Ad Account ID")
        st.dataframe(grouped_df_ads_account, use_container_width=True)

    with col4:
        # Grouped data by ad_account_id and error_type of disapproved ads
        grouped_df_ads_account_error = (
            filtered_df_ads[filtered_df_ads['ad_status'] == 'DISAPPROVED']
            .groupby(['ad_account_id', 'error_type'])
            .agg(no_of_ads=('ad_id', 'count'))
        )
        st.write("Disapproved Ads Count by Ad Account ID and Error Type")
        st.dataframe(grouped_df_ads_account_error, use_container_width=True)

    # Show two tables side by side:
    # 1. Left: By ad_account_id and status_change_date (total disapproved ads per account per date)
    # 2. Right: By ad_account_id, status_change_date, and error_type (total disapproved ads per account, date, and error type)

    grouped_df_ads_account_created_at = (
        filtered_df_ads[filtered_df_ads['ad_status'] == 'DISAPPROVED']
        .groupby(['ad_account_id', 'status_change_date'])
        .agg(no_of_ads=('ad_id', 'count'))
        .sort_index(level='status_change_date', ascending=False)
    )

    grouped_df_ads_account_created_at_error = (
        filtered_df_ads[filtered_df_ads['ad_status'] == 'DISAPPROVED']
        .groupby(['ad_account_id', 'status_change_date', 'error_type'])
        .agg(no_of_ads=('ad_id', 'count'))
        .sort_index(level='status_change_date', ascending=False)
    )

    col5, col6 = st.columns(2)

    with col5:
        st.write("Disapproved Ads Count by Ad Account ID and Status Change Date")
        st.dataframe(grouped_df_ads_account_created_at, use_container_width=True)

    with col6:
        st.write("Disapproved Ads Count by Ad Account ID, Status Change Date and Error Type")
        st.dataframe(grouped_df_ads_account_created_at_error, use_container_width=True)

# Raw Dump Section
elif page == "Raw Dump":
    st.title("📋 Raw Dump")
    
    # Sort by status_change_date descending before displaying, if column exists
    if 'created_at' in filtered_df_ads.columns:
        # Configure column to display ad_link as clickable links
        column_config = {
            "ad_link": st.column_config.LinkColumn(
                "Ad_Link",
                help="Click to open in Facebook Ads Manager",
                display_text="Ad_Link"
            )
        }
        st.dataframe(
            filtered_df_ads.sort_values('created_at', ascending=False), 
            use_container_width=True,
            column_config=column_config
        )
    else:
        # Configure column to display ad_link as clickable links
        column_config = {
            "ad_link": st.column_config.LinkColumn(
                "Ad_Link",
                help="Click to open in Facebook Ads Manager",
                display_text="Ad_Link"
            )
        }
        st.dataframe(
            filtered_df_ads, 
            use_container_width=True,
            column_config=column_config
        )
