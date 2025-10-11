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

query = '''
SELECT buid,bu.name as business_name,bu.email as email,a.ad_account_id,b.currency,
case when flag='Others' then 'Active' else COALESCE(flag,'Active') end as status,disable_date,disable_reason,
sum(case when ad_status = 'APPROVED' then 1 else 0 end) as total_ads,
sum(case when ad_status = 'DISAPPROVED' then 1 else 0 end) as disapproved_ads,
sum(case when (ad_status = 'APPROVED' and date(status_date)>=current_date-7) then 1 else 0 end) as total_ads_last7days,
sum(case when (ad_status = 'DISAPPROVED' and date(status_date)>=current_date-7) then 1 else 0 end) as disapproved_ads_last7days,
sum(sevend_spends) as "7d_spends",
sum(current_month_spends) as current_month_spends,
sum(thirtyd_spends) as "30d_spends",
sum(lifetime_spends) as lifetime_spends
from
(
SELECT a.ad_account_id,a.ad_id,ad_status,effective_status,status_date,ad_review_feedback
 FROM
(
SELECT a.ad_account_id,ad_id,ad_status,effective_status,status_date,ad_review_feedback
 FROM
(
select fad.ad_account_id,ad_id, 
case when effective_status ='DISAPPROVED' then 'DISAPPROVED' else 'APPROVED' end as ad_status,effective_status,
 date(fad.updated_at) as status_date,
row_number() over(PARTITION by ad_id order by date(fad.updated_at) desc) as rw,ad_review_feedback
from zocket_global.fb_ads_details_v3 fad
join zocket_global.fb_child_ad_accounts fcaa on fad.ad_account_id = fcaa.ad_account_id
)a
where rw=1
) a
) a
left JOIN
(select ad_account_id,
SUM(CASE WHEN date(date_start) > CURRENT_DATE - INTERVAL '7 DAY' THEN spend::float ELSE 0 END) AS "sevend_spends",
SUM(CASE WHEN date(date_start) > CURRENT_DATE - INTERVAL '30 DAY' THEN spend::float ELSE 0 END) AS "thirtyd_spends",
SUM( CASE WHEN date_start >= DATE_TRUNC('month', CURRENT_DATE) THEN spend::float ELSE 0 END) AS current_month_spends,
sum(spend) as lifetime_spends from zocket_global.ad_account_spends aas
group by 1
order by 4 desc
)s on a.ad_account_id = s.ad_account_id
left join (
    select ad_account_id,buid,created_at,prev_date,currency
from
(
    select faa.ad_account_id,app_business_id,buid,faa.created_at,currency,
    row_number() over(PARTITION by faa.ad_account_id order by date(faa.created_at) desc) as rw,
     coalesce(date(lag(faa.created_at,1) over(partition by faa.ad_account_id order by date(faa.created_at) desc)),date('2099-12-31')) as prev_date

     from zocket_global.fb_child_ad_accounts faa
     left join zocket_global.fb_child_business_managers fcbm on faa.app_business_manager_id=fcbm.id
left join 
    (SELECT
    id ,name,brand_name,json_extract_path_text(json_extract_array_element_text(business_user_ids, 0), 'role') AS role,
    json_extract_path_text(json_extract_array_element_text(business_user_ids, 0), 'business_user_id') AS buid
FROM
    zocket_global.business_profile
WHERE
    json_extract_path_text(json_extract_array_element_text(business_user_ids, 0), 'role') = 'owner' )bp on fcbm.app_business_id=bp.id
)
where rw=1
)b on 
a.ad_account_id =b.ad_account_id
left join zocket_global.business_users bu on b.buid=bu.id
left JOIN (
    select ad_account_id,disable_date,flag,disable_reason
    FROM
    (
    SELECT ad_account_id,disable_date,flag,disable_reason,row_number() over(PARTITION by ad_account_id order by coalesce(date(disable_date),date('2000-01-01') )desc ) as rw
    FROM
    (
    SELECT euid,ad_account_id,
case when flag = 'Reactivated' then reactivation_date
when flag = 'Disabled' then dt end as disable_date,
case when flag = 'Reactivated' then dt end as reactivation_date
,flag
,currency
,name as ad_account_name,bm_name,
case when disable_reason = 0 then 'NONE'
when disable_reason = 1 then  'ADS_INTEGRITY_POLICY'
when disable_reason = 2 then  'ADS_IP_REVIEW'
when disable_reason = 3 then  'RISK_PAYMENT'
when disable_reason = 4 then  'GRAY_ACCOUNT_SHUT_DOWN'
when disable_reason = 5 then  'ADS_AFC_REVIEW'
when disable_reason = 6 then  'BUSINESS_INTEGRITY_RAR'
when disable_reason = 7 then  'PERMANENT_CLOSE'
when disable_reason = 8 then  'UNUSED_RESELLER_ACCOUNT'
when disable_reason = 9 then  'UNUSED_ACCOUNT'
when disable_reason = 10 then  'UMBRELLA_AD_ACCOUNT'
when disable_reason = 11 then  'BUSINESS_MANAGER_INTEGRITY_POLICY'
when disable_reason = 12 then  'MISREPRESENTED_AD_ACCOUNT'
when disable_reason = 13 then  'AOAB_DESHARE_LEGAL_ENTITY'
when disable_reason = 14 then  'CTX_THREAD_REVIEW'
when disable_reason = 15 then  'COMPROMISED_AD_ACCOUNT' end as disable_reason
FROM
(
SELECT *,case when rw = 1 and prev_status !=1 and account_status = 1 then 'Reactivated' 
            when rw = 1 and account_status != 1 then 'Disabled' else 'Others'
            end as flag, case when rw = 1 and prev_status !=1 and account_status = 1 then prev_dt end as reactivation_date

FROM
(
select coalesce(eu.euid,cast(bp.buid as int)) as euid,COALESCE(b.name,d.name)as name,a.ad_account_id,a.account_status,disable_reason,dateadd('minute',330,a.created_at) as dt,
COALESCE(b.currency,d.currency)as currency,
COALESCE(c.name,e.name)as bm_name,
 row_number() over(partition by a.ad_account_id order by a.created_at desc) as rw,
 lag(a.account_status,1) over(PARTITION by a.ad_account_id order by dateadd('minute',330,a.created_at)) as prev_status,
 lag(dateadd('minute',330,a.created_at),1) over(PARTITION by a.ad_account_id order by dateadd('minute',330,a.created_at)) as prev_dt
-- from "dev"."public"."ad_account_webhook" a
from "dev"."z_b"."ad_account_webhook" a
left join fb_ad_accounts b on a.ad_account_id = b.ad_account_id
left join fb_business_managers c on c.id = b.app_business_manager_id
left join zocket_global.fb_child_ad_accounts d on a.ad_account_id = d.ad_account_id
left join zocket_global.fb_child_business_managers e on e.id = d.app_business_manager_id
left join 
    (SELECT
    id ,name,brand_name,json_extract_path_text(json_extract_array_element_text(business_user_ids, 0), 'role') AS role,
    json_extract_path_text(json_extract_array_element_text(business_user_ids, 0), 'business_user_id') AS buid
FROM
    zocket_global.business_profile
WHERE
    json_extract_path_text(json_extract_array_element_text(business_user_ids, 0), 'role') = 'owner' )bp on e.app_business_id=bp.id
left join enterprise_users eu on c.app_business_id=eu.euid
-- where a.ad_account_id='act_635291785907397'
order by 3
)
-- where ad_account_id='act_635291785907397'
order by ad_account_id
)
    )
    )
    where rw=1 
)da on a.ad_account_id=da.ad_account_id
-- where a.ad_account_id='act_1808607866379064'
group by 1,2,3,4,5,6,7,8
    '''


@st.cache_data(ttl=36400)  # 86400 seconds = 24 hours
@redshift_connection(db,name,passw,server,port)
def execute_query(connection, cursor,query):

    cursor.execute(query)
    column_names = [desc[0] for desc in cursor.description]
    result = pd.DataFrame(cursor.fetchall(), columns=column_names)

    return result

# df = execute_query(query=query)
df = execute_query(query=query)


df['Disapproved_Percentage'] = df['disapproved_ads'] / df['total_ads']
df['Disapproved_Percentage'] = df['Disapproved_Percentage'].fillna(0)
df['Disapproved_Percentage'] = df['Disapproved_Percentage'].apply(lambda x: "0%" if pd.isna(x) or np.isinf(x) or x == 0 else f"{round(x*100):.0f}%")


df['7d_Disapproved_Percentage'] = df['disapproved_ads_last7days'] / df['total_ads_last7days']
df['7d_Disapproved_Percentage'] = df['7d_Disapproved_Percentage'].fillna(0)
df['7d_Disapproved_Percentage'] = df['7d_Disapproved_Percentage'].apply(lambda x: "0%" if pd.isna(x) or np.isinf(x) or x == 0 else f"{round(x*100):.0f}%")


st.title("Disapproved Ads Stats")

st.dataframe(df[['business_name','email','ad_account_id','status','currency','disable_date','disable_reason','total_ads','disapproved_ads','Disapproved_Percentage','total_ads_last7days','disapproved_ads_last7days','7d_Disapproved_Percentage','7d_spends','current_month_spends','30d_spends','lifetime_spends']],use_container_width=True)