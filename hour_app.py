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
import logging
import os

# Suppress Streamlit internal logs
logging.getLogger("streamlit").setLevel(logging.ERROR)
logging.getLogger("streamlit.web").setLevel(logging.ERROR)
logging.getLogger("streamlit.runtime").setLevel(logging.ERROR)
logging.getLogger("streamlit.runtime.scriptrunner").setLevel(logging.ERROR)

# Suppress other common noisy loggers
logging.getLogger("urllib3").setLevel(logging.ERROR)
logging.getLogger("requests").setLevel(logging.ERROR)
logging.getLogger("boto3").setLevel(logging.ERROR)
logging.getLogger("botocore").setLevel(logging.ERROR)

# Set environment variable to suppress Streamlit logs
os.environ["STREAMLIT_LOGGER_LEVEL"] = "error"

def get_aws_client():
    """Get AWS client with proper error handling for Streamlit context"""
    try:
        return boto3.client(
            "secretsmanager",
            region_name=st.secrets["AWS_DEFAULT_REGION"],
            aws_access_key_id=st.secrets["AWS_ACCESS_KEY_ID"],
            aws_secret_access_key=st.secrets["AWS_SECRET_ACCESS_KEY"]
        )
    except Exception as e:
        st.error(f"Error accessing AWS secrets: {str(e)}")
        return None

def get_secret(secret_name):
    """Retrieve secret value from AWS Secrets Manager"""
    client = get_aws_client()
    if client is None:
        return None
    
    try:
        response = client.get_secret_value(SecretId=secret_name)
        return json.loads(response["SecretString"])
    except Exception as e:
        st.error(f"Error retrieving secret '{secret_name}': {str(e)}")
        return None

# Initialize secrets with error handling
secret = get_secret("G-streamlit-KAT")
if secret is None:
    st.error("Failed to retrieve database secrets. Please check your AWS configuration.")
    st.stop()

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

# Configure Streamlit to handle larger datasets and improve WebSocket stability
# This increases the message size limit to handle large dataframes
st.config.set_option('server.maxMessageSize', 1000)

# Additional WebSocket stability configurations
st.config.set_option('server.enableWebsocketCompression', True)
st.config.set_option('server.enableCORS', False)
st.config.set_option('server.maxUploadSize', 200)

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

                result = func(*args, connection=connection, cursor=cursor, **kwargs)

                cursor.close()
                connection.close()

                return result

            except Exception as e:
                return None

        return wrapper

    return decorator


ads_data_query = '''SELECT buid,a.ad_account_id,a.ad_id,ad_status,effective_status,a.created_at,edited_at as status_change_date,error_type,error_description
-- ,spend
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
  (fad.edited_at) AS edited_at,
  (fad.created_date) AS created_at,

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
where date(edited_at) = current_date
)a
where rw=1
) a
-- left join
-- ( select ad_id,sum(spend)spend  from zocket_global.fb_ads_age_gender_metrics_v3 
-- group by 1)b on a.ad_id=b.ad_id
left join zocket_global.fb_child_ad_accounts d on a.ad_account_id = d.ad_account_id
left join zocket_global.fb_child_business_managers e on e.id = d.app_business_manager_id
left join 
    (SELECT     id ,name,brand_name,json_extract_path_text(json_extract_array_element_text(business_user_ids, 0), 'role') AS role,
     json_extract_path_text(json_extract_array_element_text(business_user_ids, 0), 'business_user_id') AS buid
 FROM
     zocket_global.business_profile
 WHERE
     json_extract_path_text(json_extract_array_element_text(business_user_ids, 0), 'role') = 'owner' )bp on e.app_business_id=bp.id
'''

@st.cache_data(ttl=3600)  # Cache for 1 hour to reduce database load
@redshift_connection(db,name,passw,server,port)
def execute_query(connection, cursor,query):

    cursor.execute(query)
    column_names = [desc[0] for desc in cursor.description]
    result = pd.DataFrame(cursor.fetchall(), columns=column_names)

    return result

# Load data with error handling and progress indication
try:
    progress_bar = st.progress(0)
    status_text = st.empty()
    
    status_text.text("Connecting to database...")
    progress_bar.progress(25)
    
    df_ads = execute_query(query=ads_data_query)
    progress_bar.progress(75)
    
    status_text.text("Processing data...")
    
    if df_ads is None or df_ads.empty:
        st.error("Failed to load ads data. Please check your database connection.")
        st.stop()
    
    progress_bar.progress(100)
    status_text.text("Data loaded successfully!")
    
    # Clear progress indicators after a short delay
    import time
    time.sleep(0.5)
    progress_bar.empty()
    status_text.empty()
    
except Exception as e:
    st.error(f"Error loading data: {str(e)}")
    st.stop()

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

# Convert date columns to proper datetime format
if not df_ads.empty:
    try:
        # Convert string dates to datetime objects
        if 'created_at' in df_ads.columns:
            df_ads['created_at'] = pd.to_datetime(df_ads['created_at'])
        if 'status_change_date' in df_ads.columns:
            df_ads['status_change_date'] = pd.to_datetime(df_ads['status_change_date'])
    except Exception as e:
        st.warning(f"Warning: Could not convert date columns: {str(e)}")

# Add ad_link column to df_ads with error handling
if not df_ads.empty and 'ad_account_id' in df_ads.columns and 'ad_id' in df_ads.columns:
    try:
        df_ads['ad_link'] = df_ads.apply(lambda row: generate_ad_link(row['ad_account_id'], row['ad_id']), axis=1)
    except Exception as e:
        st.warning(f"Warning: Could not generate ad links: {str(e)}")
        df_ads['ad_link'] = "Link unavailable"

# Use all data without filters
    filtered_df_ads = df_ads.copy()

# Create tabs for navigation
tab1, tab2, tab3 = st.tabs(["⏰ Hourly Update", "📊 Today's Stats", "📋 Raw Dump"])

# Hourly Update Section
with tab1:
    try:
        st.title("⏰ Hourly Update")
        
        # Get current UTC time and calculate last 4 hours
        current_utc = pd.Timestamp.now(tz='UTC')
        four_hours_ago_utc = current_utc - pd.Timedelta(hours=4)
        
        # Display timeframe information
        st.subheader("🕐 Timeframe Information")
        col1, col2 = st.columns(2)
        
        with col1:
            st.info(f"**Current UTC Time:** {current_utc.strftime('%Y-%m-%d %H:%M:%S')} UTC")
            # Calculate IST time (UTC + 5:30)
            current_ist = current_utc + pd.Timedelta(hours=5, minutes=30)
            st.info(f"**Current IST Time:** {current_ist.strftime('%Y-%m-%d %H:%M:%S')} IST")
        
        with col2:
            # Calculate IST times for analysis period
            four_hours_ago_ist = four_hours_ago_utc + pd.Timedelta(hours=5, minutes=30)
            current_ist = current_utc + pd.Timedelta(hours=5, minutes=30)
            st.info(f"**Analysis Period:** Last 4 hours\n\nUTC: {four_hours_ago_utc.strftime('%Y-%m-%d %H:%M:%S')} - {current_utc.strftime('%Y-%m-%d %H:%M:%S')}\n\nIST: {four_hours_ago_ist.strftime('%Y-%m-%d %H:%M:%S')} - {current_ist.strftime('%Y-%m-%d %H:%M:%S')}")

        if not df_ads.empty:
            # Use the already converted datetime columns and ensure they have UTC timezone
            df_ads_hourly = df_ads.copy()
            
            # Ensure datetime columns have UTC timezone
            if 'created_at' in df_ads_hourly.columns:
                if df_ads_hourly['created_at'].dt.tz is None:
                    df_ads_hourly['created_at'] = df_ads_hourly['created_at'].dt.tz_localize('UTC')
                else:
                    df_ads_hourly['created_at'] = df_ads_hourly['created_at'].dt.tz_convert('UTC')
            
            if 'status_change_date' in df_ads_hourly.columns:
                if df_ads_hourly['status_change_date'].dt.tz is None:
                    df_ads_hourly['status_change_date'] = df_ads_hourly['status_change_date'].dt.tz_localize('UTC')
                else:
                    df_ads_hourly['status_change_date'] = df_ads_hourly['status_change_date'].dt.tz_convert('UTC')
            
            # Filter data for last 4 hours
            last_4_hours_data = df_ads_hourly[
                (df_ads_hourly['status_change_date'] >= four_hours_ago_utc) &
                (df_ads_hourly['status_change_date'] <= current_utc)
            ]
            
            # Calculate hourly metrics
            st.subheader("📊 Last 4 Hours Metrics")
            
            # Total ads rejected in last 4 hours
            total_rejected_4h = len(last_4_hours_data[last_4_hours_data['ad_status'] == 'DISAPPROVED'])
            
            # Ads rejected in the last hour specifically
            one_hour_ago_utc = current_utc - pd.Timedelta(hours=1)
            last_hour_data = df_ads_hourly[
                (df_ads_hourly['status_change_date'] >= one_hour_ago_utc) &
                (df_ads_hourly['status_change_date'] <= current_utc)
            ]
            ads_rejected_last_hour = len(last_hour_data[last_hour_data['ad_status'] == 'DISAPPROVED'])
            
            # Display metrics in columns
            col1, col2, col3 = st.columns(3)
            
            with col1:
                st.metric(
                    "Ads Rejected in Last Hour", 
                    ads_rejected_last_hour,
                    help=f"Ads rejected between {one_hour_ago_utc.strftime('%H:%M')} UTC and {current_utc.strftime('%H:%M')} UTC"
                )
            
            with col2:
                st.metric(
                    "Ads Rejected in Last 4 Hours", 
                    total_rejected_4h,
                    help=f"Total ads rejected between {four_hours_ago_utc.strftime('%H:%M')} UTC and {current_utc.strftime('%H:%M')} UTC"
                )
            
            with col3:
                # Calculate hourly breakdown
                hourly_breakdown = []
                for i in range(4):
                    hour_start = current_utc - pd.Timedelta(hours=i+1)
                    hour_end = current_utc - pd.Timedelta(hours=i)
                    hour_data = df_ads_hourly[
                        (df_ads_hourly['status_change_date'] >= hour_start) &
                        (df_ads_hourly['status_change_date'] < hour_end) &
                        (df_ads_hourly['ad_status'] == 'DISAPPROVED')
                    ]
                    # Calculate IST times for display
                    hour_start_ist = hour_start + pd.Timedelta(hours=5, minutes=30)
                    hour_end_ist = hour_end + pd.Timedelta(hours=5, minutes=30)
                    
                    hourly_breakdown.append({
                        'Hour UTC': f"{hour_start.strftime('%H')}:00 - {hour_end.strftime('%H')}:00 UTC",
                        'Hour IST': f"{hour_start_ist.strftime('%H')}:00 - {hour_end_ist.strftime('%H')}:00 IST",
                        'Rejected Ads': len(hour_data)
                    })
                
                st.metric(
                    "Peak Hour Rejections", 
                    max([h['Rejected Ads'] for h in hourly_breakdown]) if hourly_breakdown else 0,
                    help="Highest number of rejections in any single hour"
                )
            
            # Top 5 Ad Accounts with rejected ads in last 4 hours
            st.subheader("🏆 Top 5 Ad Accounts (Last 4 Hours)")
            
            if total_rejected_4h > 0:
                # Get top accounts by rejected ads count
                top_accounts_4h = (
                    last_4_hours_data[last_4_hours_data['ad_status'] == 'DISAPPROVED']
                    .groupby('ad_account_id')
                    .agg(
                        rejected_ads=('ad_id', 'count'),
                        buid=('buid', 'first')
                    )
                    .sort_values('rejected_ads', ascending=False)
                    .head(5)
                )
                
                if not top_accounts_4h.empty:
                    # Display top accounts
                    for idx, (account_id, row) in enumerate(top_accounts_4h.iterrows(), 1):
                        col1, col2, col3 = st.columns([1, 3, 1])
                        
                        with col1:
                            st.write(f"**#{idx}**")
                        
                        with col2:
                            st.write(f"**Account:** {account_id}")
                            st.write(f"**BUID:** {row['buid']}")
                        
                        with col3:
                            st.metric("Rejected Ads", row['rejected_ads'])
                    
                    # Hourly breakdown for top accounts
                    st.subheader("📈 Hourly Breakdown for Top Accounts")
                    
                    for account_id in top_accounts_4h.index[:3]:  # Show breakdown for top 3 accounts
                        st.write(f"**Account: {account_id}**")
                        
                        account_hourly_data = []
                        for i in range(4):
                            hour_start = current_utc - pd.Timedelta(hours=i+1)
                            hour_end = current_utc - pd.Timedelta(hours=i)
                            hour_data = df_ads_hourly[
                                (df_ads_hourly['ad_account_id'] == account_id) &
                                (df_ads_hourly['status_change_date'] >= hour_start) &
                                (df_ads_hourly['status_change_date'] < hour_end) &
                                (df_ads_hourly['ad_status'] == 'DISAPPROVED')
                            ]
                            # Calculate IST times for display
                            hour_start_ist = hour_start + pd.Timedelta(hours=5, minutes=30)
                            hour_end_ist = hour_end + pd.Timedelta(hours=5, minutes=30)
                            
                            account_hourly_data.append({
                                'Hour UTC': f"{hour_start.strftime('%H')}:00 - {hour_end.strftime('%H')}:00 UTC",
                                'Hour IST': f"{hour_start_ist.strftime('%H')}:00 - {hour_end_ist.strftime('%H')}:00 IST",
                                'Rejected Ads': len(hour_data)
                            })
                        
                        account_df = pd.DataFrame(account_hourly_data)
                        st.dataframe(account_df, use_container_width=True, hide_index=True)
                        st.write("---")
                
                else:
                    st.warning("No rejected ads found in the last 4 hours.")
            else:
                st.success("🎉 **Great news!** No ads were rejected in the last 4 hours.")
            
            # Additional insights
            st.subheader("💡 Additional Insights")
            
            # Error type breakdown for last 4 hours
            if total_rejected_4h > 0:
                error_breakdown = (
                    last_4_hours_data[last_4_hours_data['ad_status'] == 'DISAPPROVED']
                    .groupby('error_type')
                    .size()
                    .sort_values(ascending=False)
                )
                
                if not error_breakdown.empty:
                    st.write("**Top Error Types (Last 4 Hours):**")
                    for error_type, count in error_breakdown.head(3).items():
                        st.write(f"• {error_type}: {count} rejections")
            
            # Trend analysis
            st.subheader("📊 Trend Analysis")
            
            # Create hourly trend data grouped by actual hour
            trend_data = []
            for i in range(4):
                # Get the hour we're analyzing (going backwards from current hour)
                target_hour = current_utc - pd.Timedelta(hours=i)
                hour_start = target_hour.replace(minute=0, second=0, microsecond=0)
                hour_end = hour_start + pd.Timedelta(hours=1)
                
                # Filter data for this specific hour
                hour_data = df_ads_hourly[
                    (df_ads_hourly['status_change_date'] >= hour_start) &
                    (df_ads_hourly['status_change_date'] < hour_end) &
                    (df_ads_hourly['ad_status'] == 'DISAPPROVED')
                ]
                
                # Calculate IST time for display
                hour_start_ist = hour_start + pd.Timedelta(hours=5, minutes=30)
                
                trend_data.append({
                    'Hour': f"{hour_start.strftime('%H')} UTC",
                    'Hour_IST': f"{hour_start_ist.strftime('%H')} IST",
                    'Rejected Ads': len(hour_data)
                })
            
            trend_df = pd.DataFrame(trend_data)
            trend_df = trend_df.sort_values('Hour')
            
            # Display trend chart and table side by side
            col1, col2 = st.columns(2)
            
            with col1:
                st.write("**Hourly Rejection Trend**")
                st.line_chart(trend_df.set_index('Hour')['Rejected Ads'])
            
            with col2:
                st.write("**Hourly Breakdown**")
                # Create display table with both UTC and IST times
                display_df = trend_df[['Hour', 'Hour_IST', 'Rejected Ads']].copy()
                display_df.columns = ['UTC Hour', 'IST Hour', 'Rejected Ads']
                st.dataframe(display_df, use_container_width=True, hide_index=True)
            
        else:
            st.warning("No data available for hourly analysis.")
            
    except Exception as e:
        st.error(f"Error in Hourly Update section: {str(e)}")
        st.info("Please try refreshing the page or contact support if the issue persists.")

# Today's Stats Section
with tab2:
    try:
        st.title("📊 Today's Stats")
        
        if not df_ads.empty:
            # Calculate today's date
            today = pd.Timestamp.now().date()
            
            # Use the already converted datetime columns
            df_ads_copy = df_ads.copy()
            
            # Calculate today's metrics
            today_ads = len(df_ads_copy[
                df_ads_copy['created_at'].dt.date == today
            ])
            today_rejected = len(df_ads_copy[
                (df_ads_copy['ad_status'] == 'DISAPPROVED') &
                (df_ads_copy['status_change_date'].dt.date == today)
            ])
            
            # Top rejected account today
            today_rejected_by_account = df_ads_copy[
                (df_ads_copy['ad_status'] == 'DISAPPROVED') &
                (df_ads_copy['status_change_date'].dt.date == today)
            ].groupby('ad_account_id').size().sort_values(ascending=False)
            
            top_rejected_account_today = today_rejected_by_account.index[0] if len(today_rejected_by_account) > 0 else "None"
            top_rejected_count_today = today_rejected_by_account.iloc[0] if len(today_rejected_by_account) > 0 else 0
            
            # Display metrics in columns
            col1, col2, col3 = st.columns(3)
            
            with col1:
                st.metric("Today's Ads", today_ads)
            
            with col2:
                st.metric("Today's Rejected", today_rejected)
            
            with col3:
                st.metric("Rejection Rate", f"{(today_rejected/today_ads*100):.1f}%" if today_ads > 0 else "0%")
            
            # Top rejected account info
            st.info(f"🔴 **Top Rejected Account Today:** {top_rejected_account_today} ({top_rejected_count_today} rejections)")
        
        # Grouped Tables Section
        st.subheader("📈 Today's Analysis")
        
        # Grouped data by hour of disapproved ads for today
        today_grouped_by_hour = (
            filtered_df_ads[
                (filtered_df_ads['ad_status'] == 'DISAPPROVED') &
                (filtered_df_ads['status_change_date'].dt.date == today)
            ]
            .groupby(filtered_df_ads['status_change_date'].dt.hour)
            .agg(no_of_ads=('ad_id', 'count'))
            .sort_index(ascending=True)
        )
        # Rename the index to 'Hour' for better display
        today_grouped_by_hour.index.name = 'Hour'
        
        st.write("Today's Disapproved Ads Count by Hour")
        st.dataframe(today_grouped_by_hour, use_container_width=True)

        # Grouped data by error_type of disapproved ads for today
        today_error_grouped = (
            filtered_df_ads[
                (filtered_df_ads['ad_status'] == 'DISAPPROVED') &
                (filtered_df_ads['status_change_date'].dt.date == today)
            ]
            .groupby(['error_type'])
            .agg(no_of_ads=('ad_id', 'count'))
            .sort_values('no_of_ads', ascending=False)
        )
        st.write("Today's Disapproved Ads Count by Error Type")
        st.dataframe(today_error_grouped, use_container_width=True)

        # Top accounts today
        st.subheader("🏆 Top Ad Accounts Today")
        
        if today_rejected > 0:
            # Get top accounts by rejected ads count for today
            top_accounts_today = (
                filtered_df_ads[
                    (filtered_df_ads['ad_status'] == 'DISAPPROVED') &
                    (filtered_df_ads['status_change_date'].dt.date == today)
                ]
                .groupby('ad_account_id')
                .agg(
                    rejected_ads=('ad_id', 'count'),
                    buid=('buid', 'first')
                )
                .sort_values('rejected_ads', ascending=False)
                .head(10)
            )
        
            if not top_accounts_today.empty:
                # Create a summary table
                summary_table = top_accounts_today.reset_index()
                summary_table.columns = ['Ad Account ID', 'Rejected Ads Count', 'BUID']
                summary_table['Rank'] = range(1, len(summary_table) + 1)
                summary_table = summary_table[['Rank', 'Ad Account ID', 'BUID', 'Rejected Ads Count']]
                
                st.dataframe(summary_table, use_container_width=True, hide_index=True)
            else:
                st.warning("No rejected ads found today.")
        else:
            st.success("🎉 **Great news!** No ads were rejected today.")
        
    except Exception as e:
        st.error(f"Error in Today's Stats section: {str(e)}")
        st.info("Please try refreshing the page or contact support if the issue persists.")

# Raw Dump Section
with tab3:
    try:
        st.title("📋 Raw Dump")
        
        # Show data summary
        total_rows = len(filtered_df_ads)
        st.info(f"📊 **Total Records Available:** {total_rows:,}")
        
        # Add download options
        st.subheader("📥 Download Data")
        col1, col2 = st.columns(2)
        
        with col1:
            # CSV Download
            csv_data = filtered_df_ads.to_csv(index=False)
            st.download_button(
                label="📄 Download CSV",
                data=csv_data,
                file_name=f"ads_data_{pd.Timestamp.now().strftime('%Y%m%d_%H%M%S')}.csv",
                mime="text/csv",
                help="Download the data as CSV file"
            )
        
        with col2:
            # Excel Download
            import io
            excel_buffer = io.BytesIO()
            with pd.ExcelWriter(excel_buffer, engine='openpyxl') as writer:
                filtered_df_ads.to_excel(writer, index=False, sheet_name='Ads Data')
            excel_data = excel_buffer.getvalue()
            
            st.download_button(
                label="📊 Download Excel",
                data=excel_data,
                file_name=f"ads_data_{pd.Timestamp.now().strftime('%Y%m%d_%H%M%S')}.xlsx",
                mime="application/vnd.openxmlformats-officedocument.spreadsheetml.sheet",
                help="Download the data as Excel file"
            )
        
        # Display the raw data table
        st.subheader("📊 Raw Data Table")
        st.dataframe(filtered_df_ads, use_container_width=True, hide_index=True)
        
        st.success("✅ **Data Table:** Complete dataset displayed above. Use the download buttons to export data.")
        
    except Exception as e:
        st.error(f"Error in Raw Dump section: {str(e)}")
        st.info("Please try refreshing the page or contact support if the issue persists.")