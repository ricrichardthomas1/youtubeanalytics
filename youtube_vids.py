# import libraries
from dotenv import load_dotenv
import os
import pandas as pd
import json
from googleapiclient.discovery import build
import math
import sqlalchemy
from sqlalchemy import Table, MetaData, select
from datetime import timedelta, datetime
from airflow.decorators import dag, task

#read environment variables
load_dotenv()
api_key = os.getenv( "GOOGLE_API_KEY" )
db_host = os.getenv("DB_HOST")
db_port = os.getenv("DB_PORT")
db_user = os.getenv("DB_USER")
db_password = os.getenv("DB_PASSWORD")

# initialize youtube client and sqlalchemy connectiont to db
youtube = build('youtube', 'v3', developerKey=api_key)
db_engine = sqlalchemy.create_engine(f"postgresql+psycopg2://{db_user}:{db_password}@{db_host}:{db_port}/richard")
conn = db_engine.connect()
metadata = MetaData()

# define the default parameters dictionary object for the dag
default_args = {
    'owner': 'richard',
    'retries': 5,
    'retry_delay': timedelta(minutes=2)
    }

# define the dag
@dag(
    default_args = default_args,
    dag_id = 'fetch_youtube_data',
    description = 'Fetches most popular YouTube vidoes with description of video and statistics of the video and channel',
    start_date = datetime(2024, 4, 14),
    schedule_interval = '@daily',
    catchup = False
)
def fetch_youtube_data():
    
    # Defining task for fetching most popular videos and their statistics
    @task
    def get_popular_videos(region = 'ae,bh,dz,eg,iq,jo,kw,lb,ly,ma,om,qa,sa,tn,ye,az,by,bg,bd,ba,cz,dk,at,ch,de,gr,au,be,ca,gb,gh,ie,il,in,jm,ke,mt,ng,nz,sg,ug,us,za,zw,ar,bo,cl,co,cr,do,ec,es,gt,hn,mx,ni,pa,pe,pr,py,sv,uy,ve,ee,fi,ph,fr,sn,hr,hu,id,is,it,jp,ge,kz,kr,lu,la,lt,lv,mk,my,no,np,nl,pl,br,pt,ro,ru,lk,sk,si,me,rs,se,tz,th,tr,ua,pk,vn,hk,tw,cy,kh,li,pg',maxResults = 50):
        region = region.split(",")
        videos = []
        n = 1
        for r in region:
            vids = []
            pop_vids_india_request = youtube.videos().list(part = 'id,snippet,statistics,status,contentDetails,topicDetails', chart = 'mostPopular', regionCode = r , maxResults = maxResults)
            n+=1
            pop_vids_india = pop_vids_india_request.execute()
            vids.extend(pop_vids_india['items'])
            while pop_vids_india.get('nextPageToken', []) and n <=2000:
                pop_vids_india_request = youtube.videos().list_next(pop_vids_india_request, pop_vids_india)
                pop_vids_india = pop_vids_india_request.execute()
                vids.extend(pop_vids_india['items'])
                n+=1
            vids = pd.json_normalize(vids)
            vids['region'] = [r]*len(vids)
            vids = vids.to_dict('records')
            videos.extend(vids)
        print(f'API calls : {n}')
        final_df = pd.DataFrame(videos)
        timestamp = pd.Series([datetime.now()]*len(final_df), name = 'timestamp')
        final_df = pd.concat([timestamp,final_df], axis = 1)
        final_df.to_sql('videos', conn, if_exists='append', index=False, chunksize=200, method = 'multi')
        return final_df
    
    # Task to fetch channel details
    @task
    def get_channel_info_and_statistics(videos: pd.DataFrame):
        videos = videos
        id_list = videos['snippet.channelId'].unique()
        channels = []
        loops = math.ceil(len(id_list)/50)
        for i in range(loops):
            channels_response = youtube.channels().list(part = 'id,snippet,statistics,status,contentDetails,topicDetails', id = ",".join(id_list[i*50:(i+1)*50]), maxResults = 50).execute()
            channels.extend(channels_response['items'])
        print(f'API calls : {loops}')
        final_df = pd.json_normalize(channels)
        timestamp = pd.Series([datetime.now()]*len(final_df), name = 'timestamp')
        final_df = pd.concat([timestamp,final_df], axis = 1)
        final_df.to_sql('channels', conn, if_exists='append', index=False, chunksize=200, method = 'multi')
        return "Done!"
    
    # Task to fetch video_categories
    @task
    def get_video_categories(videos: pd.DataFrame):
        categories = set(videos['snippet.categoryId'])
        try:
            vid_categories = Table('vid_categories', metadata, autoload=True, autoload_with=db_engine)
            vid_categories = conn.execute(select(vid_categories.columns.id)).scalars().all()
            vid_categories = set(vid_categories)
            vid_categories = categories - vid_categories
        except sqlalchemy.exc.NoSuchTableError:
            vid_categories = categories
        if not vid_categories:
            return "No new categories to add!"
        else:
            vid_categories_request = youtube.videoCategories().list(part = 'snippet', id = ",".join(vid_categories))
            n = 1
            vid_categories = []
            vid_categories_response = vid_categories_request.execute()
            vid_categories.extend(vid_categories_response['items'])
            while vid_categories_response.get('nextPageToken', []) and n <=100:
                vid_categories_request = youtube.videoCategories().list_next(vid_categories_request, vid_categories_response)
                vid_categories_response = vid_categories_request.execute()
                vid_categories.extend(vid_categories_response['items'])
                n+=1
            print(f'API calls : {n}')
            vid_categories = pd.json_normalize(vid_categories)
            vid_categories.to_sql('vid_categories', conn, if_exists='append', index=False, chunksize=200, method = 'multi')
        return "Done!"
    
    # Running the tasks
    task1 = get_popular_videos()
    task2 = get_channel_info_and_statistics(task1)
    task3 = get_video_categories(task1)
    
# run the dag    
dag_instance = fetch_youtube_data()