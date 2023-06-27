
api_key='xxx' #your API key here


import pymongo
import psycopg2
import pandas as pd
import streamlit as st
from googleapiclient.discovery import build 

api_service_name="youtube"
api_version="v3"
youtube = build(api_service_name, api_version, developerKey=api_key)

host = 'aaa'
port = 'bbb'
database = 'xxxx' #the database youcreated
username = 'yyy' #your username
password = 'zzz' #your password

#connecting to psycopg2

eta = psycopg2.connect(host=host, port=port, database=database, user=username, password=password)
cursor=eta.cursor()

#this function gets the details any channel using its channel id 

def get_channel_sts(youtube,channel_id):
  
  request=youtube.channels().list(
      part="snippet,contentDetails,statistics",
      id=channel_id
  )
  response=request.execute()

  for item in response['items']: 
    data={'channelName':item['snippet']['title'],
          'channelId':item['id'],
          'subscribers':item['statistics']['subscriberCount'],
          'views':item['statistics']['viewCount'],
          'totalVideos':item['statistics']['videoCount'],
          'playlistId':item['contentDetails']['relatedPlaylists']['uploads'],
          'channel_description':item['snippet']['description']
    }    
  return data

#this function collects the playlists created in the channel 


def get_playlists(youtube,channel_id):
  request = youtube.playlists().list(
        part="snippet,contentDetails",
        channelId=channel_id,
        maxResults=25 #single request can get a max of 25 playlists
    )
  response = request.execute()
  All_data=[]
  for item in response['items']: 
     data={'PlaylistId':item['id'],
           'Title':item['snippet']['title'],
           'ChannelId':item['snippet']['channelId'],
           'ChannelName':item['snippet']['channelTitle'],
           'PublishedAt':item['snippet']['publishedAt'],
           'VideoCount':item['contentDetails']['itemCount']
           }
     All_data.append(data)

     next_page_token = response.get('nextPageToken') #to get further next page token is stored in a variable
    
     while next_page_token is not None: #it continues collecting the playlists as long as it is not none

          request = youtube.playlists().list(
              part="snippet,contentDetails",
              channelId="UCmXkiw-1x9ZhNOPz0X73tTA",
              maxResults=25)
          response = request.execute()

          for item in response['items']: 
                data={'PlaylistId':item['id'],
                      'Title':item['snippet']['title'],
                      'ChannelId':item['snippet']['channelId'],
                      'ChannelName':item['snippet']['channelTitle'],
                      'PublishedAt':item['snippet']['publishedAt'],
                      'VideoCount':item['contentDetails']['itemCount']}
                All_data.append(data)
          next_page_token = response.get('nextPageToken')
  return All_data

#gets the video ids of a channel using its uploads


def get_video_ids(youtube, playlist_id):
  request = youtube.playlistItems().list(
                part='contentDetails',
                playlistId = playlist_id,
                maxResults = 50) #max result is 50
  response = request.execute()

  video_ids = []

  for i in range(len(response['items'])):
        video_ids.append(response['items'][i]['contentDetails']['videoId'])

  next_page_token = response.get('nextPageToken') #checking for next page token to continue 
  more_pages = True

  while more_pages:
      if next_page_token is None:
          more_pages = False
      else:
          request = youtube.playlistItems().list(
                        part='contentDetails',
                        playlistId = playlist_id,
                        maxResults = 50,
                        pageToken = next_page_token)
          response = request.execute()

          for i in range(len(response['items'])):
              video_ids.append(response['items'][i]['contentDetails']['videoId'])

          next_page_token = response.get('nextPageToken')

  return video_ids

#gets the details of a video


def get_video_detail(youtube, video_id):

        request = youtube.videos().list(
            part="snippet,contentDetails,statistics",
            id=video_id
        )
        response = request.execute()

        for video in response['items']:
            stats_to_keep = {
                'snippet': ['channelTitle', 'title', 'description', 'tags', 'publishedAt','channelId'],
                'statistics': ['viewCount', 'likeCount', 'favoriteCount', 'commentCount'],
                'contentDetails': ['duration', 'definition', 'caption']
            }
            video_info = {}
            video_info['video_id'] = video['id']

            for key in stats_to_keep.keys():
                for value in stats_to_keep[key]:
                    try:
                        video_info[value] = video[key][value]
                    except KeyError:
                        video_info[value] = None

            #all_video_info.append(video_info)
        return (video_info)
    
#gets the comments under every video

def get_comments_in_videos(youtube, video_id):
    all_comments = []
    try:   
        request = youtube.commentThreads().list(
            part="snippet,replies",
            videoId=video_id
        )
        response = request.execute()
    
        for item in response['items']:
            data={'comment_id':item['snippet']['topLevelComment']['id'],
                  'comment_txt':item['snippet']['topLevelComment']['snippet']['textOriginal'],
                  'videoId':item['snippet']['topLevelComment']["snippet"]['videoId'],
                  'author_name':item['snippet']['topLevelComment']['snippet']['authorDisplayName'],
                  'published_at':item['snippet']['topLevelComment']['snippet']['publishedAt'],
            }
            all_comments.append(data)
          
    except: 
        return 'Could not get comments for video ' #in some videos the comments may be disabled
    
    return all_comments


omega=pymongo.MongoClient("xxxxxx") #copy the link from mongodb altas to establish connection and enter your password

db=omega["project"]

@st.cache_data #to prevent repeating data

#integrates all the ablove function
#and stores all the data in a collection in mongodb atlas

def channel_Details(channel_id): 
  det=get_channel_sts(youtube,channel_id)#getting channel details and storing it in det
  col=db["Channels"]
  col.insert_one(det)#storing 
  playlist=get_playlists(youtube,channel_id)
  col=db["playlists"]
  for i in playlist:
    col.insert_one(i)
  Playlist=det.get('playlistId')
  videos=get_video_ids(youtube, Playlist)
  for i in videos:
    v=get_video_detail(youtube, i)
    col=db["videos"]
    col.insert_one(v)
    c=get_comments_in_videos(youtube, i)
    if c!='Could not get comments for video ':
      for j in c:
        col=db["comments"]
        col.insert_one(j)
  return ("process for a channel is completed")


Astronomic='UCmXkiw-1x9ZhNOPz0X73tTA'
Aravind_SA='UCrJNwpevlqZLVO1LW2Mo-Ag'
BrainCraft='UCt_t6FwNsqr3WWoL6dFqG9w'
Debunked='UChagpdlC1jfNTqQc1dQ95OQ'
Garden_up='UC0nChSOqQbA6tAi8_K7pD_A'
Jordindian='UCYLS9TSah19IsB8yyUpiDzg'
Dr_Riya='UCfzzu2GRpjpKkoGQzx6nl5Q'
Lisa_Koshy='UCxSz6JVYmzVhtkraHWZC7HQ'
minutephysics='UCUHW94eEFW7hkUMVaZz4eDg'
Pentatronix='UCmv1CLT6ZcFdTJMHxaR9XeA'

#creating the channels table in posrgresql

def channels_table():

    try:
        cursor.execute('''create table if not exists channels(channelName varchar(50),
                   channelId varchar(80), 
                   subscribers bigint, 
                   views bigint,
                   totalVideos int,
                   playlistId varchar(80), 
                   channel_description text, 
                   primary key (channelId))'''
                   )
        eta.commit()
    except:
        eta.rollback()

    db=omega["project"]
    col=db["Channels"]
    data=col.find()
    doc=list(data)
    df=pd.DataFrame(doc)#converting the data into a dataframe to insert into the created table
    try:
        for _, row in df.iterrows():
            insert_query = '''
                INSERT INTO channels (channelName, channelId, subscribers, views, totalVideos, playlistId, channel_description)
                VALUES (%s, %s, %s, %s, %s, %s, %s)

            '''
            values = (
                row['channelName'],
                row['channelId'],
                row['subscribers'],
                row['views'],
                row['totalVideos'],
                row['playlistId'],
                row['channel_description']
            )
            try:
                cursor.execute(insert_query,values)
                eta.commit()
            except:
                eta.rollback()
    except:
        st.write("values already exists in the channel table")
#creating playlist table        

def playlists_table():
    try:
        cursor.execute('''create table if not exists playlists(PlaylistId varchar(100) primary key,
                   Title text, 
                   ChannelId varchar(80), 
                   ChannelName varchar(50), 
                   PublishedAt timestamp, 
                   VideoCount int)''')
        eta.commit()
    except:
        eta.rollback()
    col=db["playlists"]
    data1=col.find()
    doc1=list(data1)
    df1=pd.DataFrame(doc1)#converting the data into a dataframe to insert into the created table
    try:
        for _, row in df1.iterrows():
            insert_query = '''
                INSERT INTO playlists (PlaylistId, Title, ChannelId, ChannelName, PublishedAt, VideoCount)
                VALUES (%s, %s, %s, %s, %s, %s)

            '''
            values = (
                row['PlaylistId'],
                row['Title'],
                row['ChannelId'],
                row['ChannelName'],
                row['PublishedAt'],
                row['VideoCount']
            )
            try:
                cursor.execute(insert_query,values)
                eta.commit()
            except:
                eta.rollback()
    except:
        st.write("values already exists in the playlist table")
    
#creating videos table getting data from mongodb

def videos_table():
    try:
        cursor.execute('''create table if not exists videos(video_id varchar(50) primary key, 
                      channelTitle varchar(150), 
                      title varchar(150), 
                      description text, 
                      tags text, 
                      publishedAt timestamp, 
                      viewCount bigint, 
                      likeCount bigint,
                      favoriteCount int, 
                      commentCount int, 
                      duration varchar(15), 
                      definition varchar(10), 
                      caption varchar(50), 
                      channelId varchar(100))''')
        eta.commit()
    except:
        eta.rollback()

    col4=db["videos"]#accessing the collection videos of mongodb
    data4=col4.find()
    doc4=list(data4)
    df4=pd.DataFrame(doc4)#converting the data into a dataframe to insert into the created table
    try:
        for _, row in df4.iterrows():
            insert_query = '''
                INSERT INTO videos (video_id, channelTitle,  title, description, tags, publishedAt, 
                viewCount, likeCount, favoriteCount, commentCount, duration, definition, caption, channelId)
                VALUES (%s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s)

            '''
            values = (
                row['video_id'],
                row['channelTitle'],
                row['title'],
                row['description'],
                row['tags'],
                row['publishedAt'],
                row['viewCount'],
                row['likeCount'],
                row['favoriteCount'],
                row['commentCount'],
                row['duration'],
                row['definition'],
                row['caption'],
                row['channelId']
            )
            try:
                cursor.execute(insert_query,values)
                eta.commit()
            except:
                eta.rollback()
    except:
        st.write("values aready exists in the videos table")
    


def comments_table():
    try:
        cursor.execute('''create table if not exists comments(comment_id varchar(100) primary key, comment_txt text, 
                       videoId varchar(80), author_name varchar(150), published_at timestamp)''')
        eta.commit()
    except:
        eta.rollback()
    col3=db["comments"]
    data3=col3.find()
    doc3=list(data3)
    df3=pd.DataFrame(doc3)#converting the data into a dataframe to insert into the created table

    try:
        for _, row in df3.iterrows():
            insert_query = '''
                INSERT INTO comments (comment_id, comment_txt, videoId, author_name, published_at)
                VALUES (%s, %s, %s, %s, %s)

            '''
            values = (
                row['comment_id'],
                row['comment_txt'],
                row['videoId'],
                row['author_name'],
                row['published_at']
            )
            try:
                cursor.execute(insert_query,values)
                eta.commit()
            except:
                eta.rollback()
    except:
        st.write("values already exists in the comments table")
#calling the above four functions 
    
def tables():
    channels_table()
    playlists_table()
    videos_table()
    comments_table()
    return ("done")

def display_channels():#to display the functions in the web page when called
    try:
        cursor.execute("select*from channels;")
        tableofchannels=cursor.fetchall()
        tableofchannels=pd.DataFrame(tableofchannels)
        tableofchannels=st.dataframe(tableofchannels)
        return tableofchannels
    except:
        eta.rollback()
        cursor.execute("select*from channels;")
        tableofchannels=cursor.fetchall()
        tableofchannels=pd.DataFrame(tableofchannels)
        tableofchannels=st.dataframe(tableofchannels)
        return tableofchannels


def display_videos():#to display the functions in the web page when called
    try:
        cursor.execute("select*from videos;")
        tableofvideos=cursor.fetchall()
        tableofvideos=st.dataframe(tableofvideos)
        return tableofvideos
    except:
        eta.rollback()
        cursor.execute("select*from videos;")
        tableofvideos=cursor.fetchall()
        tableofvideos=st.dataframe(tableofvideos)
        return tableofvideos


def display_playlists():#to display the functions in the web page when called
    try:
        cursor.execute("select*from playlists;")
        tableofplaylists=cursor.fetchall()
        tableofplaylists=st.dataframe(tableofplaylists)
        return tableofplaylists
    except:
        eta.rolback()
        cursor.execute("select*from playlists;")
        tableofplaylists=cursor.fetchall()
        tableofplaylists=st.dataframe(tableofplaylists)
        return tableofplaylists

def display_comments():#to display the functions in the web page when called
    try:
        cursor.execute("select*from comments;")
        tableofcomments=cursor.fetchall()
        tableofcomments=st.dataframe(tableofcomments)
        return tableofcomments
    except:
        eta.rollback()
        cursor.execute("select*from comments;")
        tableofcomments=cursor.fetchall()
        tableofcomments=st.dataframe(tableofcomments)
        return tableofcomments

def one():
    try:
        cursor.execute("select title as videos, channeltitle as chanel_name from videos;")
        eta.commit()
        t1=cursor.fetchall()
        t1=st.dataframe(t1)
        return t1
    except:
        eta.rollback()
        cursor.execute("select title as videos, channeltitle as chanel_name from videos;")
        eta.commit()
        t1=cursor.fetchall()
        t1=st.dataframe(t1)
        return t1


def two():
    try:
        cursor.execute("select channelName as ChannelName,totalvideos as No_Videos from channels order by totalvideos desc limit 1;")
        eta.commit()
        t2=cursor.fetchall()
        t2=st.dataframe(t2)
        return t2
    except:
        eta.rollback()
        cursor.execute("select channelName as ChannelName,totalvideos as No_Videos from channels order by totalvideos desc limit 1;")
        eta.commit()
        t2=cursor.fetchall()
        t2=st.dataframe(t2)
        return t2

def three():
    try:
        cursor.execute('''select viewCount as views , channeltitle as ChannelName,title as Name from videos 
                        where viewCount is not null order by viewCount desc limit 10;''')
        eta.commit()
        t3=cursor.fetchall()
        t3=st.dataframe(t3)
        return t3
    except:
        eta.rollback()
        cursor.execute('''select viewCount as views , channeltitle as ChannelName,title as Name from videos 
                        where viewcount is not null order by viewCount desc limit 10;''')
        eta.commit()
        t3=cursor.fetchall()
        t3=st.dataframe(t3)
        return t3


def four():
    try:
        cursor.execute("select commentCount as No_comments ,title as Name from videos where commentCount is not null;") 
        eta.commit()
        t4=cursor.fetchall()
        t4=st.dataframe(t4)
        return t4
    except:
        eta.rollback()
        cursor.execute("select commentCount as No_comments ,title as Name from videos where commentCount is not null;") 
        eta.commit()
        t4=cursor.fetchall()
        t4=st.dataframe(t4)
        return t4

def five():
    try:
        cursor.execute('''select title as Video, channeltitle as ChannelName, likeCount as Likes from videos 
                       where likecount is not null order by likecount desc;''')
        eta.commit()
        t5=cursor.fetchall()
        t5=st.dataframe(t5)
        return t5
    except:
        eta.rollback()
        cursor.execute('''select title as Video, channeltitle as ChannelName, likeCount as Likes from videos 
                       where likecount is not null order by likecount desc;''')
        eta.commit()
        t5=cursor.fetchall()
        t5=st.dataframe(t5)
        return t5

def six():
    try:
        cursor.execute('''select likeCount as likes,title as Name from videos;''')
        eta.commit()
        t6=cursor.fetchall()
        t6=st.dataframe(t6)
        return t6
    except:
        eta.rollback()
        cursor.execute('''select likeCount as likes,title as Name from videos;''')
        eta.commit()
        t6=cursor.fetchall()
        t6=st.dataframe(t6)
        return t6

def seven():
    try:
        cursor.execute("select channelName as ChannelName, views as Channelviews from channels;")
        eta.commit()
        t7=cursor.fetchall()
        t7=st.dataframe(t7)
        return t7
    except:
        eta.rollback()
        cursor.execute("select channelName as ChannelName, views as Channelviews from channels;")
        eta.commit()
        t7=cursor.fetchall()
        t7=st.dataframe(t7)
        return t7

def eight():
    try:
        cursor.execute('''select title as name, publishedat as VideoRelease, channeltitle as ChannelName from videos 
                       where extract(year from publishedat) = 2022;''')
        eta.commit()
        t8=cursor.fetchall()
        t8=st.dataframe(t8)
        return t8
    except:
        eta.rollback()
        cursor.execute('''select title as name, publishedat as VideoRelease, channeltitle as ChannelName from videos 
                       where extract(year from publishedat) = 2022;''')
        eta.commit()
        t8=cursor.fetchall()
        t8=st.dataframe(t8)
        return t8
        


def Nine():
    try:
        cursor.execute('''select title as Name, channeltitle as ChannelName, commentCount as Comments from videos 
                       where commentcount is not null order by commentcount desc;''')
        eta.commit()
        t10=cursor.fetchall()
        t10=st.dataframe(t10)
        return t10
    except:
        eta.rollback()
        cursor.execute('''select title as Name, channeltitle as ChannelName, commentCount as Comments from videos 
                   where commentcount is not null order by commentcount desc;''')
        eta.commit()
        t10=cursor.fetchall()
        t10=st.dataframe(t10)
        return t10


with st.sidebar:
    st.title("YOUTUBE DATA HARVESTING AND WAREHOUSING")
    st.header("SKILL TAKE AWAY")
    st.caption('Python scripting')
    st.caption("Data Collection")
    st.caption("MongoDB")
    st.caption("API Integration")
    st.caption(" Data Managment using MongoDB (Atlas) and SQL")

options = st.multiselect(
    'select the channel here',
    [Astronomic,Aravind_SA,BrainCraft,Debunked,Garden_up,Jordindian,Dr_Riya,Lisa_Koshy,minutephysics,Pentatronix],
    [])

st.write('You Selected:', options)

if st.button("Collect and Store data"):    
    for i in options:
        output=channel_Details(i)
        st.write(output)

st.write("Click here to Migrate the data in sql tables")        
if st.button("Migrate"):
    display=tables()
    st.write(display)
    
    
frames = st.radio(
     "Select the table you want to view",
    ('None','Channel', 'Playlist', 'Video', 'Comment'))

st.write('You selected:', frames)

if frames=='None':
    st.write("select a table")
elif frames=='Channel':
    display_channels()
elif frames=='Playlist':
    display_playlists()
elif frames=='Video':
    display_videos()
elif frames=='Comment':
    display_comments()

query = st.selectbox(
    'let us do some analysis',
    ('None','All the videos and the Channel Name', 'Channels with most number of videos', '10 most viewed videos',
     'Comments in each video','Videos with highest likes', 'likes of all videos', 'views of each channel',
     'videos published in the year 2022', 'videos with highest number of comments'))

if query=='None':
    st.write("you selected None")
elif query=='All the videos and the Channel Name':
    one()
elif query=='Channels with most number of videos':
    two()
elif query=='10 most viewed videos':
    three()
elif query=='Comments in each video':
    four()
elif query=='Videos with highest likes':
    five()
elif query=='likes of all videos':
    six()
elif query=='views of each channel':
    seven()
elif query=='videos published in the year 2022':
    eight()
elif query=='videos with highest number of comments':
    Nine()
   


