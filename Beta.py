
api_key='xxx' #your api key


import pymongo
import psycopg2
import pandas as pd
import streamlit as st
from googleapiclient.discovery import build
import isodate

api_service_name="youtube"
api_version="v3"
youtube = build(api_service_name, api_version, developerKey=api_key)
#enter your credentials
host = 'xx'
port = 'yy'
database = 'zz'
username = 'aa'
password = 'bb'
#creating connection with postgresql 
eta = psycopg2.connect(host=host, port=port, database=database, user=username, password=password)
cursor=eta.cursor()

#to change the format of the duration of videos
def format_duration(duration):
    duration_obj = isodate.parse_duration(duration)
    hours = duration_obj.total_seconds() // 3600
    minutes = (duration_obj.total_seconds() % 3600) // 60
    seconds = duration_obj.total_seconds() % 60
    formatted_duration = f"{int(hours):02d}:{int(minutes):02d}:{int(seconds):02d}"
    return formatted_duration

#getting channel details using the channel id
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



def get_playlists(youtube,channel_id):
  request = youtube.playlists().list(
        part="snippet,contentDetails",
        channelId=channel_id,
        maxResults=25
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

     next_page_token = response.get('nextPageToken')
    
     while next_page_token is not None:

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


def get_video_ids(youtube, playlist_id):
  request = youtube.playlistItems().list(
                part='contentDetails',
                playlistId = playlist_id,
                maxResults = 50)
  response = request.execute()

  video_ids = []

  for i in range(len(response['items'])):
        video_ids.append(response['items'][i]['contentDetails']['videoId'])

  next_page_token = response.get('nextPageToken')
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

            for k in stats_to_keep.keys():
                for v in stats_to_keep[k]:
                    try:
                        if k == 'contentDetails' and v == 'duration':
                            video_info[v] = format_duration(video[k][v])
                        else:
                            video_info[v] = video[k][v]
                    except KeyError:
                        video_info[v] = None

            #all_video_info.append(video_info)
        return (video_info)


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
        return 'Could not get comments for video '
    
    return all_comments


omega=pymongo.MongoClient("xxxx")#enter the connection link from mongodb

db=omega["project"]#creating the database to store all the colections
col=db["Channels"]

@st.cache_data

def channel_Details(channel_id):
  det=get_channel_sts(youtube,channel_id)#getting channel details and storing it in det
  col=db["Channels"]
  col.insert_one(det)#storing it in the collection channels
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




#creating the table for the channel details and the same is done for videos, playlists and comments
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
    df=pd.DataFrame(doc)
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
    df1=pd.DataFrame(doc1)
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
                      duration interval, 
                      definition varchar(10), 
                      caption varchar(50), 
                      channelId varchar(100))''')
        eta.commit()
    except:
        eta.rollback()

    col4=db["videos"]
    data4=col4.find()
    doc4=list(data4)
    df4=pd.DataFrame(doc4)
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
    df3=pd.DataFrame(doc3)

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
    
def tables():
    channels_table()
    playlists_table()
    videos_table()
    comments_table()
    return ("Completed!!")

def display_channels():
    db=omega['project']
    col=db['Channels']
    tableofchannels=list(col.find())
    tableofchannels=st.dataframe(tableofchannels)
    return tableofchannels
    


def display_videos():
    db=omega['project']
    col=db['videos']
    tableofvideos=list(col.find())
    tableofvideos=st.dataframe(tableofvideos)
    return tableofvideos
    


def display_playlists():
    db=omega['project']
    col=db['playlists']
    tableofplaylists=list(col.find())
    tableofplaylists=st.dataframe(tableofplaylists)
    return tableofplaylists
    

def display_comments():
    db=omega['project']
    col=db['playlists']
    tableofcomments=list(col.find())
    tableofcomments=st.dataframe(tableofcomments)
    return tableofcomments
    

def one():
    try:
        cursor.execute("select title as videos, channeltitle as chanel_name from videos;")
        eta.commit()
        t1=cursor.fetchall()
        st.write(pd.DataFrame(t1, columns=['Video Title','Channel Name']))
    except:
        eta.rollback()
        cursor.execute("select title as videos, channeltitle as chanel_name from videos;")
        eta.commit()
        t1=cursor.fetchall()
        st.write(pd.DataFrame(t1, columns=['Video Title','Channel Name']))


def two():
    try:
        cursor.execute("select channelName as ChannelName,totalvideos as No_Videos from channels order by totalvideos desc limit 1;")
        eta.commit()
        t2=cursor.fetchall()
        st.write(pd.DataFrame(t2, columns=['Channel Name','No Of Videos']))
    except:
        eta.rollback()
        cursor.execute("select channelName as ChannelName,totalvideos as No_Videos from channels order by totalvideos desc limit 1;")
        eta.commit()
        t2=cursor.fetchall()
        st.write(pd.DataFrame(t2, columns=['Channel Name','No Of Videos']))

def three():
    try:
        cursor.execute('''select viewCount as views , channeltitle as ChannelName,title as Name from videos 
                        where viewCount is not null order by viewCount desc limit 10;''')
        eta.commit()
        t3=cursor.fetchall()
        st.write(pd.DataFrame(t3, columns=['Video Views','Channel Name', 'Video Title']))
    except:
        eta.rollback()
        cursor.execute('''select viewCount as views , channeltitle as ChannelName,title as Name from videos 
                        where viewcount is not null order by viewCount desc limit 10;''')
        eta.commit()
        t3=cursor.fetchall()
        st.write(pd.DataFrame(t3, columns=['Video Views','Channel Name', 'Video Title']))


def four():
    try:
        cursor.execute("select commentCount as No_comments ,title as Name from videos where commentCount is not null;") 
        eta.commit()
        t4=cursor.fetchall()
        st.write(pd.DataFrame(t4, columns=['No Of Comments', 'Video Title']))
    except:
        eta.rollback()
        cursor.execute("select commentCount as No_comments ,title as Name from videos where commentCount is not null;") 
        eta.commit()
        t4=cursor.fetchall()
        st.write(pd.DataFrame(t4, columns=['No Of Comments', 'Video Title']))

def five():
    try:
        cursor.execute('''select title as Video, channeltitle as ChannelName, likeCount as Likes from videos 
                       where likecount is not null order by likecount desc;''')
        eta.commit()
        t5=cursor.fetchall()
        st.write(pd.DataFrame(t5, columns=['Video Title', 'Channel Name','Video Likes']))
    except:
        eta.rollback()
        cursor.execute('''select title as Video, channeltitle as ChannelName, likeCount as Likes from videos 
                       where likecount is not null order by likecount desc;''')
        eta.commit()
        t5=cursor.fetchall()
        st.write(pd.DataFrame(t6, columns=['Video Title', 'Channel Name','Video Likes']))

def six():
    try:
        cursor.execute('''select likeCount as likes,title as Name from videos;''')
        eta.commit()
        t6=cursor.fetchall()
        st.write(pd.DataFrame(t6, columns=['Likes', 'Video title']))
    except:
        eta.rollback()
        cursor.execute('''select likeCount as likes,title as Name from videos;''')
        eta.commit()
        t6=cursor.fetchall()
        st.write(pd.DataFrame(t6, columns=['Likes', 'Video title']))

def seven():
    try:
        cursor.execute("select channelName as ChannelName, views as Channelviews from channels;")
        eta.commit()
        t7=cursor.fetchall()
        st.write(pd.DataFrame(t7, columns=['Channel Name', 'Channel Views']))
    except:
        eta.rollback()
        cursor.execute("select channelName as ChannelName, views as Channelviews from channels;")
        eta.commit()
        t7=cursor.fetchall()
        st.write(pd.DataFrame(t7, columns=['Channel Name', 'Channel Views']))

def eight():
    try:
        cursor.execute('''select title as name, publishedat as VideoRelease, channeltitle as ChannelName from videos 
                       where extract(year from publishedat) = 2022;''')
        eta.commit()
        t8=cursor.fetchall()
        st.write(pd.DataFrame(t8, columns=['Name', 'Video Publised On', 'ChannelName']))
    except:
        eta.rollback()
        cursor.execute('''select title as name, publishedat as VideoRelease, channeltitle as ChannelName from videos 
                       where extract(year from publishedat) = 2022;''')
        eta.commit()
        t8=cursor.fetchall()
        st.write(pd.DataFrame(t8, columns=['Name', 'Video Publised On', 'ChannelName']))
        
def nine():
    try:
        cursor.execute("SELECT channeltitle as ChannelName, AVG(duration) AS average_duration FROM videos GROUP BY channelName;")
        eta.commit()
        t9 = cursor.fetchall()
        t9 = pd.DataFrame(t9, columns=['ChannelTitle', 'Average Duration'])
        T9=[]
        for _, row in t9.iterrows():
            channel_title = row['ChannelTitle']
            average_duration = row['Average Duration']
            average_duration_str = str(average_duration)
            T9.append({"Channel Title": channel_title ,  "Average Duration": average_duration_str})
        st.write(pd.DataFrame(T9))
    except:
        eta.rollback()
        cursor.execute("SELECT channeltitle as ChannelName, AVG(duration) AS average_duration FROM videos GROUP BY channelName;")
        eta.commit()
        t9 = cursor.fetchall()
        t9 = pd.DataFrame(t9, columns=['ChannelTitle', 'Average Duration'])
        T9=[]
        for _, row in t9.iterrows():
            channel_title = row['ChannelTitle']
            average_duration = row['Average Duration']
            average_duration_str = str(average_duration)
            T9.append({"Channel Title": channel_title ,  "Average Duration": average_duration_str})
        st.write(pd.DataFrame(T9))
        

def ten():
    try:
        cursor.execute('''select title as Name, channeltitle as ChannelName, commentCount as Comments from videos 
                       where commentcount is not null order by commentcount desc;''')
        eta.commit()
        t10=cursor.fetchall()
        st.write(pd.DataFrame(t10, columns=['Video Title', 'Channel Name', 'No Of Comments']))
    except:
        eta.rollback()
        cursor.execute('''select title as Name, channeltitle as ChannelName, commentCount as Comments from videos 
                   where commentcount is not null order by commentcount desc;''')
        eta.commit()
        t10=cursor.fetchall()
        st.write(pd.DataFrame(t10, columns=['Video Title', 'Channel Name', 'No Of Comments']))


with st.sidebar:
    st.title(":violet[YOUTUBE DATA HARVESTING AND WAREHOUSING]")
    st.header("SKILL TAKE AWAY")
    st.caption('Python scripting')
    st.caption("Data Collection")
    st.caption("MongoDB")
    st.caption("API Integration")
    st.caption(" Data Managment using MongoDB (Atlas) and SQL")


channel_id = st.text_input("Enter the Channel ids here")
channels = channel_id.split(',')
channels = [ch.strip() for ch in channels if ch]

if st.button("Collect and Store data"):
    for channel in channels:
        query = {'channelId': channel}
        document = col.find_one(query)
        if document:
            st.success("Channel details of the given channel id: " + channel + " already exists")
        else:
            output = channel_Details(channel)
            st.success(output)


st.subheader("click here to migrate the data to sql tables")        
if st.button("Migrate"):
    display=tables()
    st.success(display)
    
    
frames = st.radio(
     "SELECT THE TABLE YOU WISH TO VIEW",
    ('None','Channel', 'Playlist', 'Video', 'Comment'))

st.write('You selected:', frames)

if frames=='None':
    st.write("  ")
elif frames=='Channel':
    display_channels()
elif frames=='Playlist':
    display_playlists()
elif frames=='Video':
    display_videos()
elif frames=='Comment':
    display_comments()

query = st.selectbox(
    'LET US DO SOME ANALYSIS',
    ('None','All the videos and the Channel Name', 'Channels with most number of videos', '10 most viewed videos',
     'Comments in each video','Videos with highest likes', 'likes of all videos', 'views of each channel',
     'videos published in the year 2022','average duration of all videos in each channel', 'videos with highest number of comments'))

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
elif query=='average duration of all videos in each channel':
    nine()
elif query=='videos with highest number of comments':
    ten()
   


