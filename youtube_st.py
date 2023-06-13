from googleapiclient.discovery import build
from dateutil import parser
import pandas as pd
import streamlit as st
from streamlit_option_menu import option_menu
import isodate
import pymongo
from pymongo import MongoClient
import mysql.connector
import numpy as np
from PIL import Image
import plotly.express as px

icon = Image.open("Youtube_logo.jpg")
st.set_page_config(page_title= "Youtube Data Harvesting and Warehousing",
                   page_icon= icon,
                   layout= "wide",
                   initial_sidebar_state= "expanded",
                   menu_items={'About': """# This app is created by *Pavan Kumar!*"""})
# horizontal menu
selected = option_menu(None, ["Home", "Extract & Transform", "View"], 
    icons=['house', 'tools', "card-text"], 
                        menu_icon="cast", default_index=0, orientation="horizontal")
selected

# Bridging a connection with MongoDB Atlas and Creating a new database(youtube_data)
mongo_client = MongoClient("mongodb+srv://pavanchoudhary22feb:Choudhary24@cluster0.ugz5f0c.mongodb.net/")
mongo_db = mongo_client["youtube"]

# MySQL connection

mysql_host = "127.0.0.1"
mysql_user = "root"
mysql_password = "Choudhary@120"
mysql_database = "youtube"
# Connect to MySQL
mysql_conn = mysql.connector.connect(
    host=mysql_host, user=mysql_user, password=mysql_password, database=mysql_database, auth_plugin='mysql_native_password'
)
mysql_cursor = mysql_conn.cursor()

# YouTube API setup
Api_key = 'AIzaSyA8REjpFJN_RD6r_3UvyDhCz8InXdCcuNY'
api_service_name = 'youtube'
api_version = 'v3'
youtube = build(api_service_name,api_version,developerKey=Api_key)

## Functions to get data transform data, uppload data to mongo db & migrate data to MySQL
def get_channel_data(Channel_ID):
    channel_data = []
    request = youtube.channels().list(
          part = "snippet,contentDetails,statistics",
          id = Channel_ID)
    response = request.execute()
    for iteam in response ["items"]:
        data = {'channel_id':iteam["id"],
                'channel_name':iteam["snippet"]["title"],
                'channel_playlist_id':iteam["contentDetails"]["relatedPlaylists"]["uploads"],
                'channel_type':iteam['kind'],
                'channel_subcription':iteam["statistics"]["subscriberCount"],
                'channel_views':iteam["statistics"]["viewCount"],
                'channel_total_videos':iteam["statistics"]["videoCount"],
                'channel_status':iteam["snippet"]["publishedAt"],
                'channel_description':iteam["snippet"]["description"]
               }
        channel_data.append(data)
    return (channel_data)

def get_video_ids_details(youtube, channel_playlist_id):
    
    video_id_list = []
    
    request = youtube.playlistItems().list(
        part="snippet,contentDetails",
        playlistId=channel_playlist_id,
        maxResults = 50
    )
    response = request.execute()
    
    for item in response['items']:
        data = {item['contentDetails']['videoId']}
        
    next_page_token = response.get('nextPageToken')
    while next_page_token is not None:
        request = youtube.playlistItems().list(
                    part='contentDetails',
                    playlistId = channel_playlist_id,
                    maxResults = 50,
                    pageToken = next_page_token)
        response = request.execute()

        for item in response['items']:
            video_id_list.append(item['contentDetails']['videoId'])
        
        
        next_page_token = response.get('nextPageToken')
        
    return (video_id_list)

def get_video_details(youtube, video_id_list):

    all_video_info = []
    
    for i in range(0, len(video_id_list), 50):
        request = youtube.videos().list(
            part="snippet,contentDetails,statistics",
            id=','.join(video_id_list[i:i+50])
        )
        response = request.execute() 

        for video in response['items']:
            video_info = dict(channel_name = video['snippet']['channelTitle'],
                              channel_id = video['snippet']['channelId'],
                              video_id = video['id'],
                              Title = video['snippet']['title'],
                              Tags = video['snippet'].get('tags'),
                              Thumbnail = video['snippet']['thumbnails']['default']['url'],
                              video_Description = video['snippet']['description'],
                              Published_date = video['snippet']['publishedAt'],
                              video_Duration = video['contentDetails']['duration'],
                              Views = video['statistics']['viewCount'],
                              Likes = video['statistics'].get('likeCount'),
                              Dislike = video['statistics'].get('dislikeCount'),
                              Comments = video['statistics'].get('commentCount'),
                              Favorite_count = video['statistics']['favoriteCount'],
                              video_Definition = video['contentDetails']['definition'],
                              Caption_status = video['contentDetails']['caption']
                               )
            all_video_info.append(video_info)
    
    return pd.DataFrame(all_video_info)

def get_comments_in_videos(youtube, video_id_list):
    
    all_comments = []
    
    for video_id in video_id_list:
        try:   
            request = youtube.commentThreads().list(
                part="snippet,replies",
                videoId=video_id
            )
            response = request.execute()
        
            comments_in_video = []
            for comment in response['items'][0:10]:
                comment_id = comment['id']
                comment_author = comment['snippet']['topLevelComment']['snippet']['authorDisplayName']
                comment_publish_date = comment['snippet']['topLevelComment']['snippet']['publishedAt']
                comment_text = comment['snippet']['topLevelComment']['snippet']['textOriginal']
                Like_count = comment['snippet']['topLevelComment']['snippet']['likeCount'],
                Reply_count = comment['snippet']['totalReplyCount']
                comment_info = {
                    'video_id': video_id,
                    'comment_id': comment_id,
                    'comment_author': comment_author,
                    'comment_publish_date': comment_publish_date,
                    'comment_text': comment_text,
                    'Likes':Like_count,
                    'Reply':Reply_count
                }
                
                comments_in_video.append(comment_info)

            all_comments.extend(comments_in_video)
            
        except: 
            # When error occurs - most likely because comments are disabled on a video
            print('Could not get comments for video ' + video_id)
        
    return pd.DataFrame(all_comments)


def clean_data(video_details_df, comments_df, channel_data_df):
    # Clean video details
    cols = ['Views', 'Likes', 'Dislike', 'Favorite_count', 'Comments']
    video_details_df[cols] = video_details_df[cols].apply(pd.to_numeric, errors='coerce', axis=1)
    # Create publish day (in the week) column
    video_details_df['Published_date'] = video_details_df['Published_date'].apply(lambda x: parser.parse(x)) 
    video_details_df['pushblishDayName'] = video_details_df['Published_date'].apply(lambda x: x.strftime("%A"))
    # convert duration to seconds
    video_details_df['video_Duration'] = video_details_df['video_Duration'].apply(lambda x: isodate.parse_duration(x))
    video_details_df['video_Duration'] = video_details_df['video_Duration'].astype('timedelta64[s]')
    # Add number of tags
    video_details_df['Tags'] = video_details_df['Tags'].apply(lambda x: 0 if x is None else len(x))
    ##video_df adding 2 colouumn for batter review of likes & comment
    # create 2 columns Like & Comments Ratio as per per 1000 view Count
    video_details_df['LikeRatio'] = video_details_df['Likes'] / video_details_df['Views'] * 1000
    video_details_df['CommentRatio'] = video_details_df['Comments'] / video_details_df['Views'] * 1000
    video_details_df = video_details_df.fillna(value=0)
    ## Comment_df Cleaning: Create publish day (in the week) column
    col1 = ['Likes','Reply']
    comments_df[col1] = comments_df[col1].apply(pd.to_numeric, errors='coerce', axis=1)
    comments_df['comment_publish_date'] = comments_df['comment_publish_date'].apply(lambda x: parser.parse(x))
    comments_df['comment_DayName'] = comments_df['comment_publish_date'].apply(lambda x: x.strftime("%A"))
    comments_df = comments_df.fillna(value=0)
    ## channel_details_df Cleaning: Create publish day (in the week) column & Change Date in correct format
    col2 = ['channel_views','channel_total_videos','channel_subcription']
    channel_data_df[col2] = channel_data_df[col2].apply(pd.to_numeric, errors='coerce', axis=1)
    channel_data_df['channel_status'] = channel_data_df['channel_status'].apply(lambda x: parser.parse(x))
    channel_data_df['channel_status_DayName'] = channel_data_df['channel_status'].apply(lambda x: x.strftime("%A"))
    channel_data_df = channel_data_df.fillna(value=0)
    return video_details_df, comments_df, channel_data_df

def push_data_mongodb(channel_data_cleaned, video_details_cleaned, comments_cleaned):
    try:
        mongo_db.channel_details.insert_many(channel_data_cleaned.to_dict(orient='records'))
        mongo_db.videodata.insert_many(video_details_cleaned.to_dict(orient='records'))
        mongo_db.comments_data.insert_many(comments_cleaned.to_dict(orient='records'))
        st.success("Upload to MongoDB successful!")
    except pymongo.errors.PyMongoError as e:
        st.error("Error occurred during MongoDB insertion: " + str(e))

# FUNCTION TO GET CHANNEL NAMES FROM MONGODB
def  get_channel_names():
    channel_names = []
    for i in mongo_db.channel_details.find():
        channel_names.append(i['channel_name'])
    return channel_names


# HOME PAGE
if selected == "Home":
    # Title Image
    st.image("Title.jfif")
    col1, col2 = st.columns(2, gap='medium')
    col1.markdown("# :blue[Domain] : Social Media")
    col1.markdown("# :blue[Technologies used] : Python, Youtube Data API, MongoDB, MySql, Streamlit")
    col1.markdown("# :blue[Overview] : Retrieving the Youtube channels data from the Google API, Clean data, storing it in a MongoDB Atlas, migrating and transforming data into a SQL database, querying the data and displaying it in the Streamlit app, data Visualization")
    col2.markdown("#   ")
    col2.markdown("#   ")
    col2.markdown("#   ")
    col2.image("YouTube_Logo.jpg")
    

if selected == "Extract & Transform":
    tab1, tab2 = st.tabs(["$\huge 📝 EXTRACT $", "$\huge🚀 TRANSFORM $"])

    # EXTRACT Data from YouTube TAB
    with tab1:
        st.markdown("#    ")
        st.write("### Enter YouTube Channel_ID below :")
        Channel_ID = st.text_input("Instruction: Goto channel's home page > Ctrl+U > Ctrl+F > channel_id").split(',')

        if Channel_ID and st.button("Get_youtube_Data"):
            with st.spinner('Please wait...'):
                channel_data = get_channel_data(Channel_ID)
                channel_playlist_id = channel_data[0]['channel_playlist_id']
                video_ids = get_video_ids_details(youtube, channel_playlist_id)
                video_details_df = get_video_details(youtube, video_ids)
                video_details_df['playlist_id'] = channel_playlist_id
                comments_df = get_comments_in_videos(youtube,video_ids)
                channel_data_df = pd.DataFrame(channel_data)
                video_details_cleaned, comments_cleaned, channel_data_cleaned = clean_data(video_details_df, comments_df, channel_data_df)
                st.dataframe(video_details_cleaned, width=200, height=200)
                st.dataframe(comments_cleaned, width=200, height=200)
                st.dataframe(channel_data_cleaned, width=200, height=200)
                push_data_mongodb(channel_data_cleaned, video_details_cleaned, comments_cleaned)
              
    with tab2:
        st.markdown("#   ")
        st.markdown("### Select a channel to begin Transformation to SQL")

        channel_names = get_channel_names()
        user_input = st.selectbox("Select channel", options=channel_names)

    def insert_into_channels():
        collections = mongo_db.channel_details
        query = """INSERT INTO channel_details VALUES(%s,%s,%s,%s,%s,%s,%s,%s,%s,%s)"""

        for i in collections.find({"channel_name": user_input}, {'_id': 0}):
            try:
                mysql_cursor.execute(query, tuple(i.values()))
                mysql_conn.commit()
            except mysql.connector.errors.DataError:
                st.warning("Skipped insertion for channel_details: Data too long for column. Consider truncating the data.")
            except Exception as e:
                st.error("Error occurred during MySQL insertion for channel_details: " + str(e))

    def insert_into_videos():
        collections1 = mongo_db.videodata
        query1 = """INSERT INTO videodata VALUES(%s,%s,%s,%s,%s,%s,%s,%s,%s,%s,%s,%s,%s,%s,%s,%s,%s,%s,%s,%s)"""

        for i in collections1.find({"channel_name": user_input}, {'_id': 0}):
            try:
                mysql_cursor.execute(query1, tuple(i.values()))
                mysql_conn.commit()
            except mysql.connector.errors.DataError:
                st.warning("Skipped insertion for videodata: Data too long for column. Consider truncating the data.")
            except Exception as e:
                st.error("Error occurred during MySQL insertion for videodata: " + str(e))

    def insert_into_comments():
        collections1 = mongo_db.videodata
        collections2 = mongo_db.comments_data
        query2 = """INSERT INTO comment_data VALUES(%s,%s,%s,%s,%s,%s,%s,%s)"""

        for vid in collections1.find({"channel_name": user_input}, {'_id': 0}):
            for i in collections2.find({'video_id': vid['video_id']}, {'_id': 0}):
                try:
                    mysql_cursor.execute(query2, tuple(i.values()))
                    mysql_conn.commit()
                except mysql.connector.errors.DataError:
                    st.warning("Skipped insertion for comment_data: Data too long for column. Consider truncating the data.")
                except Exception as e:
                    st.error("Error occurred during MySQL insertion for comment_data: " + str(e))

    if st.button("Submit"):
        try:
            # Insert statements for channels, videos, and comments
            insert_into_channels()
            insert_into_videos()
            insert_into_comments()
            st.success("Transformation to MySQL Successful!")
        except Exception as e:
            st.error("Error occurred during MySQL insertion: " + str(e))

# VIEW PAGE
if selected == "View":
    
    st.write("## :orange[Select any question to get Insights]")
    questions = st.selectbox('Questions',
    ['1. What are the names of all the videos and their corresponding channels?',
    '2. Which channels have the most number of videos, and how many videos do they have?',
    '3. What are the top 10 most viewed videos and their respective channels?',
    '4. How many comments were made on each video, and what are their corresponding video names?',
    '5. Which videos have the highest number of likes, and what are their corresponding channel names?',
    '6. What is the total number of likes and dislikes for each video, and what are their corresponding video names?',
    '7. What is the total number of views for each channel, and what are their corresponding channel names?',
    '8. What are the names of all the channels that have published videos in the year 2022?',
    '9. What is the average duration of all videos in each channel, and what are their corresponding channel names?',
    '10. Which videos have the highest number of comments, and what are their corresponding channel names?'])
    
    if questions == '1. What are the names of all the videos and their corresponding channels?':
        mysql_cursor.execute("""SELECT title AS Video_Title, channel_name AS Channel_Name
                            FROM videodata
                            ORDER BY channel_name""")
        df = pd.DataFrame(mysql_cursor.fetchall(),columns=mysql_cursor.column_names)
        st.write(df)
        
    elif questions == '2. Which channels have the most number of videos, and how many videos do they have?':
        mysql_cursor.execute("""SELECT channel_name AS channel_Name, channel_total_videos AS total_Videos
FROM channel_details
ORDER BY total_videos DESC""")
        df = pd.DataFrame(mysql_cursor.fetchall(),columns=mysql_cursor.column_names)
        st.write(df)
        st.write("### :green[Number of videos in each channel :]")
        #st.bar_chart(df,x= mysql_cursor.column_names[0],y= mysql_cursor.column_names[1])
        fig = px.bar(df,
                     x=mysql_cursor.column_names[0],
                     y=mysql_cursor.column_names[1],
                     orientation='v',
                     color=mysql_cursor.column_names[0]
                    )
        st.plotly_chart(fig,use_container_width=True)
        
    elif questions == '3. What are the top 10 most viewed videos and their respective channels?':
        mysql_cursor.execute("""SELECT channel_name AS channel_Name, Title AS Video_Title, Views AS Views 
                            FROM videodata
                            ORDER BY views DESC
                            LIMIT 10""")
        df = pd.DataFrame(mysql_cursor.fetchall(),columns=mysql_cursor.column_names)
        st.write(df)
        st.write("### :green[Top 10 most viewed videos :]")
        fig = px.bar(df,
                     x=mysql_cursor.column_names[2],
                     y=mysql_cursor.column_names[1],
                     orientation='h',
                     color=mysql_cursor.column_names[0]
                    )
        st.plotly_chart(fig,use_container_width=True)
        
    elif questions == '4. How many comments were made on each video, and what are their corresponding video names?':
        mysql_cursor.execute("""SELECT v.video_id AS video_id, v.Title AS video_Title, c.Total_Comments
                            FROM videodata AS v
                            LEFT JOIN (SELECT video_id,COUNT(comment_id) AS Total_Comments
                            FROM comment_data GROUP BY video_id) AS c
                            ON v.video_id = c.video_id
                            ORDER BY c.Total_Comments DESC""")
        df = pd.DataFrame(mysql_cursor.fetchall(),columns=mysql_cursor.column_names)
        st.write(df)
          
    elif questions == '5. Which videos have the highest number of likes, and what are their corresponding channel names?':
        mysql_cursor.execute("""SELECT channel_name AS channel_Name,Title AS Title,Likes AS Likes_Count
                            FROM videodata
                            ORDER BY Likes DESC
                            LIMIT 10""")
        df = pd.DataFrame(mysql_cursor.fetchall(),columns=mysql_cursor.column_names)
        st.write(df)
        st.write("### :green[Top 10 most liked videos :]")
        fig = px.bar(df,
                     x=mysql_cursor.column_names[2],
                     y=mysql_cursor.column_names[1],
                     orientation='h',
                     color=mysql_cursor.column_names[0]
                    )
        st.plotly_chart(fig,use_container_width=True)
        
    elif questions == '6. What is the total number of likes and dislikes for each video, and what are their corresponding video names?':
        mysql_cursor.execute("""SELECT Title AS Title, likes AS Likes_Count
                            FROM videodata
                            ORDER BY Likes DESC""")
        df = pd.DataFrame(mysql_cursor.fetchall(),columns=mysql_cursor.column_names)
        st.write(df)
         
    elif questions == '7. What is the total number of views for each channel, and what are their corresponding channel names?':
        mysql_cursor.execute("""SELECT channel_name AS channel_Name, channel_views AS Views
                            FROM channel_details
                            ORDER BY views DESC""")
        df = pd.DataFrame(mysql_cursor.fetchall(),columns=mysql_cursor.column_names)
        st.write(df)
        st.write("### :green[Channels vs Views :]")
        fig = px.bar(df,
                     x=mysql_cursor.column_names[0],
                     y=mysql_cursor.column_names[1],
                     orientation='v',
                     color=mysql_cursor.column_names[0]
                    )
        st.plotly_chart(fig,use_container_width=True)
        
    elif questions == '8. What are the names of all the channels that have published videos in the year 2022?':
        mysql_cursor.execute("""SELECT channel_name AS channel_Name
                            FROM videodata
                            WHERE Published_date LIKE '2022%'
                            GROUP BY channel_name
                            ORDER BY channel_name""")
        df = pd.DataFrame(mysql_cursor.fetchall(),columns=mysql_cursor.column_names)
        st.write(df)
        
    elif questions == '9. What is the average duration of all videos in each channel, and what are their corresponding channel names?':
        mysql_cursor.execute("""SELECT channel_name AS Channel_Name,
                            AVG(video_Duration)/60 AS "Average_Video_Duration (mins)"
                            FROM videodata
                            GROUP BY channel_name
                            ORDER BY AVG(video_Duration)/60 DESC""")
        df = pd.DataFrame(mysql_cursor.fetchall(),columns=mysql_cursor.column_names)
        st.write(df)
        st.write("### :green[Avg video duration for channels :]")
        fig = px.bar(df,
                     x=mysql_cursor.column_names[0],
                     y=mysql_cursor.column_names[1],
                     orientation='v',
                     color=mysql_cursor.column_names[0]
                    )
        st.plotly_chart(fig,use_container_width=True)
        
    elif questions == '10. Which videos have the highest number of comments, and what are their corresponding channel names?':
        mysql_cursor.execute("""SELECT channel_name AS channel_Name,video_id AS video_id,comments AS comments
                            FROM videodata
                            ORDER BY comments DESC
                            LIMIT 10""")
        df = pd.DataFrame(mysql_cursor.fetchall(),columns=mysql_cursor.column_names)
        st.write(df)
        st.write("### :green[Videos with most comments :]")
        fig = px.bar(df,
                     x=mysql_cursor.column_names[1],
                     y=mysql_cursor.column_names[2],
                     orientation='v',
                     color=mysql_cursor.column_names[0]
                    )
        st.plotly_chart(fig,use_container_width=True)
        