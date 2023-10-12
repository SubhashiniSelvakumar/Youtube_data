#importing the necessary libraries
import pandas as pd
import plotly.express as px
import streamlit as st
from streamlit_option_menu import option_menu
import mysql.connector
import pymongo
from googleapiclient.discovery import build
from PIL import Image

# SETTING PAGE CONFIGURATIONS
icon = Image.open("D:\Subhashini\Datascience\project\Youtube_logo.png")
st.set_page_config(page_title= "Youtube Data Harvesting and Warehousing ",
                   page_icon= icon,
                   layout= "wide",
                   initial_sidebar_state= "expanded",
                   menu_items={'About': """# This app is created by Subhashini"""})

# CSS for custom styling
custom_css = """
<style>
body {font-family: Arial, sans-serif;
    background-color: #FFFFE0;}h1 {color: #1e60a7;}h2 {color: #1e60a7;} h3 {color: #1e60a7;}
.sidebar .stSelectbox-label {
    font-size: 16px;
    color: #1e60a7 !important;
}
</style>
"""
st.markdown(custom_css, unsafe_allow_html=True)

# Mongo Db Connection
client = pymongo.MongoClient("mongodb+srv://subhashini:briyani@cluster0.neuug1f.mongodb.net/?retryWrites=true&w=majority")
db = client['youtube_data']
channel_details_collection = db['channel_details']
comments_details_collection = db['comments_details']
video_details_collection = db['video_details']

# CONNECTING WITH MYSQL DATABASE
mydb = mysql.connector.connect(
    host="localhost",
    user="root",
    password="", 
    database="Y_db"            
        )
print(mydb)
mycursor = mydb.cursor(buffered=True)

mycursor.execute("DROP TABLE IF EXISTS channels")

# Create table
mycursor.execute("""
    CREATE TABLE channels (
        Channel_id VARCHAR(50) PRIMARY KEY,
        Channel_name VARCHAR(255),
        Playlist_id VARCHAR(255),
        Subscribers INT,
        Views INT,
        Total_videos INT,
        Description TEXT,
        Country VARCHAR(50)
    )
""")
create_table_query = """
CREATE TABLE IF NOT EXISTS videos (
    video_id VARCHAR(255) PRIMARY KEY,
    channel_name VARCHAR(255),
    title VARCHAR(255),
    likes INT,
    views INT
)
"""
create_comments_table_query = """
CREATE TABLE IF NOT EXISTS comments (
    Comment_id VARCHAR(255) PRIMARY KEY,
    Video_id VARCHAR(255),
    Comment_text TEXT,
    Comment_author VARCHAR(255),
    Comment_posted_date DATETIME,
    Like_count INT,
    Reply_count INT
)
"""
mycursor.execute(create_table_query)
mycursor.execute(create_comments_table_query)
mydb.commit()

# CONNECTION WITH YOUTUBE API
api_key = "AIzaSyCxqtUdj8hQ_jSMi4X81cduw-iSXbyGttw"
youtube = build('youtube','v3',developerKey=api_key)


# FUNCTION TO GET CHANNEL ID
def get_channel_details(channel_id):
    ch_data = []
    response = youtube.channels().list(
        part='snippet,contentDetails,statistics',
        id=channel_id
    ).execute()

    if 'items' in response:
        item = response['items'][0]
        data = {
            'Channel_id': channel_id,
            'Channel_name': item['snippet']['title'],
            'Playlist_id': item['contentDetails']['relatedPlaylists']['uploads'],
            'Subscribers': item['statistics']['subscriberCount'],
            'Views': item['statistics']['viewCount'],
            'Total_videos': item['statistics']['videoCount'],
            'Description': item['snippet']['description'],
            'Country': item['snippet'].get('country')
        }
        ch_data.append(data)
    else:
        print("No channel details found in the API response.")
    return ch_data

# FUNCTION TO GET VIDEO IDS
def get_channel_videos(channel_id):
    video_ids = []
    res = youtube.channels().list(id=channel_id, 
                                  part='contentDetails').execute()
    playlist_id = res['items'][0]['contentDetails']['relatedPlaylists']['uploads']
    next_page_token = None
    
    while True:
        res = youtube.playlistItems().list(playlistId=playlist_id, 
                                           part='snippet', 
                                           maxResults=50,
                                           pageToken=next_page_token).execute()
        
        for i in range(len(res['items'])):
            video_ids.append(res['items'][i]['snippet']['resourceId']['videoId'])
        next_page_token = res.get('nextPageToken')
        
        if next_page_token is None:
            break
    return video_ids


# FUNCTION TO GET VIDEO DETAILS
def get_video_details(v_ids):
    video_stats = []
    
    for i in range(0, len(v_ids), 50):
        response = youtube.videos().list(
                    part="snippet,contentDetails,statistics",
                    id=','.join(v_ids[i:i+50])).execute()
        for video in response['items']:
            video_details = dict(Channel_name = video['snippet']['channelTitle'],
                                Channel_id = video['snippet']['channelId'],
                                Video_id = video['id'],
                                Title = video['snippet']['title'],
                                Tags = video['snippet'].get('tags'),
                                Thumbnail = video['snippet']['thumbnails']['default']['url'],
                                Description = video['snippet']['description'],
                                Published_date = video['snippet']['publishedAt'],
                                Duration = video['contentDetails']['duration'],
                                Views = video['statistics']['viewCount'],
                                Likes = video['statistics'].get('likeCount'),
                                Comments = video['statistics'].get('commentCount'),
                                Favorite_count = video['statistics']['favoriteCount'],
                                Definition = video['contentDetails']['definition'],
                                Caption_status = video['contentDetails']['caption']
                               )
            video_stats.append(video_details)
    return video_stats


# FUNCTION TO GET COMMENT DETAILS
def get_comments_details(v_id):
    comment_data = []
    try:
        next_page_token = None
        while True:
            response = youtube.commentThreads().list(part="snippet,replies",
                                                    videoId=v_id,
                                                    maxResults=100,
                                                    pageToken=next_page_token).execute()
            for cmt in response['items']:
                data = dict(Comment_id = cmt['id'],
                            Video_id = cmt['snippet']['videoId'],
                            Comment_text = cmt['snippet']['topLevelComment']['snippet']['textDisplay'],
                            Comment_author = cmt['snippet']['topLevelComment']['snippet']['authorDisplayName'],
                            Comment_posted_date = cmt['snippet']['topLevelComment']['snippet']['publishedAt'],
                            Like_count = cmt['snippet']['topLevelComment']['snippet']['likeCount'],
                            Reply_count = cmt['snippet']['totalReplyCount']
                           )
                comment_data.append(data)
            next_page_token = response.get('nextPageToken')
            if next_page_token is None:
                break
    except:
        pass
    return comment_data


# FUNCTION TO GET CHANNEL NAMES FROM MONGODB
def channel_names():   
    ch_name = []
    for i in db.channel_details.find():
        ch_name.append(i['Channel_name'])
    return ch_name

selected = st.sidebar.selectbox("Menu", ["Home", "Extract", "Transform", "View"])

# Home page
if selected == "Home":
    st.markdown("<p style='font-size:36px; font-weight:bold; color:white;'> YOUTUBE DATA HARVESTING AND WAREHOUSING</p>", unsafe_allow_html=True)
    st.markdown("<p style='font-size:30px; font-weight:bold; color:orange;'>Technologies:<br><span style='color:white;'>Python, MongoDB, YouTube Data API, MySQL, Streamlit</span></p>", unsafe_allow_html=True)
    
    st.markdown("""
    ## Project Overview
    This project is focused on retrieving and analyzing YouTube channel data using various technologies. It involves the following key steps:
    1. **Data Retrieval:** We fetch valuable information from YouTube channels using the YouTube Data API.
    2. **Data Storage:** The collected data is stored in a MongoDB database, serving as a data lake.
    3. **Data Transformation:** We transform and structure the data for efficient analysis.
    4. **SQL Database:** Data is migrated to a MySQL database for structured storage.
    5. **Data Analysis:** SQL queries provide insights on video views, likes, comments, and more.
    6. **Streamlit App:** An interactive Streamlit app visualizes the analyzed data for easy exploration.
    
    This pipeline enables data-driven decisions and insights for YouTube content creators and analysts.
    """)

# Extract page
elif selected == "Extract":
    st.markdown("<p style='font-size:30px; font-weight:bold; color:orange;'>EXTRACT</p>", unsafe_allow_html=True)
    st.markdown("#    ")
    st.write("### Enter YouTube Channel_ID below:")
    ch_id = st.text_input("Hint: Go to the channel's home page > Right click > View page source > Find channel_id").split(',')

    if ch_id and st.button("Extract Data"):
        ch_details = get_channel_details(ch_id)
        if ch_details:
            st.write(f'#### Extracted data from "{ch_details[0]["Channel_name"]} channel')
        else:
            st.write('#### No data found for the specified channel')

    if st.button("Upload to MongoDB"):
        with st.spinner('Please Wait for it...'):
            ch_details = get_channel_details(ch_id)
            v_ids = get_channel_videos(ch_id)
            vid_details = get_video_details(v_ids)
            
            def comments():
                com_d = []
                for i in v_ids:
                    com_d += get_comments_details(i)
                return com_d
            comm_details = comments()

            collections1 = db.channel_details
            collections1.insert_many(ch_details)

            collections2 = db.video_details
            collections2.insert_many(vid_details)

            collections3 = db.comments_details
            collections3.insert_many(comm_details)
            st.success("Upload to MongoDB successful !!")

# Transform page
elif selected == "Transform":
    st.markdown("<p style='font-size:30px; font-weight:bold; color:orange;'>TRANSFORM</p>", unsafe_allow_html=True)
    
    st.markdown("#   ")
    st.markdown("### Select a channel to begin Transformation to SQL")
    
    ch_names = channel_names()
    user_inp = st.selectbox("Select channel", options=ch_names)
    
    def insert_into_channels(user_inp):
        collections1 = db.channel_details
        query = """INSERT INTO channels (Channel_id, Channel_name, Playlist_id, Subscribers, Views, Total_videos, Description, Country) VALUES (%s,%s,%s,%s,%s,%s,%s,%s)"""
    
        for doc in collections1.find({"Channel_name" : user_inp}, {'_id': 0}):
           data_to_insert = (
            doc["Channel_id"],
            doc["Channel_name"],
            doc["Playlist_id"],
            doc["Subscribers"],
            doc["Views"],
            doc["Total_videos"],
            doc["Description"],
            doc["Country"]
        )
        mycursor.execute(query, data_to_insert)
        mydb.commit()

    def insert_into_videos(user_inp):
        collections2 = db.video_details
        mycursor.execute("""
        SELECT channel_name AS Channel_Name
        FROM videos
        WHERE Published_date LIKE '2022%'
        GROUP BY channel_name
        ORDER BY channel_name
     """)
       
    # def insert_into_videos():
    #     collections = db.video_details
    #     mycursor.execute("""
    #         SELECT channel_name AS Channel_Name
    #         FROM videos
    #         WHERE Published_date LIKE '2022%'
    #         GROUP BY channel_name
    #         ORDER BY channel_name
    #     """)

        query1 = """INSERT INTO videos (Video_id, Channel_name, Title, Likes, Views) VALUES (%s,%s,%s,%s,%s,%s)"""

        for doc in collections2.find({"Channel_name" : user_inp}, {"_id": 0}):
           data_to_insert = (
            doc["Video_id"],
            doc["Channel_name"],
            doc["Title"],
            doc["Likes"],
            doc["Views"]
        )
        mycursor.execute(query1, data_to_insert)
        mydb.commit()

    def insert_into_comments(user_inp):
        collections1 = db.video_details
        collections2 = db.comments_details
        query2 = """INSERT INTO comments (Comment_id, Video_id, Comment_text, Comment_author, Comment_posted_date, Like_count, Reply_count) VALUES (%s,%s,%s,%s,%s,%s,%s)"""

        for vid in collections1.find({"Channel_name" : user_inp}, {'_id' : 0}):
            for doc in collections2.find({'Video_id': vid['Video_id']}, {'_id' : 0}):
               data_to_insert = (
                doc["Comment_id"],
                doc["Video_id"],
                doc["Comment_text"],
                doc["Comment_author"],
                doc["Comment_posted_date"],
                doc["Like_count"],
                doc["Reply_count"]
             )
            mycursor.execute(query2, data_to_insert)
            mydb.commit()

    if st.button("Submit"):
        try:
           insert_into_channels(user_inp)
           insert_into_videos(user_inp)
           insert_into_comments(user_inp)
           st.success("Transformation to MySQL Successful!!!")
        except:
             st.error("Channel details already transformed!!")

# View page
elif selected == "View":
    st.write("## :orange[Select any question to get Insights]")
    questions = st.selectbox('Questions', [
        'Click the question that you would like to query',
        '1. What are the names of all the videos and their corresponding channels?',
        '2. Which channels have the most number of videos, and how many videos do they have?',
        '3. What are the top 10 most viewed videos and their respective channels?',
        '4. How many comments were made on each video, and what are their corresponding video names?',
        '5. Which videos have the highest number of likes, and what are their corresponding channel names?',
        '6. What is the total number of likes and dislikes for each video, and what are their corresponding video names?',
        '7. What is the total number of views for each channel, and what are their corresponding channel names?',
        '8. What are the names of all the channels that have published videos in the year 2022?',
        '9. What is the average duration of all videos in each channel, and what are their corresponding channel names?',
        '10. Which videos have the highest number of comments, and what are their corresponding channel names?'
    ])

    if questions == '1. What are the names of all the videos and their corresponding channels?':
        mycursor.execute("""SELECT title AS Video_Title, channel_name AS Channel_Name FROM videos ORDER BY channel_name""")
        df = pd.DataFrame(mycursor.fetchall(), columns=mycursor.column_names)
        st.write(df)

    elif questions == '2. Which channels have the most number of videos, and how many videos do they have?':
        mycursor.execute("""SELECT channel_name AS Channel_Name, total_videos AS Total_Videos FROM channels ORDER BY total_videos DESC""")
        df = pd.DataFrame(mycursor.fetchall(), columns=mycursor.column_names)
        st.write(df)
        st.write("### :green[Number of videos in each channel :]")
        fig = px.bar(df, x=mycursor.column_names[0], y=mycursor.column_names[1], orientation='v', color=mycursor.column_names[0])
        st.plotly_chart(fig, use_container_width=True)

    elif questions == '3. What are the top 10 most viewed videos and their respective channels?':
        mycursor.execute("""SELECT channel_name AS Channel_Name, title AS Video_Title, views AS Views FROM videos ORDER BY views DESC LIMIT 10""")
        df = pd.DataFrame(mycursor.fetchall(), columns=mycursor.column_names)
        st.write(df)
        st.write("### :green[Top 10 most viewed videos :]")
        fig = px.bar(df, x=mycursor.column_names[2], y=mycursor.column_names[1], orientation='h', color=mycursor.column_names[0])
        st.plotly_chart(fig, use_container_width=True)
  
    elif questions == '4. How many comments were made on each video, and what are their corresponding video names?':
        mycursor.execute("""SELECT a.video_id AS Video_id, a.title AS Video_Title, b.Total_Comments
                            FROM videos AS a
                            LEFT JOIN (SELECT video_id,COUNT(comment_id) AS Total_Comments
                            FROM comments GROUP BY video_id) AS b
                            ON a.video_id = b.video_id
                            ORDER BY b.Total_Comments DESC""")
        df = pd.DataFrame(mycursor.fetchall(),columns=mycursor.column_names)
        st.write(df)
          
    elif questions == '5. Which videos have the highest number of likes, and what are their corresponding channel names?':
        mycursor.execute("""SELECT channel_name AS Channel_Name,title AS Title,likes AS Likes_Count 
                            FROM videos
                            ORDER BY likes DESC
                            LIMIT 10""")
        df = pd.DataFrame(mycursor.fetchall(),columns=mycursor.column_names)
        st.write(df)
        st.write("### :green[Top 10 most liked videos :]")
        fig = px.bar(df,
                     x=mycursor.column_names[2],
                     y=mycursor.column_names[1],
                     orientation='h',
                     color=mycursor.column_names[0]
                    )
        st.plotly_chart(fig,use_container_width=True)
        
    elif questions == '6. What is the total number of likes and dislikes for each video, and what are their corresponding video names?':
        mycursor.execute("""SELECT title AS Title, likes AS Likes_Count
                            FROM videos
                            ORDER BY likes DESC""")
        df = pd.DataFrame(mycursor.fetchall(),columns=mycursor.column_names)
        st.write(df)
         
    elif questions == '7. What is the total number of views for each channel, and what are their corresponding channel names?':
        mycursor.execute("""SELECT channel_name AS Channel_Name, views AS Views
                            FROM channels
                            ORDER BY views DESC""")
        df = pd.DataFrame(mycursor.fetchall(),columns=mycursor.column_names)
        st.write(df)
        st.write("### :green[Channels vs Views :]")
        fig = px.bar(df,
                     x=mycursor.column_names[0],
                     y=mycursor.column_names[1],
                     orientation='v',
                     color=mycursor.column_names[0]
                    )
        st.plotly_chart(fig,use_container_width=True)
        
    elif questions == '8. What are the names of all the channels that have published videos in the year 2022?':
        mycursor.execute("""SELECT channel_name AS Channel_Name
                            FROM videos
                            WHERE published_date LIKE '2022%'
                            GROUP BY channel_name
                            ORDER BY channel_name""")
        df = pd.DataFrame(mycursor.fetchall(),columns=mycursor.column_names)
        st.write(df)
        
    elif questions == '9. What is the average duration of all videos in each channel, and what are their corresponding channel names?':
        mycursor.execute("""SELECT channel_name, 
                        SUM(duration_sec) / COUNT(*) AS average_duration
                        FROM (
                            SELECT channel_name, 
                            CASE
                                WHEN duration REGEXP '^PT[0-9]+H[0-9]+M[0-9]+S$' THEN 
                                TIME_TO_SEC(CONCAT(
                                SUBSTRING_INDEX(SUBSTRING_INDEX(duration, 'H', 1), 'T', -1), ':',
                            SUBSTRING_INDEX(SUBSTRING_INDEX(duration, 'M', 1), 'H', -1), ':',
                            SUBSTRING_INDEX(SUBSTRING_INDEX(duration, 'S', 1), 'M', -1)
                            ))
                                WHEN duration REGEXP '^PT[0-9]+M[0-9]+S$' THEN 
                                TIME_TO_SEC(CONCAT(
                                '0:', SUBSTRING_INDEX(SUBSTRING_INDEX(duration, 'M', 1), 'T', -1), ':',
                                SUBSTRING_INDEX(SUBSTRING_INDEX(duration, 'S', 1), 'M', -1)
                            ))
                                WHEN duration REGEXP '^PT[0-9]+S$' THEN 
                                TIME_TO_SEC(CONCAT('0:0:', SUBSTRING_INDEX(SUBSTRING_INDEX(duration, 'S', 1), 'T', -1)))
                                END AS duration_sec
                        FROM videos
                        ) AS subquery
                        GROUP BY channel_name""")
        df = pd.DataFrame(mycursor.fetchall(),columns=mycursor.column_names
                          )
        st.write(df)
        st.write("### :green[Average video duration for channels :]")
        

        
    elif questions == '10. Which videos have the highest number of comments, and what are their corresponding channel names?':
        mycursor.execute("""SELECT channel_name AS Channel_Name,video_id AS Video_ID,comments AS Comments
                            FROM videos
                            ORDER BY comments DESC
                            LIMIT 10""")
        df = pd.DataFrame(mycursor.fetchall(),columns=mycursor.column_names)
        st.write(df)
        st.write("### :green[Videos with most comments :]")
        fig = px.bar(df,
                     x=mycursor.column_names[1],
                     y=mycursor.column_names[2],
                     orientation='v',
                     color=mycursor.column_names[0]
                    )
        st.plotly_chart(fig,use_container_width=True)

mycursor.close()
mydb.close()
