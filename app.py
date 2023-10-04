import os
import streamlit as st
from googleapiclient.discovery import build
from pymongo import MongoClient
from apiclient.errors import HttpError

# Set your API key and YouTube channel ID
API_KEY = 'AIzaSyCxqtUdj8hQ_jSMi4X81cduw-iSXbyGttw'
CHANNEL_ID = 'UCvYI7HB4z9WjJ-s5zA6WaGg'
#CHANNEL_ID="UCYwrS5QvBY_JbSdbINLey6Q"


# Set up the YouTube Data API client
youtube = build('youtube', 'v3', developerKey=API_KEY)

# Initialize MongoDB client and database
client = MongoClient('mongodb+srv://subhashini:briyani@cluster0.neuug1f.mongodb.net/?retryWrites=true&w=majority')  # Replace with your MongoDB connection URI
db = client['Youtube']  # Replace 'youtube_data' with your desired database name
collection = db['renalut_channel'] 

def fetch_and_store_channel_data():
    channel_data = youtube.channels().list(
        part='snippet',
        id=CHANNEL_ID
    ).execute()

    if 'items' in channel_data and channel_data['items']:
        channel_information = {
            'channel_name': channel_data['items'][0]['snippet']['title'],
            'channel_description': channel_data['items'][0]['snippet']['description'],
        }
        if 'contentDetails' in channel_data['items'][0]:
            channel_information['playlists'] = channel_data['items'][0]['contentDetails']['relatedPlaylists']['uploads']
        else:
            channel_information['playlists'] = 'Not Available'
        collection.insert_one(channel_information)

def fetch_and_store_videos():
    request = youtube.search().list(
        part='snippet',
        channelId=CHANNEL_ID,
        maxResults=50
    )

    while request:
        response = request.execute()
        for item in response['items']:
            if item['id']['kind'] == 'youtube#video':
                video_id = item['id']['videoId']
                new_document = {
                    'video_id': 'Kwmh3RRS2UQ',
                    'title': item['snippet']['title'],
                    'description': item['snippet']['description'],
                    'published_at': item['snippet']['publishedAt'],
                    'channel_id': item['snippet']['channelId'],
                    'comments': {}
                }
                video_response = youtube.videos().list(
                    part='snippet,statistics',
                    id=video_id
                ).execute()

                if 'items' in video_response and video_response['items']:
                    new_document['view_count'] = video_response['items'][0]['statistics'].get('viewCount', 'Not Available')
                    new_document['like_count'] = video_response['items'][0]['statistics'].get('likeCount', 'Not Available')
                    new_document['dislike_count'] = video_response['items'][0]['statistics'].get('dislikeCount', 'Not Available')

                    if 'snippet' in video_response['items'][0] and 'commentsDisabled' in video_response['items'][0]['snippet']:
                        new_document['comments'] = 'Comments Disabled'
                    else:
                        comments_response = None
                        try:
                            comments_response = youtube.commentThreads().list(
                                part='snippet',
                                videoId=video_id,
                                textFormat='plainText'
                            ).execute()
                            if comments_response and 'items' in comments_response:
                                for comment in comments_response['items']:
                                    comment_id = comment['snippet']['topLevelComment']['id']
                                    comment_information = {
                                        'comment_text': comment['snippet']['topLevelComment']['snippet']['textDisplay'],
                                        'comment_author': comment['snippet']['topLevelComment']['snippet']['authorDisplayName'],
                                        'comment_published_at': comment['snippet']['topLevelComment']['snippet']['publishedAt']
                                    }
                                    new_document['comments'][comment_id] = comment_information
                        except HttpError as e:
                            if e.resp.status == 403:
                                new_document['comments'] = 'Comments Disabled'
                            else:
                                raise  # Re-raise the exception if it's not a 403 error

                collection.insert_one(new_document)
        request = youtube.search().list_next(request, response)

def fetch_and_store_videos():
    if __name__ == '__main__':
       st.title('YouTube Channel Data Fetcher')
       if st.button('Fetch Channel Data'):
          fetch_and_store_channel_data()
          st.success('Channel data fetched and stored successfully!')
       if st.button('Fetch Videos'):
          fetch_and_store_videos()
          st.success('Videos fetched and stored successfully!')


    fetch_and_store_channel_data()
    fetch_and_store_videos()
