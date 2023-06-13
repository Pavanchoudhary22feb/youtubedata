# youtubedata
Youtube Data Warehousing and Transformation and Analysis

This Web Application will fatch all data youtube data from youtube web gooogle api. 
we are fatching 3 types of data as follows.
1. Channels Details,
2. Video Data
3. Comment Data

# Note: We are using streamlit to devlope web application which is based on python code.
Import All required liberaries if they are not available install them using pip

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


Create Page configguration and options menu.
Create a briidge for MongoDB Atlas
Create Mysql Connaction
Setup youtube API Key
Create 7 functions which will work as follows.

get_channel_data = This function will pull the channel data based on channel id user input
get_video_ids_details = we need to extract video id based on channel_playlist_id = channel_data[0]['channel_playlist_id'] extrected from above above function
get_video_details = this function will extract video data details as per video id extracted from above function
get_comments_in_video = this function will extract comments data based on video ids
clean_data = this function clean data extracted from above 3 functions channel data video data and comment data 
push_data_mongodb = now data is ready to push to mongo DB this function will push above 3 data frame to 3 mongo db collection 
get_channel_name = this function will extract channnel name list from mongo db which will help in transforming data to mysql schama 

## Now we are prepared to devlop Streamlit web application based on 

Page is devided in 3 parts in horizontial Home, Extract & Transform , View

Home = Basic Infromation about project
Extract & Transform = devidecd in 2 parts 1 Extract & 2 Transform
Extract =  all code related to data extract from youtube api and cleaning and pushing to mongo db (using pymongo)
Transform = All mongo db data mappped to mysql schema and transformed (create tables and colouns named sql querry are attached) using pymongo
View = aable to answer all 10 question related to project.

## Note: for opening streamlit web application open anaconda terminal use > streamlit run youtube_st.py
## Any dificulties you mmay fase due to machine change and connectiion setup.



