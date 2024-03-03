# Youtube_Data_Harvesting_and_Warehousing
  A Python application is designed to collect, process, and analyze data from YouTube channels using the YouTube Data API, Python, Mongodb, SQL and Streamlit.

## Overview
  Python application is designed to collect, process, and analyze data from YouTube channels using the YouTube Data API. It leverages multiple data storage solutions, including MongoDB and PostgreSQL, to store and analyze the collected data. The primary components of this application include data retrieval from the API, data warehousing, and various data analysis functions.

## Features(ETL Zones)
### Data Collection Zone: 
You can input a YouTube channel ID, and the application will retrieve channel details, video details, and comment details from the YouTube Data API. The collected data is stored in a MongoDB database.
### Data Conversion Zone:
After collecting data in MongoDB, you can select a channel name, and the application will migrate the data from MongoDB to a PostgreSQL database for further analysis.
### Data Analysis Zone:
The application provides several pre-defined data analysis functions to extract insights from the collected data. These include statistics on video views, likes, dislikes, comments, and more.
