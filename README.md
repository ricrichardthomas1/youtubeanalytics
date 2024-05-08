# youtubeanalytics
 Youtube analytics using airflow and tableau

 This repo contains the files for the ETL+Data VIz project on YouTube Analytics:

WHat do we have here?
Through this project, I have used the YouTube Data API Python client to pull data from youtube on the most popular youtube videos in respective countries daily, and visualize it on Tableau.

Tools used:
Programming Language: Python 3.7.13
IDE: VS Code
Orchestration: Apache Airflow
Database: PostgreSQL
Database client: DBeaver
Viz: Tableau Desktop

The gist:
The simplified workflow is illustrated in ___________. 

Basic Outline:

1. API requests:
1. Fetch details of the 'mostPopular' videos for the respective country using the videos.list() method of the youtube API client.
2. Fetch channel details of videos using channel.list() method.
3. Fetch video category details using the videocategories.list() method.
2. 
