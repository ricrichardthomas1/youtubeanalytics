# youtubeanalytics
 Youtube analytics using airflow and tableau

 This repo contains the files for the ETL+Data VIz project on YouTube Analytics:

## What do we have here?
Through this project, I have used the [YouTube Data API](https://developers.google.com/youtube/v3) to pull data from youtube on the most popular youtube videos in respective countries daily, and visualize it on Tableau.

## Tools used:
- Programming: [Python 3.7.13](https://www.python.org/downloads/release/python-3713/), [Pandas](https://pandas.pydata.org/), [Youtube API Python client Library](https://github.com/googleapis/google-api-python-client)
- IDE: [VS Code](https://code.visualstudio.com/)
- Orchestration:[Apache Airflow](https://airflow.apache.org/) running on [Docker containers](https://www.docker.com/)
- Database: [PostgreSQL](https://www.postgresql.org/)
- Database client: [DBeaver](https://dbeaver.io/)
- Viz: [Tableau Desktop](https://www.tableau.com/support/releases/desktop/2024.1.2)

## Basic Outline

Below image depicts the basic process flow:

![Image depicting workflow](/ELT.png) 

**1. Extract**: Requests were made to the Youtube Data API using the python client library to:
   - Fetch details of the 'mostPopular' videos for the respective country using the [videos.list()](https://googleapis.github.io/google-api-python-client/docs/dyn/youtube_v3.videos.html#list) method of the youtube API client.
   - Fetch channel details of videos using [channel.list()](https://googleapis.github.io/google-api-python-client/docs/dyn/youtube_v3.channels.html#list) method.
   - Fetch video category details using the [videocategories.list()](https://googleapis.github.io/google-api-python-client/docs/dyn/youtube_v3.videoCategories.html#list) method.

**2. Transform**: Used pandas to flatten and transform the json files received as the API responses for respective requests into dataframes mirroring the format of the tables the data needs to be stored as in a PostgreSQL database.

**3. Load**: Loaded the final transformed data to SQL tables in a Postgres Database hosted on localsystem

The above ETL pipeline is run daily by wrapping the necessary python functions into tasks of an [Airflow dag](/youtube_vids.py) and scheduled to run daily.

**4. Visualize** the Data on Tableau [Published Viz on Tableau Public](https://public.tableau.com/app/profile/richard.t.vetticad/viz/PopularYouTubeVideos/WhatstheworldwatchingonYouTube?publish=yes)

