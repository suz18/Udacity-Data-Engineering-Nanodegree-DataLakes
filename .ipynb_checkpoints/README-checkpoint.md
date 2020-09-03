### Project: Data Lake

#### Introduction
A music streaming startup, Sparkify, has grown their user base and song database and want to move their data warehouse to a data lake. Their data resides in S3, in a directory of JSON logs on user activity on the app, as well as a directory with JSON metadata on the songs in their app.

The purpose of this project is to build an ETL pipeline that extracts song and log data from S3, processes them using Spark, and loads the data back into S3 as a set of dimensional tables. This will allow the analytics team to continue finding insights in what songs the users are listening to.

You'll be able to test your database and ETL pipeline by running queries given to you by the analytics team from Sparkify and compare your results with their expected results.

#### Database Schema Design
The database schema design adopted in this is a star schema so that it can be optimized for queries on song play analysis.     
There is one fact table, songplays, which records in event data associated with song plays.      
The songplays fact table is accompanied with four dimension tables, users, songs, artists and time.     

##### Fact Table
1. songplays - records in log data associated with song plays i.e. records with page NextSong
• songplay_id, start_time, user_id, level, song_id, artist_id, session_id, location, user_agent

##### Dimension Tables
2. users - users in the app
• user_id, first_name, last_name, gender, level
3. songs - songs in music database
• song_id, title, artist_id, year, duration
4. artists - artists in music database
• artist_id, name, location, lattitude, longitude
5. time - timestamps of records in songplays broken down into specific units
• start_time, hour, day, week, month, year, weekday

#### ETL pipeline
The ETL pipeline built for this project is as the below:
- Source data log and songs are loaded from S3.
- The data is processed into analytics tables using Spark. 
- Analytics tables are then loaded back into S3. 