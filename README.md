# Sparkify Data Lake Project with Spark
 Sparkify wants to move their date warehouse to a data lake. Specifically, I bulid an ETL pipline to extract their data from **S3** and processes them using **Spark**, and loads the data into a new **S3** as a set of *dimensional tables*. 
 
### Dataset 
Datasets used in this project are provided in two public **S3** buckets. One bucket contains info about songs and artists, the second bucket has info concerning actions done by users (which song are listening, etc.. ). The objects contained in both buckets are JSON files.

### Database Schema

#### Fact Table

##### songplays - records in log data associated with song plays i.e. records with page NextSong

**songplay_id, start_time, user_id, level, song_id, artist_id, session_id, location, user_agent**

#### Dimension Tables

##### users - users in the app
**user_id, first_name, last_name, gender, level**

##### songs - songs in music database
**song_id, title, artist_id, year, duration**

##### artists - artists in music database
**artist_id, name, location, lattitude, longitude**

##### time - timestamps of records in songplays broken down into specific units
**start_time, hour, day, week, month, year, weekday**


### Project Structure

+ `etl.py` - The ETL to reads data from **S3**, processes that data using **Spark**, and writes them to a new **S3**
+ `dl.cfg` - Configuration file that contains info about AWS credentials
+ `test.ipynb` - test file for `etl.py`

The ETL job processes the `song files` then the `log files`. The `song files` are listed and iterated over entering relevant information in the artists and the song folders in parquet. The `log files` are filtered by the *NextSong* action. The subsequent dataset is then processed to extract the date, time, year etc. fields and records are then appropriately entered into the `time`, `users` and `songplays` folders in parquet for analysis.
