\# YouTube Data Analysis Using Hadoop



\## Project Overview



This project analyzes YouTube trending video data using Hadoop MapReduce and Apache Pig.



The project performs three MapReduce analyses and five Pig analyses on a YouTube trending dataset collected from Kaggle.



\## Dataset



Dataset Name: YouTube Trending Videos Stats 2026



Dataset Source:  

https://www.kaggle.com/datasets/bsthere/youtube-trending-videos-stats-2026



File Used:



US\_Trending.csv



Number of Raw Records:



16400



The dataset contains information such as:



\- Video ID

\- Trending Date

\- Video Title

\- Channel Title

\- Views

\- Likes

\- Dislikes

\- Publish Time

\- Category ID

\- Tags

\- Comments

\- Channel ID

\- Description



\## Technologies Used



\- Java 1.8

\- Hadoop 3.2.4

\- Apache Pig 0.18.0

\- Apache Maven

\- HDFS

\- YARN



\## Data Preprocessing



The original CSV dataset was processed using Java.



Apache Commons CSV was used to safely parse the CSV file because fields such as video titles, tags, and descriptions may contain commas.



The preprocessing program creates a tab-separated file:



US\_Trending\_Clean.tsv



Preprocessing Summary:



\- Raw Records: 16400

\- Skipped Records: 0

\- Unique Videos Kept: 16400



The processed dataset contains the following fields:



video\_id  

trending\_date  

title  

channel\_title  

views  

likes  

category\_id  

comments  

channel\_id



\## MapReduce Analysis



\### 1. Top 10 Categories by Number of Videos



Calculates the number of videos for each category and returns the top 10 categories.



Project Folder:



Top10Categories



\### 2. Top 10 Most Viewed Videos



Returns the top 10 videos with the highest number of views.



Project Folder:



Top10ViewedVideos



\### 3. Top 10 Channels by Number of Videos



Groups videos using channel ID and returns the top 10 channels based on the number of videos appearing in the dataset.



Project Folder:



Top10Channels



\## Apache Pig Analysis



Five Pig analyses were performed.



\### 1. Top 5 Categories



Returns the five categories containing the highest number of videos.



Script:



PigAnalysis/scripts/top5Categories.pig



\### 2. Top 10 Liked Videos



Returns the ten videos with the highest number of likes.



Script:



PigAnalysis/scripts/top10Liked.pig



\### 3. Top 10 Liked Videos by Category



Returns up to ten videos with the highest number of likes for each category.



Script:



PigAnalysis/scripts/top10LikedByCategories.pig



\### 4. Top 10 Viewed Videos



Returns the ten videos with the highest number of views.



Script:



PigAnalysis/scripts/top10Viewed.pig



\### 5. Top 10 Viewed Videos by Category



Returns up to ten videos with the highest number of views for each category.



Script:



PigAnalysis/scripts/top10ViewedByCategories.pig



\## Project Structure



```text

YouTube-Data-Analysis-using-Hadoop/

|

|-- Preprocessing/

|-- Top10Categories/

|-- Top10ViewedVideos/

|-- Top10Channels/

|

|-- PigAnalysis/

|   |-- scripts/

|   `-- outputFiles/

|

|-- MR Results/

|   `-- outputFiles/

|

|-- mergedataset/

|   |-- raw/

|   |-- processed/

|   |-- data\_dictionary.csv

|   `-- dataset\_source.txt

|

|-- screenshots/

|-- .gitignore

`-- README.md





\## Author



Name: MD Tarek Hossen



Student ID: 0222220005101004



Course: Big Data Analysis Lab

