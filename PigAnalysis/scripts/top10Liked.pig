records = LOAD '/user/DELL/youtube_project/clean/US_Trending_Clean.tsv'
USING PigStorage('\t')
AS (
    video_id:chararray,
    trending_date:chararray,
    title:chararray,
    channel_title:chararray,
    views:long,
    likes:long,
    category_id:chararray,
    comments:long,
    channel_id:chararray
);

data = FILTER records BY video_id != 'video_id';

selected = FOREACH data GENERATE
    video_id,
    title,
    channel_title,
    likes;

sorted = ORDER selected BY likes DESC;

top10 = LIMIT sorted 10;

DUMP top10;

STORE top10
INTO '/user/DELL/youtube_project/output/pig/top10_liked'
USING PigStorage('\t');