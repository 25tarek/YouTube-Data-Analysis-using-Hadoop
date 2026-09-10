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

grouped = GROUP data BY category_id;

counts = FOREACH grouped GENERATE
    group AS category_id,
    COUNT(data) AS video_count;

sorted = ORDER counts BY video_count DESC;

top5 = LIMIT sorted 5;

DUMP top5;

STORE top5
INTO '/user/DELL/youtube_project/output/pig/top5_categories'
USING PigStorage('\t');