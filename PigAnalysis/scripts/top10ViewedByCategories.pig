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

top10_by_category = FOREACH grouped {
    ordered = ORDER data BY views DESC;
    limited = LIMIT ordered 10;

    GENERATE
        group AS category_id,
        FLATTEN(
            limited.(
                video_id,
                title,
                channel_title,
                views
            )
        );
};

DUMP top10_by_category;

STORE top10_by_category
INTO '/user/DELL/youtube_project/output/pig/top10_viewed_by_category'
USING PigStorage('\t');