MERGE INTO yt.videos AS tar
USING (
    SELECT
        JSON_VALUE(value, '$.kind') AS kind,
        JSON_VALUE(value, '$.id') AS video_id,
        JSON_VALUE(value, '$.snippet.channelId') AS channel_id,
        JSON_VALUE(value, '$.snippet.channelTitle') AS channel_title,
        JSON_VALUE(value, '$.snippet.title') AS video_title,
        JSON_VALUE(value, '$.snippet.description') AS video_description,
        JSON_VALUE(value, '$.snippet.publishedAt') AS published_at,
        JSON_VALUE(value, '$.contentDetails.duration') AS duration,
        JSON_VALUE(value, '$.contentDetails.dimension') AS dimension,
        JSON_VALUE(value, '$.contentDetails.definition') AS definition,
        JSON_VALUE(value, '$.statistics.viewCount') AS view_count,
        JSON_VALUE(value, '$.statistics.likeCount') AS like_count,
        JSON_VALUE(value, '$.statistics.commentCount') AS comment_count
    FROM yt.stg_videos
    CROSS APPLY OPENJSON(items)
) AS src

ON tar.video_id = src.video_id

WHEN MATCHED THEN
UPDATE SET
    tar.kind = src.kind,
    tar.channel_id = src.channel_id,
    tar.channel_title = src.channel_title,
    tar.video_title = src.video_title,
    tar.video_description = src.video_description,
    tar.published_at = src.published_at,
    tar.duration = src.duration,
    tar.dimension = src.dimension,
    tar.definition = src.definition,
    tar.view_count = src.view_count,
    tar.like_count = src.like_count,
    tar.comment_count = src.comment_count,
    tar._elt_update_datetime = GETDATE()

WHEN NOT MATCHED THEN
INSERT (
    kind,
    video_id,
    channel_id,
    channel_title,
    video_title,
    video_description,
    published_at,
    duration,
    dimension,
    definition,
    view_count,
    like_count,
    comment_count
)
VALUES (
    src.kind,
    src.video_id,
    src.channel_id,
    src.channel_title,
    src.video_title,
    src.video_description,
    src.published_at,
    src.duration,
    src.dimension,
    src.definition,
    src.view_count,
    src.like_count,
    src.comment_count
);