MERGE INTO yt.channels AS tar
USING (
    SELECT 
        JSON_VALUE(value, '$.kind') AS kind,
        JSON_VALUE(value, '$.id') AS channel_id,
        JSON_VALUE(value, '$.snippet.title') AS channel_title,
        JSON_VALUE(value, '$.snippet.description') AS channel_description,
        JSON_VALUE(value, '$.statistics.viewCount') AS statistics_view_count,
        JSON_VALUE(value, '$.statistics.subscriberCount') AS subscribers,
        JSON_VALUE(value, '$.statistics.videoCount') AS statistics_video_count
    FROM yt.stg_channels
    CROSS APPLY OPENJSON(items)
) AS src
ON tar.channel_id = src.channel_id

WHEN MATCHED THEN
UPDATE SET 
    tar.kind = src.kind,
    tar.channel_title = src.channel_title,
    tar.channel_description = src.channel_description,
    tar.statistics_view_count = src.statistics_view_count,
    tar.subscribers = src.subscribers,
    tar.statistics_video_count = src.statistics_video_count,
    tar._elt_update_datetime = GETDATE()

WHEN NOT MATCHED THEN
INSERT 
(
    kind,
    channel_id,
    channel_title,
    channel_description,
    statistics_view_count,
    subscribers,
    statistics_video_count
)
VALUES
(
    src.kind,
    src.channel_id,
    src.channel_title,
    src.channel_description,
    src.statistics_view_count,
    src.subscribers,
    src.statistics_video_count
);