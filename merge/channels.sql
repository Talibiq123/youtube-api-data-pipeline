MERGE yt.channels as tar
USING (
    SELECT
        JSON_VALUE(json_data, '$.items[0].kind') as kind,
        JSON_VALUE(json_data, '$.items[0].id') as channel_id,
        JSON_VALUE(json_data, '$.items[0].snippet.title') as channel_title,
        JSON_VALUE(json_data, '$.items[0].snippet.description') as channel_description,
        JSON_VALUE(json_data, '$.items[0].statistics.viewCount') as statistics_view_count,
        JSON_VALUE(json_data, '$.items[0].statistics.subscriberCount') AS subscribers,
        JSON_VALUE(json_data, '$.items[0].statistics.videoCount') as statistics_video_count
        from yt.stg_channels
) as src
ON
    tar.channel_id = src.channel_id
WHEN NOT MATCHED BY TARGET THEN
INSERT (
    kind,
    channel_id,
    channel_title,
    channel_description,
    statistics_view_count,
    subscribers,
    statistics_video_count
)
VALUES (
    src.kind,
    src.channel_id,
    src.channel_title,
    src.channel_description,
    src.statistics_view_count,
    src.subscribers,
    src.statistics_video_count
)
WHEN MATCHED THEN UPDATE
    SET
        tar.kind=src.kind,
        tar.channel_title=src.channel_title,
        tar.channel_description=src.channel_description,
        tar.statistics_view_count=src.statistics_view_count,
        tar.subscribers=src.subscribers,
        tar.statistics_video_count=src.statistics_video_count;
