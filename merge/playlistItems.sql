MERGE yt.playlist_items AS tar
USING (
    SELECT
        JSON_VALUE(value, '$.kind') AS kind,
        JSON_VALUE(value, '$.id') AS playlist_item_id,
        JSON_VALUE(value, '$.snippet.playlistId') AS playlist_id,
        JSON_VALUE(value, '$.contentDetails.videoId') AS video_id,
        JSON_VALUE(value, '$.snippet.channelId') AS channel_id,
        JSON_VALUE(value, '$.snippet.title') AS video_title,
        JSON_VALUE(value, '$.snippet.description') AS video_description,
        JSON_VALUE(value, '$.snippet.publishedAt') AS published_at,
        JSON_VALUE(value, '$.contentDetails.videoPublishedAt') AS video_published_at,
        JSON_VALUE(value, '$.snippet.position') AS position,
        JSON_VALUE(value, '$.status.privacyStatus') AS privacy_status
    FROM yt.stg_playlist_items
    CROSS APPLY OPENJSON(json_data, '$.items')
) AS src
ON
    tar.playlist_item_id = src.playlist_item_id

WHEN NOT MATCHED BY TARGET THEN
INSERT (
    kind,
    playlist_item_id,
    playlist_id,
    video_id,
    channel_id,
    video_title,
    video_description,
    published_at,
    video_published_at,
    position,
    privacy_status
)
VALUES (
    src.kind,
    src.playlist_item_id,
    src.playlist_id,
    src.video_id,
    src.channel_id,
    src.video_title,
    src.video_description,
    src.published_at,
    src.video_published_at,
    src.position,
    src.privacy_status
)

WHEN MATCHED THEN
UPDATE SET
    tar.kind = src.kind,
    tar.playlist_id = src.playlist_id,
    tar.video_id = src.video_id,
    tar.channel_id = src.channel_id,
    tar.video_title = src.video_title,
    tar.video_description = src.video_description,
    tar.published_at = src.published_at,
    tar.video_published_at = src.video_published_at,
    tar.position = src.position,
    tar.privacy_status = src.privacy_status,
    tar._elt_update_datetime = GETDATE();