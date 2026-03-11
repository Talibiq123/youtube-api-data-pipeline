MERGE yt.playlists AS tar
USING (
    SELECT
        JSON_VALUE(value, '$.kind') AS kind,
        JSON_VALUE(value, '$.id') AS playlist_id,
        JSON_VALUE(value, '$.snippet.channelId') AS channel_id,
        JSON_VALUE(value, '$.snippet.title') AS playlist_title,
        JSON_VALUE(value, '$.snippet.description') AS playlist_description,
        JSON_VALUE(value, '$.snippet.publishedAt') AS published_at,
        JSON_VALUE(value, '$.contentDetails.itemCount') AS item_count
    FROM yt.stg_playlists
    CROSS APPLY OPENJSON(json_data, '$.items')
) AS src
ON
    tar.playlist_id = src.playlist_id
WHEN NOT MATCHED BY TARGET THEN
INSERT (
    kind,
    playlist_id,
    channel_id,
    playlist_title,
    playlist_description,
    published_at,
    item_count
)
VALUES (
    src.kind,
    src.playlist_id,
    src.channel_id,
    src.playlist_title,
    src.playlist_description,
    src.published_at,
    src.item_count
)
WHEN MATCHED THEN
UPDATE SET
    tar.kind = src.kind,
    tar.channel_id = src.channel_id,
    tar.playlist_title = src.playlist_title,
    tar.playlist_description = src.playlist_description,
    tar.published_at = src.published_at,
    tar.item_count = src.item_count;