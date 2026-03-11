IF NOT EXISTS (
    SELECT *
    FROM INFORMATION_SCHEMA.TABLES
    WHERE TABLE_NAME = 'channels'
    AND TABLE_SCHEMA = 'yt'
)
BEGIN
    CREATE TABLE yt.channels(
        kind VARCHAR(100),
        channel_id VARCHAR(100),
        channel_title VARCHAR(500),
        channel_description VARCHAR(MAX),
        statistics_view_count BIGINT,
        subscribers BIGINT,
        statistics_video_count INT,
        _elt_insert_datetime DATETIME2 DEFAULT GETDATE(),
        _elt_update_datetime DATETIME2 DEFAULT GETDATE()
    )
END;

IF NOT EXISTS (
    SELECT *
    FROM INFORMATION_SCHEMA.TABLES
    WHERE TABLE_NAME = 'playlists'
    AND TABLE_SCHEMA = 'yt'
)
BEGIN
    CREATE TABLE yt.playlists(
        kind VARCHAR(100),
        playlist_id VARCHAR(100),
        channel_id VARCHAR(100),
        playlist_title VARCHAR(500),
        playlist_description VARCHAR(MAX),
        published_at DATETIME2,
        item_count INT,
        _elt_insert_datetime DATETIME2 DEFAULT GETDATE(),
        _elt_update_datetime DATETIME2 DEFAULT GETDATE()
    )
END;
