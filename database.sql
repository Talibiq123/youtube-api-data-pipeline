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

IF NOT EXISTS (
    SELECT *
    FROM INFORMATION_SCHEMA.TABLES
    WHERE TABLE_NAME = 'playlist_items'
    AND TABLE_SCHEMA = 'yt'
)
BEGIN
    CREATE TABLE yt.playlist_items(
        kind VARCHAR(100),
        playlist_item_id VARCHAR(200),
        playlist_id VARCHAR(100),
        video_id VARCHAR(50),
        channel_id VARCHAR(100),
        video_title VARCHAR(500),
        video_description VARCHAR(MAX),
        published_at DATETIME2,
        video_published_at DATETIME2,
        position INT,
        privacy_status VARCHAR(50),
        _elt_insert_datetime DATETIME2 DEFAULT GETDATE(),
        _elt_update_datetime DATETIME2 DEFAULT GETDATE()
    )
END;

