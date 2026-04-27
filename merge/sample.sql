SELECT 
	kind , 
	JSON_VALUE(items, '$[0].id') as channel_id,
	JSON_VALUE(items, '$[0].statistics.viewCount') as statistics_view_count,
	JSON_VALUE(items, '$[0].statistics.subscriberCount') as statistics_subscriber_count,
	JSON_VALUE(items, '$[0].statistics.videoCount') as statistics_video_count
FROM yt.stg_channels;


select * from  yt.channel_details;

MERGE INTO yt.channel_details as tar
using (
	SELECT 
		kind , 
		JSON_VALUE(items, '$[0].id') as channel_id,
		JSON_VALUE(items, '$[0].statistics.viewCount') as statistics_view_count,
		JSON_VALUE(items, '$[0].statistics.subscriberCount') as statistics_subscriber_count,
		JSON_VALUE(items, '$[0].statistics.videoCount') as statistics_video_count
	FROM yt.stg_channels
) as src
on tar.channel_id = src.channel_id
when matched then
update set 
	tar.statistics_view_count = src.statistics_view_count,
	tar.statistics_subscriber_count = src.statistics_subscriber_count,
	tar.statistics_video_count = src.statistics_video_count,
	tar._elt_update_datetime = getdate()
when not matched then
	insert 
	(
	    channel_id,
	    statistics_view_count,
	    statistics_subscriber_count,
	    statistics_video_count
	)
	values(
	    src.channel_id,
	    src.statistics_view_count,
	    src.statistics_subscriber_count,
	    src.statistics_video_count
	);

    