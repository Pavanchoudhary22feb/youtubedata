USE youtube;

CREATE TABLE channel_details (
channel_id VARCHAR(255),
channel_name VARCHAR(255) PRIMARY KEY,
channel_playlist_id VARCHAR(255),
channel_type  VARCHAR(255),
channel_subcription INT,
channel_views INT,
channel_total_videos INT,
channel_status DATETIME,
channel_description TEXT,
channel_status_DayName VARCHAR(255)
);

CREATE TABLE videodata (
channel_name VARCHAR(255),
channel_id VARCHAR(255),
video_id VARCHAR(255) PRIMARY KEY,
Title  VARCHAR(255),
Tags INT,
Thumbnail  VARCHAR(255),
video_Description  VARCHAR(10000),
Published_date DATETIME,
video_Duration  INT,
Views INT,
Likes INT,
Dislike INT,
Comments INT,
Favorite_count INT,
video_Definition VARCHAR(255),
Caption_status VARCHAR(255),
playlist_id VARCHAR(255),
pushblishDayName VARCHAR(255),
LikeRatio INT,
CommentRatio INT, 
    FOREIGN KEY (channel_name) REFERENCES channel_details(channel_name)
);

CREATE TABLE comment_data (
video_id VARCHAR(255),
comment_id VARCHAR(255),
comment_author VARCHAR(255),
comment_publish_date DATETIME,
comment_text VARCHAR(255),
Likes INT,
Reply INT,
comment_DayName VARCHAR(255),

    FOREIGN KEY (video_id) REFERENCES videodata(video_id)
);
