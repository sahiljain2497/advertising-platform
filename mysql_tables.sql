#create ads table

CREATE TABLE `ads` (
  `text` text NOT NULL,
  `category` varchar(1000) NOT NULL,
  `keywords` varchar(4000) NOT NULL,
  `campaign_id` varchar(500) NOT NULL,
  `status` varchar(500) NOT NULL,
  `target_gender` varchar(1000) NOT NULL,
  `target_age_start` int(1) NOT NULL,
  `target_age_end` int(100) NOT NULL,
  `target_city` varchar(1000) NOT NULL,
  `target_state` varchar(1000) NOT NULL,
  `target_country` varchar(1000) NOT NULL,
  `target_income_bucket` varchar(1000) NOT NULL,
  `target_device` varchar(1000) NOT NULL,
  `cpc` double NOT NULL,
  `cpa` double NOT NULL,
  `cpm` double NOT NULL,
  `budget` double NOT NULL,
  `current_slot_budget` double NOT NULL,
  `date_range_start` varchar(1000) NOT NULL,
  `date_range_end` varchar(1000) NOT NULL,
  `time_range_start` varchar(1000) NOT NULL,
  `time_range_end` varchar(1000) NOT NULL,
  PRIMARY KEY (`campaign_id`)
) ENGINE=InnoDB DEFAULT CHARSET=utf8;

#alter ads table to save emoji

ALTER TABLE
    `ads`
    CONVERT TO CHARACTER SET utf8mb4
    COLLATE utf8mb4_unicode_ci;

#create server_ads table

CREATE TABLE `served_ads` (
  `request_id` varchar(1000) NOT NULL,
  `campaign_id` varchar(1000) NOT NULL,
  `user_id` varchar(1000) DEFAULT NULL,
  `auction_cpm` double NOT NULL,
  `auction_cpc` double NOT NULL,
  `auction_cpa` double NOT NULL,
  `target_age_range` varchar(1000) NOT NULL,
  `target_location` varchar(1000) NOT NULL,
  `target_gender` varchar(1000) NOT NULL,
  `target_income_bucket` varchar(1000) NOT NULL,
  `target_device_type` varchar(1000) NOT NULL,
  `campaign_start_time` varchar(1000) NOT NULL,
  `campaign_end_time` varchar(1000) NOT NULL,
  `timestamp` timestamp NOT NULL,
  PRIMARY KEY (`request_id`)
) ENGINE=InnoDB DEFAULT CHARSET=utf8;

#ad index for performance

ALTER TABLE served_ads ADD INDEX (campaign_id);
ALTER TABLE served_ads ADD INDEX (user_id);

#create users table

CREATE TABLE `users` (
  `id` varchar(1000) NOT NULL,
  `age` int(100) NOT NULL,
  `gender` varchar(50) NOT NULL,
  `internet_usage` varchar(100) NOT NULL,
  `income_bucket` varchar(100) NOT NULL,
  `user_agent_string` varchar(1000) NOT NULL,
  `device_type` varchar(1000) NOT NULL,
  `websites` varchar(1000) NOT NULL,
  `movies` varchar(1000) NOT NULL,
  `music` varchar(1000) NOT NULL,
  `programs` varchar(1000) NOT NULL,
  `books` varchar(1000) NOT NULL,
  `negatives` varchar(4000) NOT NULL,
  `positives` varchar(4000) NOT NULL,
  PRIMARY KEY (`id`)
) ENGINE=InnoDB DEFAULT CHARSET=utf8;

#load users data from csv

LOAD data local infile '~/capstone/users_500k.csv' INTO TABLE users fields terminated by '|' IGNORE 1 LINES;

-- CREATE TABLE `user_feedback` (
--   `campaign_id` varchar(500) NOT NULL,
--   `user_id` varchar(500) NULL,
--   `request_id` varchar(100) NOT NULL,
--   `click` integer(100) NOT NULL,
--   `view` integer NOT NULL,
--   `acquisition` integer NOT NULL,
--   `auction_cpm` double NOT NULL,
--   `auction_cpc` double NOT NULL,
--   `auction_cpa` double NOT NULL,
--   `target_age_range` varchar(1000) NULL,
--   `target_location` varchar(1000) NULL,
--   `target_gender` varchar(1000) NULL,
--   `target_income_bucket` varchar(1000) NULL,
--   `target_device_type` varchar(1000) NULL,
--   `campaign_start_time` varchar(1000) NOT NULL,
--   `campaign_end_time` varchar(1000) NOT NULL,
--   `user_action` varchar(1000) NOT NULL,
--   `expenditure` varchar(1000) NOT NULL,
--   `timestamp` timestamp NOT NULL,
--   PRIMARY KEY (`request_id`)
-- ) ENGINE=InnoDB DEFAULT CHARSET=utf8;
