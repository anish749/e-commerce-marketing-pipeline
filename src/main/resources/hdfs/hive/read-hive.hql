ALTER TABLE raw.omniturelogs
ADD IF NOT EXISTS PARTITION (date_ts = "${date_ts}")
LOCATION "${omniture_raw_path}"
;
