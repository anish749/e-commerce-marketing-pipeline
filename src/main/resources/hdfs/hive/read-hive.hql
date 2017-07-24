ALTER TABLE raw.omniturelogs
ADD PARTITION (date_ts = "${date_ts}")
LOCATION "${omniture_raw_path}"
;
