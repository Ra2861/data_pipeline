CREATE TABLE IF NOT EXISTS default.cat_images (
  image_url String,
  download_date DateTime,  -- Used for partitioning
  PRIMARY KEY (download_date)  -- Only download_date in primary key
) ENGINE = MergeTree()
ORDER BY (download_date);  -- Only download_date used for sorting (consistent)
