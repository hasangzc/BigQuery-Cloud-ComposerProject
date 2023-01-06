CREATE OR REPLACE TABLE `numeric-analogy-373619.airbnb_dataset.airbnb_data` AS
SELECT r.listing_id, r.host_name, r.last_review, p.price, t.room_type
FROM `numeric-analogy-373619.airbnb_staging_dataset.dataset_review` r
JOIN `numeric-analogy-373619.airbnb_staging_dataset.dataset_price` p
ON r.listing_id = p.listing_id
JOIN `numeric-analogy-373619.airbnb_staging_dataset.dataset_room` t
ON r.listing_id = t.listing_id
