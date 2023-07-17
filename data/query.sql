SELECT
    audience_upload_id,
    c.name AS crm_list_name,
    c.data_source_category_id,
    c.advertiser_id,
    c.create_time,
    c.deprecated,
    c.entry_count
FROM
    ui.audience_uploads c
JOIN
    advertisers a USING (advertiser_id)
LEFT JOIN
    tpa.data_source_categories o USING (data_source_category_id)
WHERE
    audience_upload_id > 3336
    AND c.data_source_id != 9
    AND c.data_source_id != 8
    AND c.data_source_id != 10
ORDER BY
    1;
