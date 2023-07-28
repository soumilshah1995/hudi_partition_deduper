# hudi_partition_deduper
# Remove Duplicates from given Hudi Partition 
```
    helper_class = PartitionDeduper(
        hudi_path="file:///C:/tmp/hudidb/demo/",
        hudi_partition_to_dedup="country=IN",
        record_key="uuid",
        precom_key="uuid",
        hudi_table_name='demo',
        table_type='COPY_ON_WRITE',
        partition_fields='country',
        spark_session=spark
    )
    dedup_df = helper_class.dedup()

```
