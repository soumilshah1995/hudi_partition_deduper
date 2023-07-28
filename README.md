
# Watch 
![image](https://github.com/soumilshah1995/hudi_partition_deduper/assets/39345855/2ee596e7-eb2f-4818-9c3f-3a788a2d36e8)

#### Video
https://www.youtube.com/watch?v=ue5M0_euwis


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
