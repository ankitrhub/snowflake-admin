

select
 query_id, step_id, operator_id, operator_type
 , operator_attributes:table_name::string tablename
 , array_size(operator_attributes:columns) table_ncols
 , execution_time_breakdown:overall_percentage::float*100 as time_percent
 , execution_time_breakdown:local_disk_io::float*100 local_disk_io
 , execution_time_breakdown:processing::float*100 processing
 , execution_time_breakdown:synchronization::float*100 synchronization
 , operator_statistics:io:bytes_scanned::int bytes_scanned
 , operator_statistics:io:percentage_scanned_from_cache::float percentage_scanned_from_cache
 , operator_statistics:network:network_bytes::int network_bytes
 , operator_statistics:spilling:bytes_spilled_local_storage::int local_spilled_bytes
 , operator_statistics:pruning:partitions_scanned::int scanned 
 , operator_statistics:pruning:partitions_total::int total
 , scanned/total ratio
From table(get_query_operator_stats(last_query_id()))
order by time_percent desc 
