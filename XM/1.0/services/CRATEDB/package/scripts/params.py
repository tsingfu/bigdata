#!/usr/bin/env python

from resource_management import *
from resource_management.libraries.script.script import Script

# server configurations
config = Script.get_config()

conf_dir = "/etc/crate"
crate_user = config['configurations']['crate-env']['crate_user']
user_group = config['configurations']['crate-env']['user_group']
log_dir = config['configurations']['crate-env']['crate_log_dir']
pid_dir = '/var/run/crate'
pid_file = '/var/run/crate/crate.pid'
hostname = config['hostname']
java64_home = config['hostLevelParams']['java_home']
crate_env_sh_template = config['configurations']['crate-env']['content']

cluster_name = config['configurations']['crate-site']['cluster_name']
seed_node1 = config['configurations']['crate-site']['seed_node1']
seed_node2 = config['configurations']['crate-site']['seed_node2']
seed_node3 = config['configurations']['crate-site']['seed_node3']

path_data = config['configurations']['crate-site']['path_data']
http_port = config['configurations']['crate-site']['http_port']
transport_tcp_port = config['configurations']['crate-site']['transport_tcp_port']

recover_after_time = config['configurations']['crate-site']['recover_after_time']
recover_after_data_nodes = config['configurations']['crate-site']['recover_after_data_nodes']
expected_data_nodes = config['configurations']['crate-site']['expected_data_nodes']
discovery_zen_ping_multicast_enabled = config['configurations']['crate-site']['discovery_zen_ping_multicast_enabled']
index_merge_scheduler_max_thread_count = config['configurations']['crate-site']['index_merge_scheduler_max_thread_count']
index_translog_flush_threshold_size = config['configurations']['crate-site']['index_translog_flush_threshold_size']
index_refresh_interval = config['configurations']['crate-site']['index_refresh_interval']
index_store_throttle_type = config['configurations']['crate-site']['index_store_throttle_type']
index_number_of_shards = config['configurations']['crate-site']['index_number_of_shards']
index_number_of_replicas = config['configurations']['crate-site']['index_number_of_replicas']
index_buffer_size = config['configurations']['crate-site']['index_buffer_size']
mlockall = config['configurations']['crate-site']['mlockall']
threadpool_bulk_queue_size = config['configurations']['crate-site']['threadpool_bulk_queue_size']
cluster_routing_allocation_node_concurrent_recoveries = config['configurations']['crate-site']['cluster_routing_allocation_node_concurrent_recoveries']
cluster_routing_allocation_disk_watermark_low = config['configurations']['crate-site']['cluster_routing_allocation_disk_watermark_low']
cluster_routing_allocation_disk_threshold_enabled = config['configurations']['crate-site']['cluster_routing_allocation_disk_threshold_enabled']
cluster_routing_allocation_disk_watermark_high = config['configurations']['crate-site']['cluster_routing_allocation_disk_watermark_high']
indices_fielddata_cache_size = config['configurations']['crate-site']['indices_fielddata_cache_size']
indices_cluster_send_refresh_mapping = config['configurations']['crate-site']['indices_cluster_send_refresh_mapping']
threadpool_index_queue_size = config['configurations']['crate-site']['threadpool_index_queue_size']

discovery_zen_ping_timeout = config['configurations']['crate-site']['discovery_zen_ping_timeout']
discovery_zen_fd_ping_interval = config['configurations']['crate-site']['discovery_zen_fd_ping_interval']
discovery_zen_fd_ping_timeout = config['configurations']['crate-site']['discovery_zen_fd_ping_timeout']
discovery_zen_fd_ping_retries = config['configurations']['crate-site']['discovery_zen_fd_ping_retries']

es_heap_size = config['configurations']['crate-site']['es_heap_size']