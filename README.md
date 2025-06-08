# bustub-public
bustub是一个小型的关系型数据库，主要实现了缓冲区、索引、语句执行、并发控制。

缓冲区有LRU和LRU-K两种底层设计，所有Page操作均在BufferPool上进行。缓冲区部分实现文件：
src/buffer/lru_replacer.cpp、
src/buffer/buffer_pool_manager_instance.cpp、
src/buffer/parallel_buffer_pool_manager.cpp

在索引层面实现了可扩展哈希表，索引部分实现文件：
src/storage/page/hash_table_directory_page.cpp、
src/storage/page/hash_table_bucket_page.cpp、
src/container/hash/extendible_hash_table.cpp

语句执行采用火山模型，支持select、insert、delete、update、join、aggregation、limit、distinct操作。语句执行部分的实现文件：
src/execution/seq_scan_executor.cpp、
src/execution/insert_executor.cpp、
src/execution/update_executor.cpp、
src/execution/delete_executor.cpp、
src/execution/nested_loop_join_executor.cpp、
src/execution/hash_join_executor.cpp、
src/execution/aggregation_executor.cpp、
src/execution/limit_executor.cpp、
src/execution/distinct_executor.cpp

并发控制采用两阶段锁设计，实现未提交读、提交读、可重复读三种隔离级别，通过wound-wait算法预防死锁。并发控制部分的实现文件：
src/concurrency/lock_manager.cpp
