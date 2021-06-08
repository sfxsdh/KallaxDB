# KallaxDB
KallaxDB is a table-less hash-based key-value store developed for storage hardware with built-in transparent compression. It saves each key-value pair to a specific location/page of a storage device according to the hashed key and keeps those overflow key-value pairs in a temporary store. Also, it performs background rehashing when the space allocated for database is not large enough. The source code of KallaxDB uses KVell as a reference (https://github.com/BLepers/KVell). 

# Environment set up
* Use `-o discard` option when mounting a storage device.
* Put RocksDB library 'librocksdb.so' or 'librocksdb.a' into the system path like '/usr/lib64'. Compile the RocksDB library using the source code which can be unzipped from rocksdb-master.zip in the directory. Refer to the compile instructions of RocksDB itself.
* `make` to compile KallaxDB source code.

# KallaxDB test
* KallaxDB integrates several YCSB tests including ycsba, ycsbb, ycsbc, ycsbd, ycsbe, ycsbf. There is also a write-only test which is called ycsbg. Use ycsba as an example, one test command is like  
`./bench --create_db=1 --nb_workers=16 --nb_rehash=4 --load_kv_num=1000000 --slab_size=81920 --nb_slabs=16 --request_kv_num=200000 --loader_num=4 --client_num=8 --db_bench=ycsba --value_size=400  --nb_paths=1 
--db_path=/kallaxdb`.  

* Description of key parameters in the command:

 Parameter  | Default value | Description
  --------- | ------------- | -----------
 create_db  | 1             | Whether to create a new database for test
 nb_workers | 1             | Number of background threads for read and write
 nb_rehash  | 1             | Number of rehash threads
 nb_slabs   | 1             | Number of slabs
 slab_size  | 1             | Total size of all slabs (MB)
 load_kv_num| 40000000      | Number of KVs for loading
 loader_num | 1             | Number of threads when loading
 client_num | 1             | Number of test threads
 value_size | 200           | Byte length of value
 key_size   | 16            | Byte length of key (minimum 8)
 page_size  | 4096          | Byte length of page size
 queue_depth| 64            | IO queue depth of each background thread
 nb_paths   | 0             | Number of paths for database files
 db_path    | null          | Absolute file paths for database; number of paths equal to nb_paths and paths are separated by comma like --db_path=/dir0,/dir1 
 db_bench   | ycsba         | Benchmark for test
