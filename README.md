# MapReduce 
MapReduce is a programming model and an associated implementation for processing large data sets. The general MapReduce program includes a map task, which filters and sorts input data, and a reduce task, which performs a summary operation. The map and reduce tasks can be divided among multiple worker nodes and executed in parallel. A master node orchestrates the communications and data transfers between the various nodes. The key benefits of MapReduce include scalability and fault-tolerance.

For the purpose of solving a word count problem, this MapReduce implementation contains the following stages:
1. Map : Worker nodes process input files and produce intermediate files with key-value pairs.
2. Reduce : Worker nodes aggregate key-value pairs from the map stage and save the aggregated results as reduced intermediate files.
3. Merge : The master node combines reduced intermediate output files from all reducers into a single consolidated output file.

For this implementation of MapReduce, each input file is processed by a separate worker node executing a mapper task. The mapper task generates <key, count> pairs and assigns pairs between as many intermediate files as there are reducers. Hashing the keys using the C++ hash function allows pairs with matching keys to be assigned to the same intermediate file. Once all intermediate files are generated, the master node assigns these to workers. The workers execute the reducer task on corresponding intermediate files. These generate new <key, count> pairs representing the number of instances of each keyword. Finally, the master node consolidates reducer output into a single sorted combined output file.

Master-worker communication is handled by sending messages over sockets between the master node and worker nodes. The messages allow the master node to tell worker nodes when to start map and reduce tasks. The messages also allow worker nodes to communicate their statuses (ready or task completed) to the master node.

Task scheduling is executed on a “first come, first served” basis. The master node initially directs worker nodes to execute the map task. As input files are mapped, the master node will direct freed worker nodes to any remaining unprocessed input files. The master node waits until all inputs have been processed before directing worker nodes to execute the reduce task. The master node again waits until all intermediate files are processed before executing the merge task and combining the reduced files into a single output file.
