# raft

A multi-raft implementation built on top of the [CoreOS etcd raft library](https://github.com/etcd-io/etcd). 

## features  
- multi-raft support    
- snapshot manager   
- merged and compressed heartbeat message    
- check down replica      
- single raft's panic is allowed, detectable  
- new wal implementation    
- export more run status    
- implementation batch commit
