# raft

A multi-raft implementation based on the [coreos/etcd](https://github.com/coreos/etcd) raft library.


## features  
- multi-raft support    
- snapshopt manager   
- merged and compressed heartbeat message    
- check down replica      
- single raft's panic is allowed, detectable  
- new wal implementation    
- export more run status    
- implementation batch commit
