# raft
A multi-raft implementation built on top of the CoreOS etcd raft library  [coreos/etcd](https://github.com/coreos/etcd)


## features  
- multi-raft support    
- snapshopt manager   
- merged and compressed heartbeat message    
- check down replica      
- single raft's panic is allowed, detectable  
- new wal implementation    
- export more run status    
- implementation batch commit
