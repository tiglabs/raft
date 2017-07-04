package raft

type (
	fsmState     byte
	replicaState byte
)

const (
	stateFollower    fsmState = 0
	stateCandidate            = 1
	stateLeader               = 2
	stateElectionACK          = 3

	replicaStateProbe     replicaState = 0
	replicaStateReplicate              = 1
	replicaStateSnapshot               = 2
)

func (st fsmState) String() string {
	switch st {
	case 0:
		return "StateFollower"
	case 1:
		return "StateCandidate"
	case 2:
		return "StateLeader"
	case 3:
		return "StateElectionACK"
	}
	return ""
}

func (st replicaState) String() string {
	switch st {
	case 1:
		return "ReplicaStateReplicate"
	case 2:
		return "ReplicaStateSnapshot"
	default:
		return "ReplicaStateProbe"
	}
}
