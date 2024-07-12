package main

import (
	"fmt"
	"strings"
)

type ReplicationInfo struct {
	Role                       string
	ConnectedSlaves            int
	MasterReplid               string
	MasterReplOffset           int
	SecondReplOffset           int
	ReplBacklogActive          int
	ReplBacklogSize            int
	ReplBacklogFirstByteOffset int
	ReplBacklogHistlen         int
}

func (info *ReplicationInfo) ToString() string {
	sb := strings.Builder{}
	WriteLine(&sb, "# Replication")
	WriteLine(&sb, fmt.Sprintf("role:%s\n", info.Role))
	WriteLine(&sb, fmt.Sprintf("connected_slaves:%d\n", info.ConnectedSlaves))
	WriteLine(&sb, fmt.Sprintf("master_replid:%s\n", info.MasterReplid))
	WriteLine(&sb, fmt.Sprintf("master_repl_offset:%d\n", info.MasterReplOffset))
	WriteLine(&sb, fmt.Sprintf("second_repl_offset:%d\n", info.SecondReplOffset))
	WriteLine(&sb, fmt.Sprintf("repl_backlog_active:%d\n", info.ReplBacklogActive))
	WriteLine(&sb, fmt.Sprintf("repl_backlog_size:%d\n", info.ReplBacklogSize))
	WriteLine(&sb, fmt.Sprintf("repl_backlog_first_byte_offset:%d\n", info.ReplBacklogFirstByteOffset))
	WriteLine(&sb, fmt.Sprintf("ReplBack:%d\n", info.MasterReplOffset))

	return sb.String()
}
