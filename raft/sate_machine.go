package raft

import (
	"context"
	"encoding/json"
	"errors"
	"io"
	"raft-redis-cluster/store"

	"github.com/hashicorp/raft"
)

var _ raft.FSM = (*StateMachine)(nil)

type Op int

const (
	Put Op = iota
	Del
)

type KVCmd struct {
	Op  Op     `json:"op"`
	Key []byte `json:"key"`
	Val []byte `json:"val"`
}

func NewStateMachine(store store.Store) *StateMachine {
	return &StateMachine{store: store}
}

type StateMachine struct {
	store store.Store
}

func (s *StateMachine) Apply(log *raft.Log) any {
	ctx := context.Background()
	c := &KVCmd{}

	err := json.Unmarshal(log.Data, c)
	if err != nil {
		return err
	}

	return s.handleRequest(ctx, c)
}

func (s *StateMachine) Restore(rc io.ReadCloser) error {
	return s.store.Restore(rc)
}

func (s *StateMachine) Snapshot() (raft.FSMSnapshot, error) {
	rc, err := s.store.Snapshot()
	if err != nil {
		return nil, err
	}
	return &KVSnapshot{rc}, nil
}

var ErrUnknownOp = errors.New("unknown op")

func (s *StateMachine) handleRequest(ctx context.Context, c *KVCmd) error {
	switch c.Op {
	case Put:
		return s.store.Put(ctx, c.Key, c.Val)
	case Del:
		return s.store.Delete(ctx, c.Key)
	default:
		return ErrUnknownOp
	}
}
