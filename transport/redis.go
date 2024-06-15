package transport

import (
	"context"
	"encoding/json"
	"errors"
	"log"
	"net"
	"raft-redis-cluster/raft"
	"raft-redis-cluster/store"
	"strings"
	"time"

	"github.com/tidwall/redcon"

	hraft "github.com/hashicorp/raft"
)

type Redis struct {
	listen      net.Listener
	store       store.Store
	stableStore hraft.StableStore
	id          hraft.ServerID
	raft        *hraft.Raft
}

func NewRedis(id hraft.ServerID, raft *hraft.Raft, store store.Store, stableStore hraft.StableStore) *Redis {
	return &Redis{
		store:       store,
		stableStore: stableStore,
		id:          id,
		raft:        raft,
	}
}

func (r *Redis) Serve(addr string) error {
	var err error
	r.listen, err = net.Listen("tcp", addr)
	if err != nil {
		return err
	}
	return r.handle()
}

func (r *Redis) handle() error {
	return redcon.Serve(r.listen, func(conn redcon.Conn, cmd redcon.Command) {
		err := r.validateCmd(cmd)
		if err != nil {
			conn.WriteError(err.Error())
			return
		}
		r.processCmd(conn, cmd)
	}, func(conn redcon.Conn) bool {
		return true
	},
		func(conn redcon.Conn, err error) {
			if err != nil {
				log.Default().Printf("redis: %v", err)
			}
		},
	)
}

var argsLen = map[string]int{
	"GET": 2,
	"SET": 3,
	"DEL": 2,
}

const (
	commandName = 0
	keyName     = 1
	value       = 2
)

func (r *Redis) validateCmd(cmd redcon.Command) error {
	if len(cmd.Args) == 0 {
		return errors.New("empty command")
	}
	if len(cmd.Args) < argsLen[string(cmd.Args[commandName])] {
		return errors.New("invalid command")
	}

	plainCmd := strings.ToUpper(string(cmd.Args[commandName]))
	if len(cmd.Args) != argsLen[plainCmd] {
		return errors.New("invalid command")
	}

	return nil
}

func (r *Redis) processCmd(conn redcon.Conn, cmd redcon.Command) {
	ctx := context.Background()

	if r.raft.State() != hraft.Leader {
		_, lid := r.raft.LeaderWithID()
		add, err := store.GetRedisAddrByNodeID(r.stableStore, lid)
		if err != nil {
			conn.WriteError(err.Error())
			return
		}
		conn.WriteError("MOVED -1" + add)
		return
	}

	plainCmd := strings.ToUpper(string(cmd.Args[commandName]))
	switch plainCmd {
	case "GET":
		val, err := r.store.Get(ctx, cmd.Args[keyName])
		if err != nil {
			switch {
			case errors.Is(err, store.ErrKeyNotFound):
				conn.WriteNull()
				return
			default:
				conn.WriteError(err.Error())
			}
		}
		conn.WriteBulk(val)
	case "SET":
		kvCmd := &raft.KVCmd{
			Op:  raft.Put,
			Key: cmd.Args[keyName],
			Val: cmd.Args[value],
		}
		b, err := json.Marshal(kvCmd)
		if err != nil {
			conn.WriteError(err.Error())
			return
		}
		f := r.raft.Apply(b, time.Second*1)
		if f.Error() != nil {
			conn.WriteError(f.Error().Error())
			return
		}
		conn.WriteString("OK")
	case "DEL":
		kvCmd := &raft.KVCmd{
			Op:  raft.Del,
			Key: cmd.Args[keyName],
		}
		b, err := json.Marshal(kvCmd)
		if err != nil {
			conn.WriteError(err.Error())
			return
		}
		f := r.raft.Apply(b, time.Second*1)
		if f.Error() != nil {
			conn.WriteError(f.Error().Error())
			return
		}
		res := f.Response()
		err, ok := res.(error)
		if ok {
			conn.WriteError(err.Error())
			return
		}
		conn.WriteInt(1)
	default:
		conn.WriteError("unknown command")
	}
}

func (r *Redis) Close() error {
	return r.listen.Close()
}

func (r *Redis) Addr() net.Addr {
	return r.listen.Addr()
}
