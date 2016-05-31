package thrift_client_pool

import (
	"errors"
	"sync"

	"math/rand"
	"reflect"
	"time"

	"sync/atomic"

	"git.apache.org/thrift.git/lib/go/thrift"
)

var (
	ErrPoolClosed                  = errors.New("pool has been closed")
	ErrPoolMaxOpenReached          = errors.New("pool max open client limit reached")
	ErrClientMissingTransportField = errors.New("client missing transport field")
	ErrClientNilTransportField     = errors.New("client transport field is nil")
	errNoPooledClient              = errors.New("No pooled client")
)

type Client interface{}
type PooledClient interface {
	Close() error
	RawClient() Client
	MarkUnusable()
}

// client pool interface
type ClientPool interface {
	// get client from pool
	Get() (PooledClient, error)
	// close pool, all pooled clients will be closed
	Close() error
	// number of pooled clients
	Size() int
}

type ClientFactory func(openedSocket thrift.TTransport) Client

type ChannelClientPool struct {
	mu      sync.Mutex
	clients chan Client

	opened         uint32
	maxIdle        uint32
	maxOpen        uint32
	servers        []string
	connectTimeout time.Duration
	readTimeout    time.Duration
	clientFactory  ClientFactory
}

// thrift service client wrapped with pool manage information
type pooledClient struct {
	Client
	pool     *ChannelClientPool
	unusable bool
}

func (cli *pooledClient) Close() error {
	return cli.pool.closePooledClient(cli)
}

func (cli *pooledClient) RawClient() Client {
	return cli.Client
}

// MarkUnusable() marks the connection not usable any more, to let the pool close it instead of returning it to pool.
func (cli *pooledClient) MarkUnusable() {
	cli.unusable = true
}

func NewChannelClientPool(maxIdle, maxOpen uint32, servers []string, connectTimeout, readTimeout time.Duration, clientFactory ClientFactory) *ChannelClientPool {
	pool := &ChannelClientPool{
		clients:        make(chan Client, maxIdle),
		maxIdle:        maxIdle,
		maxOpen:        maxOpen,
		servers:        servers,
		connectTimeout: connectTimeout,
		readTimeout:    readTimeout,
		clientFactory:  clientFactory,
	}
	return pool
}

func (pool *ChannelClientPool) Get() (cli PooledClient, err error) {
	rawCli, err := pool.getFromPool()
	if err == ErrPoolClosed {
		return nil, err
	}
	if rawCli == nil {
		rawCli, err = pool.openClient()
		if err != nil {
			return
		}
	}
	cli = &pooledClient{
		Client: rawCli,
		pool:   pool,
	}
	return
}

func (pool *ChannelClientPool) Close() (err error) {
	pool.mu.Lock()
	clients := pool.clients
	pool.clients = nil
	pool.mu.Unlock()
	for {
		select {
		case rawCli := <-clients:
			curErr := pool.closeClient(rawCli)
			if err == nil {
				err = curErr
			}
		default:
			return
		}
	}
}

func (pool *ChannelClientPool) Size() int {
	pool.mu.Lock()
	defer pool.mu.Unlock()
	return len(pool.clients)
}

func (pool *ChannelClientPool) getFromPool() (rawCli Client, err error) {
	pool.mu.Lock()
	defer pool.mu.Unlock()
	if pool.clients == nil {
		return nil, ErrPoolClosed
	}
	select {
	case rawCli = <-pool.clients:
		return
	default:
		return nil, errNoPooledClient
	}
}

func (pool *ChannelClientPool) closePooledClient(cli *pooledClient) error {
	if cli.unusable {
		return pool.closeClient(cli.Client)
	}

	pool.mu.Lock()
	if pool.clients != nil {
		select {
		case pool.clients <- cli.Client:
			cli.Client = nil
		default:
		}
	}
	pool.mu.Unlock()

	return pool.closeClient(cli.Client)
}

func (pool *ChannelClientPool) openClient() (cli Client, err error) {
	if pool.maxOpen != 0 && atomic.LoadUint32(&pool.opened) >= pool.maxOpen {
		return nil, ErrPoolMaxOpenReached
	}

	server := pool.servers[rand.Int()%len(pool.servers)]
	var socket *thrift.TSocket
	if socket, err = thrift.NewTSocket(server); err != nil {
		return
	}
	socket.SetTimeout(pool.connectTimeout)
	if err = socket.Open(); err != nil {
		return
	}
	socket.SetTimeout(pool.readTimeout)
	if pool.maxOpen != 0 {
		atomic.AddUint32(&pool.opened, 1)
	}
	return pool.clientFactory(socket), nil
}

func (pool *ChannelClientPool) closeClient(cli Client) (err error) {
	if cli == nil {
		return nil
	}
	if pool.maxOpen != 0 {
		atomic.AddUint32(&pool.opened, ^uint32(0))
	}
	if v := reflect.ValueOf(cli).Elem().FieldByName("Transport"); !v.IsValid() {
		return ErrClientMissingTransportField
	} else if v.IsNil() {
		return ErrClientNilTransportField
	} else {
		if transport, ok := v.Interface().(thrift.TTransport); !ok {
			panic(v)
		} else {
			return transport.Close()
		}
	}
}
