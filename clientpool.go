package thrift_client_pool

import (
	"errors"
	"sync"

	"math/rand"
	"reflect"
	"time"

	"git.apache.org/thrift.git/lib/go/thrift"
)

var (
	ErrPoolClosed                  = errors.New("pool has been closed")
	ErrPoolMaxOpenReached          = errors.New("pool max open client limit reached")
	ErrClientMissingTransportField = errors.New("client missing transport field")
	ErrClientNilTransportField     = errors.New("client transport field is nil")
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

type ClientFactory func(openedSocket *thrift.TSocket) Client

type ChannelClientPool struct {
	mu               sync.Mutex
	clients          chan Client
	maxIdle          uint32
	maxOpen          uint32
	opened           uint32
	servers          []string
	connectTimeout   time.Duration
	readTimeout      time.Duration
	transportFactory thrift.TTransportFactory
	protocolFactory  thrift.TProtocolFactory
	clientFactory    ClientFactory
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
	var (
		rawCli Client
		ok     bool
	)
	clients := pool.getClients()
	if clients == nil {
		return nil, ErrPoolClosed
	}
	select {
	case rawCli, ok = <-clients:
		if !ok {
			return nil, ErrPoolClosed
		}
	default:
	}
	if rawCli == nil {
		if pool.maxOpenReached() {
			return nil, ErrPoolMaxOpenReached
		}
		if rawCli, err = pool.openClient(); err != nil {
			return nil, err
		}
	}
	cli = &pooledClient{
		Client: rawCli,
		pool:   pool,
	}
	return
}

func (pool *ChannelClientPool) Close() (err error) {
	clients := pool.clients
	func() {
		pool.mu.Lock()
		defer pool.mu.Unlock()
		pool.clients = nil
	}()
	for {
		rawCli, ok := <-clients
		if !ok {
			break
		}
		curErr := pool.closeClient(rawCli)
		if err == nil {
			err = curErr
		}
	}
	return
}

func (pool *ChannelClientPool) Size() int {
	return len(pool.clients)
}

func (pool *ChannelClientPool) getClients() chan Client {
	pool.mu.Lock()
	defer pool.mu.Unlock()
	return pool.clients
}

func (pool *ChannelClientPool) closePooledClient(cli *pooledClient) error {
	if !cli.unusable {
		select {
		case pool.clients <- cli.Client:
			return nil
		default:
		}
	}
	return pool.closeClient(cli.Client)
}

func (pool *ChannelClientPool) maxOpenReached() bool {
	if pool.maxOpen == 0 {
		return false
	}
	pool.mu.Lock()
	defer pool.mu.Unlock()
	return pool.opened >= pool.maxOpen
}

func (pool *ChannelClientPool) openClient() (cli Client, err error) {
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
	pool.mu.Lock()
	defer pool.mu.Unlock()
	pool.opened += 1
	return pool.clientFactory(socket), nil
}

func (pool *ChannelClientPool) closeClient(cli Client) (err error) {
	if cli == nil {
		return nil
	}
	if v := reflect.ValueOf(cli).Elem().FieldByName("Transport"); !v.IsValid() {
		return ErrClientMissingTransportField
	} else if v.IsNil() {
		return ErrClientNilTransportField
	} else {
		if transport, ok := v.Interface().(thrift.TTransport); !ok {
			// should never happen
			panic(v)
		} else {
			pool.mu.Lock()
			defer pool.mu.Unlock()
			pool.opened -= 1
			return transport.Close()
		}
	}
}
