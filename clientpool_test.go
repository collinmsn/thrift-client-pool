package thrift_client_pool

import (
	"testing"

	"net"
	"os"

	"time"

	"git.apache.org/thrift.git/lib/go/thrift"
	"github.com/collinmsn/thrift-client-pool/example"
)

var transportFactory thrift.TTransportFactory
var protocolFactory thrift.TProtocolFactory
var serverAddr net.Addr

type ExampleHandler struct {
}

func (h *ExampleHandler) Add(num1 int32, num2 int32) (r int32, err error) {
	return num1 + num2, nil
}
func (h *ExampleHandler) AddTimeout(num1 int32, num2 int32, client_timeout_ms int32) (r int32, err error) {
	<-time.After(time.Duration(client_timeout_ms+5) * time.Millisecond)
	return num1 + num2, nil
}

func TestGet(t *testing.T) {
	servers := []string{
		serverAddr.String(),
	}
	var maxIdle uint32 = 1
	var timeoutMs int32 = 5
	pool := NewChannelClientPool(maxIdle, 0, servers, 0, time.Duration(timeoutMs)*time.Millisecond,
		func(openedSocket thrift.TTransport) Client {
			transport := transportFactory.GetTransport(openedSocket)
			return example.NewExampleClientFactory(transport, protocolFactory)
		},
	)
	func() {
		pooledClient, err := pool.Get()
		if err != nil {
			t.Error(err)
		}
		defer pooledClient.Close()
		rawClient, ok := pooledClient.RawClient().(*example.ExampleClient)
		if !ok {
			t.Error("convert to raw client failed")
		}
		if v, err := rawClient.Add(1, 2); err != nil {
			t.Error(err)
		} else if v != 3 {
			t.Error("call rpc failed")
		}
	}()
	func() {
		pooledClient, err := pool.Get()
		if err != nil {
			t.Error(err)
		}
		defer pooledClient.Close()
		rawClient := pooledClient.RawClient().(*example.ExampleClient)
		if _, err := rawClient.AddTimeout(1, 2, timeoutMs); err == nil {
			t.Error("timeout expected")
		} else {
			pooledClient.MarkUnusable()
		}
	}()
}

func TestMain(m *testing.M) {
	transportFactory = thrift.NewTBufferedTransportFactory(8192)
	transportFactory = thrift.NewTFramedTransportFactory(transportFactory)
	protocolFactory = thrift.NewTBinaryProtocolFactoryDefault()

	serverTransport, err := thrift.NewTServerSocket("127.0.0.1:0")
	if err != nil {
		panic(err)
	}
	// call listen here to avoid synchronizing server.serve with test cases
	if err = serverTransport.Listen(); err != nil {
		panic(err)
	}
	serverAddr = serverTransport.Addr()
	handler := &ExampleHandler{}
	processor := example.NewExampleProcessor(handler)
	server := thrift.NewTSimpleServer6(processor, serverTransport, transportFactory, transportFactory, protocolFactory, protocolFactory)
	go server.Serve()
	os.Exit(m.Run())
}
