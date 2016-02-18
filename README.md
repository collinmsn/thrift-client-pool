# thrift-client-pool

一个golang版的thrift连接池，goroutine safe。使用方法：
```
import (
  "github.com/collinmsn/thrift-client-pool/example"
  "github.com/collinmsn/thrift-client-pool"
)

// create pool
pool := thrift_client_pool.NewChannelClientPool(maxIdle, maxOpen, servers, connectTimeout, readTimeout,
  func(openedSocket *thrift.TSocket) thrift_client_pool.Client {
		return example.NewExampleClientFactory(transportFactory.GetTransport(openedSocket), protocolFactory)
  },
)

// get client from pool	
pooledClient, err := pool.Get()
if err != nil {
  // handle error
}
// resort to defer to return client to pool properly
defer pooledClient.Close()

// use client
exampleClient := pooledClient.RawClient().(*example.ExampleClient)
rsp, err := exampleclient.Add(1, 2)
if err != nil {
  // if error occured, mark client as unusable
  pooledClient.MarkUnusable()
}
```
将github.com/collinmsn/thrift-client-pool/example换成你实际使用的thrift生成的package
