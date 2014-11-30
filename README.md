# lvat

lvat is a tiny Go daemon that will consume a syslog feed sent over HTTP and allow simple lookups to be performed on the data that it collects.

In its normal operation, it acts an HTTP syslog drain for [Logplex](https://github.com/heroku/logplex). Received messages are buffered, parsed to see whether they match storage criteria, then compressed with gzip and stored to Redis. To ensure correct parallel operation at scale, lvat uses a form of optimistic locking built on top of [Redis' transaction mechanism](http://redis.io/topics/transactions) so that messages from disparate sources can be combined under common keys and be stored as part of the same gziped blob.

See [ltat](https://github.com/brandur/hutils) for client support in querying a running lvat instance.

## Install & Run

``` bash
go get -u github.com/kr/godep
godep go build

redis-server &

export API_KEY=my-secret
export REDIS_URL=redis://localhost:6379
export PORT=5000
./lvat
```

Note that Redis should probably be running as a cache with `maxmemorypolicy = allkeys-lru`.
