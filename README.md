# lvat

lvat is a tiny Go daemon that will consume a syslog feed sent over HTTP and allow simple lookups to be performed on the data that it collects.

See [lvat](https://github.com/brandur/hutils) for client support in querying a running lvat instance.

## Install & Run

``` bash
go get -u github.com/kr/godep
godep go build

redis-server &

export API_KEY=my-secret
export REDIS_URL=redis://:pass@localhost:6379
export PORT=5000
./lvat
```

Note that Redis should probably be running as a cache with `maxmemorypolicy = allkeys-lru`.

## Field Notes

* Based on some rough calculations, an r3.large instance worth of memory will be able to store roughly 2.5 hours worth of log traces from the produciton API. This number could be vastly improved with in-store compression.
