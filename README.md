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

## Todo

* "Couldn't push message to Redis: OOM command not allowed when used memory > 'maxmemory'."
* Multi-request IDs: ltank-request_id-aa693837-a0f6-414f-8d92-1d306fc9eca9,2c8e202e-fc2e-4265-8b3e-eb5bbe9f36c7
* Gzipped responses.

## Field Notes

* Based on some rough calculations, an r3.large instance worth of memory will be able to store roughly 2.5 hours worth of log traces from the produciton API. This number could be vastly improved with in-store compression.
