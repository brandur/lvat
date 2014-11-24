# lvat

lvat is a tiny Go daemon that will consume a syslog feed sent over HTTP and allow simple lookups to be performed on the data that it collects.

See [lvat](https://github.com/brandur/hutils) for client support in querying a running lvat instance.

## Install & Run

``` bash
go get -u
go build

redis-server &

export API_KEY=my-secret
export PORT=5000
./lvat
```
