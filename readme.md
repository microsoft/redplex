# redplex

Project status: redplex is actively maintained and has been running in production at Mixer since December 2017.

redplex is a tool to multiplex Redis pubsub. It implements the Redis protocol and is a drop-in replacement for existing Redis pubsub servers, simply boot redplex and change your port number. This is a useful tool in situations where you have very many readers for pubsub events, as Redis pubsub throughput is inversely proportional to the number of subscribers for the event.

> Note: some Redis clients have health checks that call commands like INFO on boot. You'll want to turn these off, as redplex does not implement commands expect for SUBSCRIBE, PSUBSCRIBE, UNSUBSCRIBE, PUNSUBSCRIBE, and EXIT. 

### Usage

```
➜  redplex git:(master) ./redplex --help
usage: redplex [<flags>]

Flags:
      --help                     Show context-sensitive help (also try --help-long and --help-man).
  -l, --listen="127.0.0.1:3000"  Address to listen on
  -n, --network="tcp"            Network to listen on
      --remote="127.0.0.1:6379"  Remote address of the Redis server
      --remote-network="tcp"     Remote network to dial through (usually tcp or tcp6)
      --sentinels=SENTINELS ...  A list of Redis sentinel addresses
      --sentinel-name=SENTINEL-NAME
                                 The name of the sentinel master
      --log-level="info"         Log level (one of debug, info, warn, error
      --dial-timeout=10s         Timeout connecting to Redis
      --write-timeout=2s         Timeout during write operations
      --pprof-server=PPROF-SERVER
                                 Address to bind a pprof server on. Not bound if empty.
```
