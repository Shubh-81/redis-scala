# Redis Clone (Scala)

This project is a Redis server implementation written in Scala, as part of the [Codecrafters](https://codecrafters.io/) Redis challenge. It aims to mimic the core features of Redis, including key-value storage, streams, replication, and RDB persistence.

## Features
- Basic Redis commands: `PING`, `ECHO`, `SET`, `GET`, `INCR`, `KEYS`, `TYPE`, `CONFIG`, `SAVE`, etc.
- Stream support: `XADD`, `XRANGE`, `XREAD`
- Replication: Master-slave support with `REPLCONF`, `PSYNC`, `WAIT`
- RDB persistence: Save and load state to/from disk
- RESP protocol support (serialization/deserialization)
- Multi/Exec transaction support

## Project Structure
```
/src
  /main/scala/codecrafters_redis
    Server.scala           # Main server logic
    /utils
      EventProcessor.scala # Command processing and event handling
      RESPEncoder.scala    # RESP protocol encoding
      RESPDecoder.scala    # RESP protocol decoding
      RDBParserEncoder.scala # RDB file encoding/decoding
  /test                   # (Optional) Test files
```

## Getting Started

### Prerequisites
- Scala (2.13 or compatible)
- sbt (Scala Build Tool)

### Setup
1. Clone the repository:
   ```sh
   git clone <your-repo-url>
   cd codecrafters-redis-scala
   ```
2. Build the project:
   ```sh
   sbt compile
   ```

### Running the Server
You can run the server using sbt:
```sh
./your_program.sh --port 6379
```
By default, the server listens on `localhost:6379`.

#### Custom Configuration
You can specify custom configuration via command-line arguments:
- `--dir <dir>`: Directory for RDB file
- `--dbfilename <filename>`: RDB file name
- `--port <port>`: Port to listen on
- `--host <host>`: Host to bind
- `--replicaof <host> <port>`: Start as a replica of another Redis server

Example:
```sh
./your_program "run --dir ./data --dbfilename dump.rdb --port 6380"
```

## Usage
You can use the standard `redis-cli` to connect and interact with your server:
```sh
redis-cli -port 6379
```
Example commands:
```
SET foo bar
GET foo
INCR counter
XADD mystream * key1 value1
XRANGE mystream - +
SAVE
```


## Notes
- This project is for educational purposes and does not implement all Redis features or optimizations.
- Error handling and edge cases are handled to mimic Redis behavior as closely as possible.
- Persistence is via a simple RDB file format.

