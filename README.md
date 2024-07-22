Redis Database implementation in Go, based on the Code Crafters Challenge.
- Supports handling concurrent connections to key-value store
- Supports value expiry
- Supports replication (replicants sync with master database to handle additional clients)
- Supports RDB persistence (retrieving and loading in-memory data as a persistent file format) [TODO]
- Supports creating real-time data streams [TODO]
- Supports transactions (executing a sequence of commands as a single atomic operation, either all succeeding or all failing) [TODO]
