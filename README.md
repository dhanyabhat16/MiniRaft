# MiniRaft

MiniRaft is a small Raft-inspired drawing app. A browser frontend sends stroke events to a WebSocket gateway, and the gateway forwards them to a replica cluster that elects a leader, replicates committed strokes, and keeps the canvas state available across failover.

## Architecture

See [ARCHITECTURE.md](ARCHITECTURE.md) for a simple service diagram.

## Services

- `frontend`: Static Nginx site for the drawing board UI on port `8080`
- `gateway`: WebSocket and HTTP entrypoint on port `4000`
- `replica1`, `replica2`, `replica3`: Raft-style replicas on ports `3001`, `3002`, and `3003`

## Run With Docker Compose

From the repository root:

```bash
docker compose up --build
```

Open these URLs after the stack starts:

- Frontend: http://localhost:8080
- Gateway: http://localhost:4000

## Run Locally

Install dependencies in the service folders first:

```bash
cd gateway && npm install
cd ../replica && npm install
```

Start the gateway in one terminal:

```bash
cd gateway
npm start
```

Start a replica in another terminal, then repeat for additional replicas with the right environment variables:

```bash
cd replica
REPLICA_ID=replica1 PORT=3001 PEERS=http://localhost:3002,http://localhost:3003 GATEWAY_URL=http://localhost:4000 npm start
```

Serve the frontend from `frontend/` with any static server or Nginx pointing at `index.html`.

## Notes

- The gateway discovers the current leader by polling replica status endpoints.
- Replicas exchange vote, heartbeat, append, and sync messages to keep the log consistent.
- The frontend connects to the gateway over WebSocket and draws strokes as they are broadcast.
