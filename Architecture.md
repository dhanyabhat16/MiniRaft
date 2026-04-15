# MiniRaft Architecture

```mermaid
graph TD
  User[User Browser] --> Frontend[Frontend / Nginx :8080]
  Frontend -->|WebSocket strokes| Gateway[Gateway :4000]
  Gateway -->|POST /intake, /broadcast| Leader[(Current Leader Replica)]
  Leader --> Replica1[Replica 1 :3001]
  Leader --> Replica2[Replica 2 :3002]
  Leader --> Replica3[Replica 3 :3003]
  Replica1 <-->|vote / heartbeat / append / sync| Replica2
  Replica1 <-->|vote / heartbeat / append / sync| Replica3
  Replica2 <-->|vote / heartbeat / append / sync| Replica3
  Gateway -->|poll /status and /sync-log| Replica1
  Gateway -->|poll /status and /sync-log| Replica2
  Gateway -->|poll /status and /sync-log| Replica3
```

## Flow

1. The browser opens the frontend at port `8080`.
2. The frontend sends drawing strokes to the gateway over WebSocket.
3. The gateway finds the active leader and forwards each stroke for intake.
4. The leader replicates committed entries to follower replicas.
5. The gateway polls replica status and sync endpoints so connected clients stay updated during leader changes.
