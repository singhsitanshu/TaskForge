# TaskForge monitoring

Prometheus scrapes the API directly and discovers every Compose worker and scheduler replica through DNS A-record service discovery. Grafana is provisioned with the Prometheus datasource and the `TaskForge Operations` dashboard at startup; no UI setup is required.

Start the stack with `make up`, then open Prometheus at <http://localhost:9090> and Grafana at <http://localhost:3001>. The default local Grafana login is `admin` / `taskforge`; override it in `.env` outside local development.

Global task, attempt, lease, and worker gauges are PostgreSQL samples emitted by every scheduler replica. They are eventually consistent. Queries must use `max` across scheduler replicas, as the supplied dashboard does; summing them would multiply the same database state by the replica count. Event counters and histograms are process-owned and should be summed across replicas.

See [`docs/tf-011.md`](../docs/tf-011.md) for the metric contract, label rules, latency definitions, and operational semantics.
