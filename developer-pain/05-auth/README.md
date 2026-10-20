> **LaserData proof · Developer pain 05: TLS / SASL / auth setup**  
> Blog: [How Apache Iggy secures a stream: TLS, scoped users and tokens, in five minutes](https://claude.ai/artifact/6U58MVy4i1ozNW4xZmcRpL) · Doc: [Secure Apache Iggy in 5 minutes](https://claude.ai/artifact/REMoG5hfUcyW9SM1uGCr6P) · Posts: [ready-to-publish drafts](https://claude.ai/artifact/VHZp6Z8yooKaSaCES2bPnG#auth)  
> Where this code goes: LaserData repo: a starter project to copy.  
> Status: not yet run against a live Apache Iggy server unless a checklist below says so. Nothing has been opened upstream.

# iggy-secure-starter

A secure Apache Iggy setup you can run in five minutes, from [LaserData](https://laserdata.com).

It starts one Apache Iggy 0.9 server and two example services, and shows the four controls
you want before anything reaches production:

| Control | How this starter does it |
|---|---|
| Encryption in transit | TLS on the TCP and HTTP APIs, with a certificate from your own CA |
| No default credentials | The root user is created from `.env` on first start; `iggy`/`iggy` is never used |
| Least privilege | One user per service. `order-api` may only send to `orders`; `billing-worker` may only read it |
| No passwords in services | Each service logs in with a personal access token (PAT) that expires in 90 days |

## Run it

```bash
cp .env.example .env               # then set IGGY_ADMIN_PASSWORD to: openssl rand -base64 30
./scripts/gen-certs.sh             # private CA + server certificate for "iggy"
docker compose up -d iggy
docker compose --profile setup run --rm provision     # stream, topic, users, tokens
docker compose --profile app up -d order-api billing-worker
docker compose logs -f billing-worker
```

Prove the permissions hold:

```bash
docker compose --profile app run --rm \
  -e IGGY_PAT_FILE=/secrets/billing-worker.pat billing-worker python check_permissions.py
```

Expected: both checks print `OK ... denied`.

## What each file does

- `compose.yaml` turns on TLS (`IGGY_TCP_TLS_*`, `IGGY_HTTP_TLS_*`), sets the root user from
  `.env` (`IGGY_ROOT_USERNAME`, `IGGY_ROOT_PASSWORD`, read only when the data directory is empty),
  and binds only the TCP and HTTP listeners to the network. QUIC and WebSocket stay on the
  container's loopback.
- `scripts/gen-certs.sh` makes a CA and a server certificate with the names `iggy`, `localhost`
  and `127.0.0.1`. Clients trust `certs/ca.pem`; only the server sees `server.key`.
- `services/provision.py` runs once as the admin. It creates the stream, a user per service with
  stream-scoped permissions, and mints each service a PAT through `POST /personal-access-tokens`.
- `services/common.py` connects with `iggy://<token>@iggy:8090?tls=true&tls_domain=iggy&tls_ca_file=...`.
  A connection string with a single credential part is a personal access token.

## Connecting from your laptop

The server advertises itself as `iggy`. Add `127.0.0.1 iggy` to `/etc/hosts`, then use the same
connection string with `tls_ca_file=./certs/ca.pem`.

## Going further

- **Encryption at rest:** set `IGGY_ENCRYPTION_ENABLED=true` and `IGGY_ENCRYPTION_KEY` to a
  base64-encoded 32-byte key. Message payloads and user headers are encrypted with AES-256-GCM.
- **Rotating a token:** mint a new PAT for the user, roll the service, then delete the old one
  (`DELETE /personal-access-tokens/{name}` or `iggy pat delete <name>` logged in as that user).
- **Clusters:** turn on `cluster.auth` (a shared secret on every node) and `cluster.tls` for
  replica-to-replica traffic. See the LaserData guide "Secure Apache Iggy in 5 minutes".

Status: scripts and Python files are syntax-checked and the SDK calls are checked against
`apache-iggy` 0.9.0. The full `docker compose` run has not been executed yet.

Maintained by LaserData, which builds and runs Apache Iggy.
