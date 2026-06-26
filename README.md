# vertex-php

The **PHP implementation** of [Vertex](https://github.com/dengxuan/Vertex).

The authoritative wire format and transport contract live in the
[Vertex spec repo](https://github.com/dengxuan/Vertex). **Any wire-spec change
lands there first**, with a companion PR here. This repo does not define the
protocol — it tracks it.

## Spec version pinning

This repo pins an exact Vertex spec commit in [`scripts/.spec-ref`](scripts/.spec-ref).
The `.proto` files under [`protos/`](protos/) are vendored from the spec repo at
that SHA, and the PHP code under `generated/` is produced from them. CI verifies
the vendored protos still match the pinned SHA on every push.

Currently pinned: `40c53d9` — the same spec commit as
[vertex-go](https://github.com/dengxuan/vertex-go) and
[vertex-dotnet](https://github.com/dengxuan/vertex-dotnet). All three
implementations track the same wire surface by sharing this SHA.

### Bumping the spec

```bash
# 1. edit scripts/.spec-ref to the new dengxuan/Vertex commit SHA
# 2. re-vendor protos and regenerate PHP:
bash scripts/sync-protos.sh
# 3. commit the updated protos/, generated/, and scripts/.spec-ref together
```

`sync-protos.sh` needs only `protoc` on PATH (no `grpc_php_plugin` — the
transport is hand-written, so we generate protobuf *message* classes only). CI
runs it with `--check-only`, which re-fetches the vendored `.proto` without
regenerating code, then fails if the checked-in protos drifted from the pinned
SHA.

## Reference

- Wire format: [Vertex / spec / wire-format.md](https://github.com/dengxuan/Vertex/blob/main/spec/wire-format.md)
- Transport contract (4 invariants): [Vertex / spec / transport-contract.md](https://github.com/dengxuan/Vertex/blob/main/spec/transport-contract.md)

## Runtime: Swoole (required)

Vertex is a **long-lived bidirectional connection** protocol. The transport
contract's four invariants (spec/transport-contract.md) assume a persistent
read loop, per-message concurrent dispatch, and a connection whose lifecycle is
independent of any single request. PHP's traditional php-fpm / mod_php
request-per-process model cannot host that.

This SDK therefore targets **[Swoole](https://swoole.com/)** (`ext-swoole`):

- a resident process + event loop,
- go-style coroutines for per-message dispatch (invariant #1),
- `Swoole\Coroutine\Channel` for the inbound queue and the send mutex,
- a coroutine HTTP/2 client driving the gRPC bidi stream, so the read loop and
  write path get the byte-level control invariants #3 / #4 demand.

The gRPC transport is hand-written on Swoole's HTTP/2 client rather than
`ext-grpc`; we only use `protoc` (no `grpc_php_plugin`) to emit the protobuf
*message* classes.

## Development

This SDK builds and tests itself in isolation — no sibling repos required. The
simplest path is the dev container (`.devcontainer/`), which ships PHP 8.3 +
ext-swoole + composer + protoc. In VS Code: **Reopen in Container**.

Once inside (or on any host with PHP 8.3 + ext-swoole + composer):

```bash
composer install          # one-time: pull dev dependencies

composer test             # run the PHPUnit unit tests
composer lint             # check code style (no changes)
composer fix              # apply code-style fixes
composer analyse          # PHPStan static analysis (src/, level 6)
composer check            # lint + analyse + test — the full pre-push gate

composer sync-protos      # re-vendor protos + regenerate message classes
                          # (after editing scripts/.spec-ref)
```

Run `composer run-script --list` to see every script with its description.

The unit tests (`tests/`) cover the wire-format encode/decode and the gRPC
TransportFrame framing — pure byte logic that needs no live connection or peer.
Cross-language interop (`compat/hello`) lives in the Vertex spec repo and is run
from there, not here.

## Status

**Stage 1 (transport + wire) — code complete, unverified.** Implemented:

- `src/Messaging/Envelope.php` — 4-frame wire encode/decode (spec §2)
- `src/Transport/Grpc/GrpcClientTransport.php` — Swoole HTTP/2 gRPC bidi client
  honoring the four invariants (read loop, send mutex, graceful half-close)
- `src/Serialization/ProtobufSerializer.php` — protobuf payloads + canonical
  topic from the descriptor full name
- `src/Messaging/MessagingChannel.php` — `publish()` + event `subscribe()`
- compat scenario: `Vertex/compat/hello/php-client` + `run-php.sh`

> ⚠️ **Not yet run end-to-end.** This was authored in an environment with no
> PHP / Swoole, so it has not been linted, unit-tested, or interop-verified
> against the .NET server. Treat it as a reviewed draft pending a first run on
> a box with `php` + `ext-swoole`. The `compat/hello` row for PHP is marked 🚧
> until `./run-php.sh` passes.

**Not yet implemented (later stages):** RPC (`Invoke` / `HandleRequest`),
reconnect policy, server-side transport, ZeroMQ transport, peer-authentication.

CI skips build/test steps until a `composer.json` + `src/` package exists (both
now do, but the PHP test job needs a real PHP+Swoole runner image to be useful).
