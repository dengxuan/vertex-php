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

**Stage 1 (transport + wire) and Stage 2 (RPC) — implemented and verified.**

- `src/Messaging/Envelope.php` — 4-frame wire encode/decode (spec §2)
- `src/Transport/Grpc/GrpcClientTransport.php` — Swoole HTTP/2 gRPC bidi client
  honoring the four invariants. Send-loop + recv-loop architecture (the read
  side uses `usePipelineRead` so streamed responses arrive on a long-lived
  bidi stream); frame reassembly in `src/Transport/Grpc/FrameReassembler.php`.
- `src/Serialization/ProtobufSerializer.php` — protobuf payloads + canonical
  topic from the descriptor full name
- `src/Messaging/MessagingChannel.php` — events (`publish()` / `subscribe()`)
  **and RPC (`invoke()` / `handle()`)**: request/response over the 4-frame
  envelope, 32-hex request_id, error responses (`!`-topic + UTF-8), peer-
  disconnect grace period. Exceptions in `src/Messaging/Rpc/`.

Verified: php-cs-fixer + phpstan level 6 + 28 unit tests green; interop against
the .NET server in the spec repo — `compat/hello` (event), `compat/hello-rpc`
(RPC), and `compat/bidi-echo` (a bounded long-lived bidi smoke: ~145k round-
trips in 30s, 0 failures, concurrent bursts with no interleave).

**gRPC is client-only — by design, not a gap.** PHP cannot host a *bidirectional*
gRPC server: Swoole's HTTP/2 server buffers the request body until the client
half-closes, so it can't read a long-lived stream's frames incrementally (and
the wider PHP ecosystem agrees — grpc.io says "use another language to create a
gRPC server"). A PHP peer that must *be called* will use the ZeroMQ transport
(below), not gRPC.

**Not yet implemented (later stages):** reconnect policy, ZeroMQ transport
(incl. the Router role for being called), peer-authentication, MessagePack
serializer.

CI runs build/test on a PHP+Swoole image (composer.json + src/ in place).
