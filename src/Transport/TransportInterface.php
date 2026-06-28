<?php

// Licensed to the Gordon under one or more agreements.
// Gordon licenses this file to you under the MIT license.

declare(strict_types=1);

namespace Vertex\Transport;

use Swoole\Coroutine\Channel;

/**
 * A Vertex transport: a long-lived connection that carries multi-frame
 * envelopes both ways. Implementations MUST satisfy the four invariants in
 * spec/transport-contract.md:
 *
 *   #1 The read loop only does lightweight non-blocking routing — it pushes
 *      each inbound message onto {@see receive()} and immediately reads the
 *      next. It never calls a user handler on the read coroutine.
 *   #2 A single send/handler failure is NOT a disconnect. {@see send()}
 *      failures surface to the caller only; they never close the inbound
 *      channel or stop the read loop.
 *   #3 Cancellation/timeout on send is honored only before the first byte hits
 *      the wire. Once frames start flushing, the send runs to completion (a
 *      mid-stream HTTP/2 RST_STREAM would tear down every multiplexed request).
 *   #4 The read loop is the SOLE source of peer-connection state. Only its
 *      receive error path emits Disconnected — never the send path.
 */
interface TransportInterface
{
    /** Logical transport name (defaults to the server address). */
    public function name(): string;

    /**
     * Transmit one envelope's frames as a single multi-frame message.
     *
     * Blocks until a live connection exists (or the pre-wire timeout elapses).
     * Throws on send failure; per invariant #2 the caller's failure does not
     * affect the connection or other in-flight messages.
     *
     * @param string       $target ignored for single-peer client transports
     * @param list<string> $frames envelope frames from {@see \Vertex\Messaging\Envelope::encode()}
     */
    public function send(string $target, array $frames): void;

    /**
     * The inbound message channel. The read loop pushes a
     * {@see TransportMessage} per fully-reassembled envelope; consumers pop in
     * a coroutine. Closed (pop returns false) when the transport shuts down.
     *
     * Swoole's Channel is untyped; every element popped here is a
     * {@see TransportMessage}.
     */
    public function receive(): Channel;

    /**
     * Subscribe to peer-connection liveness changes (Connected / Disconnected).
     * The messaging layer uses this to fail pending RPC invokes when a peer
     * goes away. Per invariant #4 only the read loop concludes Disconnected.
     *
     * The current state is replayed to a handler at subscription time so a late
     * subscriber doesn't miss the connection it's already on. Handlers must not
     * throw; a throwing handler is isolated and logged, never propagated.
     *
     * @param callable(PeerConnectionEvent): void $handler
     */
    public function onPeerConnectionChanged(callable $handler): void;

    /**
     * Gracefully shut down: half-close the send side so queued outbound frames
     * flush, drain the read loop, then release the connection. Idempotent.
     */
    public function close(): void;
}
