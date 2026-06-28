<?php

// Licensed to the Gordon under one or more agreements.
// Gordon licenses this file to you under the MIT license.

declare(strict_types=1);

namespace Vertex\Transport\Grpc;

use Swoole\Coroutine;
use Swoole\Coroutine\Channel;
use Swoole\Coroutine\Http2\Client as Http2Client;
use Swoole\Http2\Request as Http2Request;
use Vertex\Transport\PeerConnectionEvent;
use Vertex\Transport\PeerConnectionState;
use Vertex\Transport\TransportInterface;
use Vertex\Transport\TransportMessage;
use Vertex\Transport\TransportSendException;

/**
 * Client-side Vertex gRPC bidi transport, built on Swoole's coroutine HTTP/2
 * client. The PHP mirror of vertex-go's transport/grpc and vertex-dotnet's
 * Vertex.Transport.Grpc.
 *
 * The Vertex gRPC wire is a single bidirectional HTTP/2 stream on the
 * `vertex.transport.grpc.v1.Bidi/Connect` method. Each direction carries a
 * length-prefixed sequence of `TransportFrame` protobuf messages; one Vertex
 * envelope is split into 4 consecutive frames with end_of_message=true on the
 * 4th (spec/wire-format.md §4).
 *
 * We talk raw HTTP/2 + manual gRPC length-prefix framing rather than going
 * through ext-grpc, because that gives us byte-level control over the read
 * loop and the write path — exactly what the four transport invariants
 * require us to get right.
 *
 * ## Why two background coroutines
 *
 * Swoole's `Coroutine\Http2\Client` only surfaces a streamed response from
 * `recv()` when the request is opened with `usePipelineRead = true` AND a
 * dedicated coroutine drains `recv(-1)` continuously — the pattern the official
 * swoole/grpc client uses. A naive "send() then write() then recv() inline"
 * loop never sees the server's intermediate DATA frames on a long-lived bidi
 * stream: recv() blocks until end-of-stream, which for a never-ending bidi
 * stream is never. So this transport runs:
 *
 *   - a SEND loop: the sole coroutine that touches the client's write side.
 *     Every send()/half-close is funneled through {@see $sendCh}, which both
 *     serializes writers (no interleaved frames, wire-format §4) and keeps all
 *     HTTP/2 writes on one coroutine as Swoole requires.
 *   - a RECV loop: the sole coroutine that calls recv(). It reassembles frames
 *     and pushes envelopes onto {@see $inbound}.
 *
 * Invariants (spec/transport-contract.md):
 *   #1 {@see recvLoop()} only reassembles frames and pushes onto $inbound.
 *   #2 {@see send()} throws on failure; never touches the connection state.
 *   #3 The send loop serializes all writes; send()'s timeout guards only the
 *      enqueue handshake, never a partially-written envelope.
 *   #4 Only {@see recvLoop()}'s recv() error path drives Disconnected.
 */
final class GrpcClientTransport implements TransportInterface
{
    /** gRPC method path for the Vertex bidi service. */
    private const METHOD_PATH = '/vertex.transport.grpc.v1.Bidi/Connect';

    /**
     * Seconds close() waits for the recv loop to observe the server's
     * response-side close after half-closing the send side. Bounds the
     * flush-before-close drain so a wedged peer can't hang shutdown.
     */
    private const CLOSE_DRAIN_TIMEOUT = 5.0;

    private readonly string $name;
    private readonly TransportFrameCodec $codec;
    private Http2Client $client;
    private int $streamId = 0;

    /** Inbound queue; every element is a {@see TransportMessage} (Channel is untyped). */
    private Channel $inbound;

    /**
     * Outbound command queue drained by the send loop. Each element is either:
     *   - a {@see Http2Request} to open the stream, or
     *   - array{0:int,1:string,2:bool} = [streamId, data, endStream] for a write.
     * The send loop pushes each call's result onto {@see $sendRet}, so callers
     * get a synchronous-looking send() while all writes stay on one coroutine.
     */
    private Channel $sendCh;

    /** 1-capacity handshake channel: the send loop's result for each command. */
    private Channel $sendRet;

    /**
     * Serializes send() callers so their (enqueue, await-result) handshakes on
     * the single-capacity {@see $sendCh}/{@see $sendRet} pair don't interleave.
     * A buffered Channel pop/push is the Swoole idiom for a mutex whose
     * acquisition can time out (invariant #3's allowed pre-wire cancel point).
     */
    private Channel $writeLock;

    /** Signaled by the recv loop on each exit (flush-before-close + reconnect). */
    private Channel $recvDone;

    /**
     * 1-capacity channel connect() blocks on for the first connection result:
     * true on first success, or a \Throwable to rethrow on terminal first-
     * connect failure. Pushed exactly once by the run loop.
     */
    private Channel $firstConnect;

    /** Pushed by close() to wake the run loop out of a backoff sleep. */
    private Channel $closeSignal;

    /** @var list<callable(PeerConnectionEvent): void> peer-connection subscribers */
    private array $peerHandlers = [];

    /** Last state delivered to subscribers; replayed to late ones. Null until first connect. */
    private ?PeerConnectionState $lastState = null;

    private bool $connected = false;
    private bool $closed = false;

    /**
     * @param string          $serverAddr host:port of the Vertex gRPC server
     * @param string          $authority   :authority pseudo-header (defaults to serverAddr)
     * @param float           $sendTimeout seconds to wait for the write lock before send() throws
     * @param ReconnectPolicy $reconnect   backoff policy; default 1s→30s ±20%
     */
    /**
     * @param string                       $serverAddr  host:port of the Vertex gRPC server
     * @param string                       $authority   :authority pseudo-header (defaults to serverAddr)
     * @param float                        $sendTimeout seconds to wait for the write lock before send() throws
     * @param ReconnectPolicy              $reconnect   backoff policy; default 1s→30s ±20%
     * @param array<string,string>|callable():array<string,string> $metadata
     *        connect-time gRPC metadata (HTTP/2 headers) merged into the bidi
     *        request, e.g. ['x-merchant-id' => '...', 'x-signature' => '...'] for
     *        connection-level auth. Reserved/pseudo headers (:authority,
     *        content-type, te, user-agent) cannot be overridden. Pass a callable
     *        to recompute the headers on every (re)connect — lets rotating
     *        credentials refresh without rebuilding the transport. The caller
     *        signs; the transport only carries the headers.
     */
    public function __construct(
        private readonly string $serverAddr,
        ?string $name = null,
        private readonly string $authority = '',
        private readonly float $sendTimeout = 5.0,
        private readonly ReconnectPolicy $reconnect = new ReconnectPolicy(),
        private readonly mixed $metadata = [],
    ) {
        $this->name = $name ?? $serverAddr;
        $this->codec = new TransportFrameCodec();
        $this->inbound = new Channel(256);
        $this->sendCh = new Channel(1);
        $this->sendRet = new Channel(1);
        $this->recvDone = new Channel(1);
        $this->firstConnect = new Channel(1);
        $this->closeSignal = new Channel(1);
        $this->writeLock = new Channel(1);
        $this->writeLock->push(true); // unlocked
    }

    /**
     * Establish the HTTP/2 connection and open the bidi stream. Must be called
     * inside a coroutine (Swoole\Coroutine\run or an existing coroutine).
     *
     * Spawns the send and recv loops. After this returns, send() will work and
     * inbound messages flow onto receive().
     *
     * @throws \RuntimeException if the connection or stream cannot be opened
     */
    public function connect(): void
    {
        // The send loop owns the client write side for the transport's whole
        // life, across reconnects; start it once. It reads $this->client each
        // iteration, so reconnect just swaps the field.
        Coroutine::create(fn () => $this->sendLoop());

        // The run loop drives connect → read → disconnect → backoff → reconnect.
        Coroutine::create(fn () => $this->runLoop());

        // Block until the first connection succeeds (or fails terminally), so
        // that after connect() returns, send() has a live stream — mirrors
        // vertex-dotnet's _firstConnectTcs.
        $first = $this->firstConnect->pop();
        if ($first instanceof \Throwable) {
            throw $first;
        }
    }

    public function name(): string
    {
        return $this->name;
    }

    public function receive(): Channel
    {
        return $this->inbound;
    }

    public function onPeerConnectionChanged(callable $handler): void
    {
        $this->peerHandlers[] = $handler;

        // Replay the current state so a late subscriber doesn't miss the
        // connection it's already on (mirrors vertex-dotnet's replay).
        if ($this->lastState !== null) {
            $this->invokePeerHandler($handler, new PeerConnectionEvent($this->serverAddr, $this->lastState));
        }
    }

    /**
     * Drives the transport's whole life: connect, run the read loop until the
     * peer drops, raise Connected/Disconnected, then back off and reconnect per
     * the policy. The SOLE source of connection-liveness judgment (invariant
     * #4). Exits only on close() or when reconnect is disabled after a drop.
     */
    private function runLoop(): void
    {
        $attempt = 0;
        $everConnected = false;

        while (!$this->closed) {
            try {
                $this->connectOnce();
                $attempt = 0; // success resets backoff
                $this->connected = true;
                if (!$everConnected) {
                    $everConnected = true;
                    $this->firstConnect->push(true);
                }
                $this->raisePeerEvent(PeerConnectionState::Connected);

                // Blocks until the peer drops (recv loop pushes $recvDone on exit).
                $this->recvLoop();

                // We were connected and just lost it: raise Disconnected exactly
                // once on the Connected→Disconnected edge. A *failed reconnect
                // attempt* (the catch below) must NOT re-raise it — mirrors
                // vertex-dotnet, which only emits on a real drop.
                $this->connected = false;
                $this->teardownClient();
                $this->raisePeerEvent(PeerConnectionState::Disconnected);
            } catch (\Throwable $e) {
                // Connect/open failure (initial or a reconnect attempt). Never
                // emits Disconnected — we weren't connected this cycle. If we've
                // never connected and won't retry, surface it to connect().
                $this->connected = false;
                $this->teardownClient();
                if (!$everConnected && !$this->reconnect->enabled) {
                    $this->firstConnect->push($e);

                    return;
                }
            }

            if ($this->closed || !$this->reconnect->enabled) {
                break;
            }

            // Back off, then reconnect. Sleep on $closeSignal so close() during a
            // backoff window wakes us immediately instead of waiting it out.
            ++$attempt;
            if ($this->closeSignal->pop($this->reconnect->backoffFor($attempt)) !== false) {
                break; // close() pushed the signal
            }
        }

        // Never connected and gave up (reconnect disabled) — unblock connect().
        if (!$everConnected) {
            $this->firstConnect->push(new TransportSendException(
                \sprintf('vertex grpc: could not connect to %s', $this->serverAddr)
            ));
        }

        // No more inbound after the run loop ends.
        $this->inbound->close();
    }

    /**
     * Open one fresh connection + bidi stream, populating $client / $streamId.
     * Throws on any failure so runLoop() can back off and retry.
     */
    private function connectOnce(): void
    {
        [$host, $port] = $this->splitAddr($this->serverAddr);

        $client = new Http2Client($host, $port);
        $client->set([
            'timeout' => -1,            // the stream is long-lived; no overall timeout
            'open_http2_protocol' => true,
        ]);
        if (!$client->connect()) {
            throw new TransportSendException(
                \sprintf('vertex grpc: connect %s failed: %s', $this->serverAddr, $client->errMsg)
            );
        }
        $this->client = $client;

        // Open the bidi stream: one long-lived HTTP/2 request. pipeline keeps the
        // request (send) side open for streaming writes; usePipelineRead makes
        // recv() surface the server's streamed DATA frames as they arrive rather
        // than buffering until end-of-stream (required for a never-ending bidi).
        $req = new Http2Request();
        $req->method = 'POST';
        $req->path = self::METHOD_PATH;
        $req->headers = $this->buildHeaders();
        $req->pipeline = true;
        // Real Swoole runtime property (Swoole >= 4.5.3); the static stubs omit
        // it, so phpstan can't see it. Without it recv() buffers until end-of-
        // stream and never surfaces a long-lived bidi stream's DATA frames.
        // @phpstan-ignore property.notFound
        $req->usePipelineRead = true;

        $streamId = $this->submit($req);
        if ($streamId === false || $streamId <= 0) {
            throw new TransportSendException(
                \sprintf('vertex grpc: open stream on %s failed: %s', $this->serverAddr, $this->client->errMsg)
            );
        }
        $this->streamId = $streamId;
    }

    /** Reserved/pseudo headers the caller's metadata may not override. */
    private const RESERVED_HEADERS = ['content-type', 'te', ':authority', 'user-agent'];

    /**
     * Build the bidi request headers: the caller's connect metadata first, then
     * the reserved headers on top so they always win. Resolved fresh on every
     * (re)connect — a callable $metadata is invoked here, so rotating
     * credentials refresh without rebuilding the transport.
     *
     * @return array<string,string>
     */
    private function buildHeaders(): array
    {
        $metadata = \is_callable($this->metadata) ? ($this->metadata)() : $this->metadata;
        $authority = $this->authority !== '' ? $this->authority : $this->serverAddr;

        return self::mergeHeaders($metadata, $authority);
    }

    /**
     * Merge caller metadata under the reserved headers (reserved always win).
     * Pure and static so the precedence rules are unit-testable without a
     * connection.
     *
     * @param array<string,string> $metadata
     * @return array<string,string>
     */
    public static function mergeHeaders(array $metadata, string $authority): array
    {
        $headers = [];
        foreach ($metadata as $key => $value) {
            // Drop any attempt to set a reserved header; the framework owns those.
            if (!\in_array(\strtolower((string) $key), self::RESERVED_HEADERS, true)) {
                $headers[(string) $key] = (string) $value;
            }
        }

        // Reserved headers last so they overwrite anything the caller slipped in.
        $headers['content-type'] = 'application/grpc+proto';
        $headers['te'] = 'trailers';
        $headers[':authority'] = $authority;
        $headers['user-agent'] = 'vertex-php/0.1 swoole';

        return $headers;
    }

    /** Close the current connection's client between reconnect attempts. */
    private function teardownClient(): void
    {
        if (isset($this->client)) {
            @$this->client->close();
        }
    }

    /**
     * Deliver a peer-connection state change to every subscriber and record it
     * for replay. Handlers are isolated: a thrower doesn't break the others or
     * the run loop (invariant #2 in spirit — one handler's failure is not fatal).
     */
    private function raisePeerEvent(PeerConnectionState $state): void
    {
        $this->lastState = $state;
        $event = new PeerConnectionEvent($this->serverAddr, $state);
        foreach ($this->peerHandlers as $handler) {
            $this->invokePeerHandler($handler, $event);
        }
    }

    /** @param callable(PeerConnectionEvent): void $handler */
    private function invokePeerHandler(callable $handler, PeerConnectionEvent $event): void
    {
        try {
            $handler($event);
        } catch (\Throwable) {
            // Swallow: a subscriber's failure must not disturb the transport.
        }
    }

    /**
     * Send one envelope's frames as consecutive gRPC-framed TransportFrame
     * messages, the last marked end_of_message.
     *
     * Invariant #3: the timeout below guards only the write-lock wait. Once we
     * hold the lock and hand frames to the send loop, they run to completion —
     * a mid-envelope abort would RST_STREAM the whole bidi stream and kill every
     * other in-flight message multiplexed on it.
     *
     * @param list<string> $frames
     */
    public function send(string $target, array $frames): void
    {
        if ($this->closed) {
            throw new TransportSendException('vertex grpc: transport closed');
        }

        // Acquire the write lock — invariant #3's one legitimate cancel point.
        $token = $this->writeLock->pop($this->sendTimeout);
        if ($token === false) {
            throw new TransportSendException('vertex grpc: timed out acquiring write lock');
        }

        try {
            // Disconnected (e.g. mid-reconnect backoff): fail just this send,
            // immediately, without waiting for the peer to come back — mirrors
            // vertex-dotnet, which throws TransportSendException here rather than
            // blocking. A caller may retry once the peer reconnects.
            if (!$this->connected) {
                throw new TransportSendException(\sprintf(
                    'vertex grpc: not connected to %s (reconnecting)',
                    $this->serverAddr
                ));
            }

            $last = \count($frames) - 1;
            foreach ($frames as $i => $frame) {
                $eom = $i === $last;
                $grpcFramed = $this->codec->encode($frame, $eom);

                // end_stream stays false: this is a long-lived bidi stream; we
                // half-close it only in close() via CloseSend semantics.
                $ok = $this->submit([$this->streamId, $grpcFramed, false]);
                if ($ok === false) {
                    throw new TransportSendException(\sprintf(
                        'vertex grpc: write frame %d/%d failed: %s',
                        $i + 1,
                        $last + 1,
                        $this->client->errMsg
                    ));
                }
            }
        } finally {
            // Release the lock regardless of outcome (invariant #2: a send
            // failure must not wedge the transport).
            $this->writeLock->push(true);
        }
    }

    /**
     * Gracefully shut down. Half-closes the send side so any frames already
     * written reach the server, waits for the recv loop to observe the server's
     * response-side close (so buffered writes flush before we drop the socket),
     * then stops both loops. Idempotent.
     */
    public function close(): void
    {
        if ($this->closed) {
            return;
        }
        $this->closed = true;

        // Wake the run loop if it's mid-backoff between reconnects.
        $this->closeSignal->push(true);

        if ($this->connected) {
            // Half-close client→server through the send loop: an empty DATA frame
            // with end_stream=true, ordered behind any frames already queued, so a
            // publish-then-close does not lose the envelope's tail.
            $this->submit([$this->streamId, '', true]);

            // Wait for the recv loop to drain to the server's response-side close
            // before tearing down. Swoole buffers writes; closing the socket while
            // just-written frames are in flight RST_STREAMs the connection and the
            // server never sees them. Bounded so a wedged peer can't hang close().
            $this->recvDone->pop(self::CLOSE_DRAIN_TIMEOUT);
        }

        // Stop the send loop (it owns the client write side and closes the socket
        // on exit). The run loop sees $closed, exits, and closes $inbound — the
        // sole inbound-close site, so callers blocked on receive() unblock once.
        $this->sendCh->close();
    }

    /**
     * The sole coroutine that touches the client's write side. Pops commands off
     * $sendCh — a Request to open the stream, or [streamId, data, end] to write —
     * runs each, and hands the result back on $sendRet. On $sendCh close it tears
     * the client down. Keeping every write on one coroutine satisfies Swoole's
     * single-writer requirement and serializes envelope frames (wire-format §4).
     */
    private function sendLoop(): void
    {
        while (true) {
            $cmd = $this->sendCh->pop(-1);
            if ($cmd === false) {
                // $sendCh closed by close(): no more writes. Drop the socket;
                // the recv loop observes the close and exits on its own.
                break;
            }

            if ($cmd instanceof Http2Request) {
                $ret = $this->client->send($cmd);
            } else {
                /** @var array{0:int,1:string,2:bool} $cmd */
                $ret = $this->client->write($cmd[0], $cmd[1], $cmd[2]);
            }
            $this->sendRet->push($ret);
        }

        if (isset($this->client)) {
            $this->client->close();
        }
    }

    /**
     * Hand one command to the send loop and await its result. Serialized by the
     * caller's write lock (send()) or only run during single-threaded connect()/
     * close(), so the 1-capacity $sendCh/$sendRet handshake never interleaves.
     *
     * @param Http2Request|array{0:int,1:string,2:bool} $cmd
     * @return int|bool the underlying send()/write() return (stream id or ok flag)
     */
    private function submit(Http2Request|array $cmd): int|bool
    {
        $this->sendCh->push($cmd);

        return $this->sendRet->pop();
    }

    /**
     * Invariant #1: the sole coroutine that calls recv(). Pulls gRPC-framed
     * TransportFrames off the stream, reassembles envelopes (4 frames, ending on
     * end_of_message), and pushes each completed envelope onto $inbound. Never
     * calls user code. Invariant #4: this loop's recv-failure / end-of-stream
     * path is the only place that concludes the peer is gone.
     */
    private function recvLoop(): void
    {
        $reassembler = new FrameReassembler($this->serverAddr, $this->codec);

        try {
            while (!$this->closed) {
                // recv(-1) blocks until a frame, a clean close (GOAWAY/FIN), or a
                // socket error. KNOWN LIMITATION: a *hard* disconnect (peer crash
                // / network cut with no GOAWAY) is only noticed when the OS TCP
                // stack gives up (minutes). Graceful disconnects — server restart,
                // rolling deploy — are detected immediately and reconnect works.
                // Future: bounded-timeout recv + a read-idle threshold to surface
                // dead connections as a read timeout (spec transport-contract §
                // invariant #4: "heartbeat should surface as a read-loop read
                // timeout, not a parallel source of truth").
                $response = $this->client->recv(-1);
                if ($response === false) {
                    // recv failure / stream end — the sole legitimate disconnect
                    // signal (invariant #4).
                    $this->connected = false;
                    break;
                }

                // Reassembly (partial / multi-frame / boundary handling) lives in
                // FrameReassembler so it can be unit-tested without a connection.
                foreach ($reassembler->feed($response->data ?? '') as $message) {
                    $this->inbound->push($message);
                }

                if (($response->pipeline ?? true) === false) {
                    // Server half-closed its response side: clean end of stream.
                    $this->connected = false;
                    break;
                }
            }
        } finally {
            // Unblock close()'s flush-before-close wait. Only push when close()
            // is the reason we're exiting — on a plain disconnect (reconnect
            // path) nobody pops $recvDone, and a second push would block the
            // next cycle's recv loop on the 1-capacity channel.
            if ($this->closed) {
                $this->recvDone->push(true);
            }
        }
    }

    /**
     * @return array{0: string, 1: int} [host, port]
     */
    private function splitAddr(string $addr): array
    {
        $pos = \strrpos($addr, ':');
        if ($pos === false) {
            throw new \InvalidArgumentException('vertex grpc: server address must be host:port, got ' . $addr);
        }

        return [\substr($addr, 0, $pos), (int) \substr($addr, $pos + 1)];
    }
}
