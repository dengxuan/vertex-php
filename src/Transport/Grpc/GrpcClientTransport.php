<?php

// Licensed to the Gordon under one or more agreements.
// Gordon licenses this file to you under the MIT license.

declare(strict_types=1);

namespace Vertex\Transport\Grpc;

use Swoole\Coroutine;
use Swoole\Coroutine\Channel;
use Swoole\Coroutine\Http2\Client as Http2Client;
use Swoole\Http2\Request as Http2Request;
use Vertex\Transport\TransportInterface;
use Vertex\Transport\TransportMessage;

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

    /** Signaled once by the recv loop on exit (flush-before-close, see close()). */
    private Channel $recvDone;

    private bool $connected = false;
    private bool $closed = false;

    /**
     * @param string $serverAddr host:port of the Vertex gRPC server
     * @param string $authority   :authority pseudo-header (defaults to serverAddr)
     * @param float  $sendTimeout seconds to wait for the write lock before send() throws
     */
    public function __construct(
        private readonly string $serverAddr,
        ?string $name = null,
        private readonly string $authority = '',
        private readonly float $sendTimeout = 5.0,
    ) {
        $this->name = $name ?? $serverAddr;
        $this->codec = new TransportFrameCodec();
        $this->inbound = new Channel(256);
        $this->sendCh = new Channel(1);
        $this->sendRet = new Channel(1);
        $this->recvDone = new Channel(1);
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
        [$host, $port] = $this->splitAddr($this->serverAddr);

        $this->client = new Http2Client($host, $port);
        $this->client->set([
            'timeout' => -1,            // the stream is long-lived; no overall timeout
            'open_http2_protocol' => true,
        ]);
        if (!$this->client->connect()) {
            throw new \RuntimeException(
                \sprintf('vertex grpc: connect %s failed: %s', $this->serverAddr, $this->client->errMsg)
            );
        }

        // The send loop owns the write side; start it before opening the stream
        // so the open itself goes through the same single-writer coroutine.
        Coroutine::create(fn () => $this->sendLoop());

        // Open the bidi stream: one long-lived HTTP/2 request. pipeline keeps the
        // request (send) side open for streaming writes; usePipelineRead makes
        // recv() surface the server's streamed DATA frames as they arrive rather
        // than buffering until end-of-stream (required for a never-ending bidi).
        $req = new Http2Request();
        $req->method = 'POST';
        $req->path = self::METHOD_PATH;
        $req->headers = [
            'content-type' => 'application/grpc+proto',
            'te' => 'trailers',
            ':authority' => $this->authority !== '' ? $this->authority : $this->serverAddr,
            'user-agent' => 'vertex-php/0.1 swoole',
        ];
        $req->pipeline = true;
        // Real Swoole runtime property (Swoole >= 4.5.3); the static stubs omit
        // it, so phpstan can't see it. Without it recv() buffers until end-of-
        // stream and never surfaces a long-lived bidi stream's DATA frames.
        // @phpstan-ignore property.notFound
        $req->usePipelineRead = true;

        $streamId = $this->submit($req);
        if ($streamId === false || $streamId <= 0) {
            throw new \RuntimeException(
                \sprintf('vertex grpc: open stream on %s failed: %s', $this->serverAddr, $this->client->errMsg)
            );
        }
        $this->streamId = $streamId;
        $this->connected = true;

        // Invariant #4: the recv loop is the sole owner of connection liveness.
        Coroutine::create(fn () => $this->recvLoop());
    }

    public function name(): string
    {
        return $this->name;
    }

    public function receive(): Channel
    {
        return $this->inbound;
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
            throw new \RuntimeException('vertex grpc: transport closed');
        }

        // Acquire the write lock — invariant #3's one legitimate cancel point.
        $token = $this->writeLock->pop($this->sendTimeout);
        if ($token === false) {
            throw new \RuntimeException('vertex grpc: timed out acquiring write lock');
        }

        try {
            if (!$this->connected) {
                throw new \RuntimeException('vertex grpc: not connected');
            }

            $last = \count($frames) - 1;
            foreach ($frames as $i => $frame) {
                $eom = $i === $last;
                $grpcFramed = $this->codec->encode($frame, $eom);

                // end_stream stays false: this is a long-lived bidi stream; we
                // half-close it only in close() via CloseSend semantics.
                $ok = $this->submit([$this->streamId, $grpcFramed, false]);
                if ($ok === false) {
                    throw new \RuntimeException(\sprintf(
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

        // Half-close client→server through the send loop: an empty DATA frame
        // with end_stream=true, ordered behind any frames already queued. A
        // publish-then-close does not lose the envelope's tail.
        if ($this->connected) {
            $this->submit([$this->streamId, '', true]);
        }

        // Wait for the recv loop to drain to the server's response-side close
        // before tearing down. Swoole buffers writes; closing the socket while
        // just-written frames are still in flight RST_STREAMs the connection and
        // the server never sees them. Bounded so a wedged peer can't hang close().
        if ($this->connected) {
            $this->recvDone->pop(self::CLOSE_DRAIN_TIMEOUT);
        }

        // Stop the send loop (it owns the client write side and closes the
        // socket on exit), then unblock anything still waiting on inbound.
        $this->sendCh->close();
        $this->inbound->close();
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
            // Unblock close()'s flush-before-close wait on every exit path.
            $this->recvDone->push(true);
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
