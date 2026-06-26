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
 * Invariants (spec/transport-contract.md):
 *   #1 {@see readLoop()} only reassembles frames and pushes onto $inbound.
 *   #2 {@see send()} throws on failure; never touches the connection state.
 *   #3 {@see send()} serializes writers via $writeLock; the timeout guards
 *      only lock acquisition, never a partially-written envelope.
 *   #4 Only {@see readLoop()}'s recv error path drives Disconnected.
 */
final class GrpcClientTransport implements TransportInterface
{
    /** gRPC method path for the Vertex bidi service. */
    private const METHOD_PATH = '/vertex.transport.grpc.v1.Bidi/Connect';

    private readonly string $name;
    private readonly TransportFrameCodec $codec;
    private Http2Client $client;
    private int $streamId = 0;

    /** Inbound queue; every element is a {@see TransportMessage} (Channel is untyped). */
    private Channel $inbound;

    /**
     * 1-capacity channel used as a coroutine mutex serializing concurrent
     * send() callers — HTTP/2 forbids interleaved writes on one stream, and
     * Vertex frames of distinct envelopes must not interleave (wire-format
     * §4). A buffered Channel pop/push is the Swoole idiom for a mutex whose
     * acquisition can time out (invariant #3's allowed pre-wire cancel point).
     */
    private Channel $writeLock;

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
        $this->writeLock = new Channel(1);
        $this->writeLock->push(true); // unlocked
    }

    /**
     * Establish the HTTP/2 connection and open the bidi stream. Must be called
     * inside a coroutine (Swoole\Coroutine\run or an existing coroutine).
     *
     * Spawns the read-loop coroutine. After this returns, send() will work and
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

        // Open the bidi stream: a single long-lived HTTP/2 request with
        // pipeline mode so we can keep writing DATA frames after the headers.
        $req = new Http2Request();
        $req->method = 'POST';
        $req->path = self::METHOD_PATH;
        $req->headers = [
            'content-type' => 'application/grpc+proto',
            'te' => 'trailers',
            ':authority' => $this->authority !== '' ? $this->authority : $this->serverAddr,
            'user-agent' => 'vertex-php/0.1 swoole',
        ];
        $req->pipeline = true; // keep the request stream open for streaming writes

        $streamId = $this->client->send($req);
        if ($streamId === false) {
            throw new \RuntimeException(
                \sprintf('vertex grpc: open stream on %s failed: %s', $this->serverAddr, $this->client->errMsg)
            );
        }
        $this->streamId = $streamId;
        $this->connected = true;

        // Invariant #4: the read loop is the sole owner of connection liveness.
        Coroutine::create(fn () => $this->readLoop());
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
     * hold the lock and begin writing DATA frames, we run to completion — a
     * mid-envelope abort would RST_STREAM the whole bidi stream and kill every
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
                $ok = $this->client->write($this->streamId, $grpcFramed, false);
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
     * Graceful shutdown. Half-closes the send side so any frames already
     * written reach the server (mirrors vertex-go's CloseSend on Close), drains
     * the read loop, then releases the HTTP/2 connection. Idempotent.
     */
    public function close(): void
    {
        if ($this->closed) {
            return;
        }
        $this->closed = true;

        // Half-close client->server: an empty DATA frame with end_stream=true.
        // Frames written before this are ordered ahead of the end-of-stream
        // marker on the HTTP/2 wire, so a publish-then-close does not lose the
        // envelope's tail (the exact failure vertex-go's 5e41b8d fixed).
        if ($this->connected && isset($this->client)) {
            @$this->client->write($this->streamId, '', true);
        }

        // Let the read loop observe the server's response-side close and exit
        // on its own (invariant #4). It pushes nothing more after that.
        $this->inbound->close();

        if (isset($this->client)) {
            $this->client->close();
        }
    }

    /**
     * Invariant #1: pull gRPC-framed TransportFrames off the stream, reassemble
     * envelopes (4 frames, ending on end_of_message), and push each completed
     * envelope onto $inbound. Never calls user code. Invariant #4: this loop's
     * recv-failure path is the only place that concludes the peer is gone.
     */
    private function readLoop(): void
    {
        /** @var list<string> $acc */
        $acc = [];
        $buffer = '';

        while (!$this->closed) {
            $response = $this->client->recv();
            if ($response === false) {
                // recv failure / stream end — the sole legitimate disconnect
                // signal (invariant #4). Stop; close() (or a future reconnect
                // policy) decides what happens next.
                $this->connected = false;
                break;
            }

            if ($response->data !== null && $response->data !== '') {
                $buffer .= $response->data;
                // A single HTTP/2 DATA chunk may contain several gRPC-framed
                // messages, or a partial one. Pull every complete message out.
                while (($frame = $this->codec->tryDecodeNext($buffer)) !== null) {
                    [$payload, $eom] = $frame;
                    $acc[] = $payload;
                    if ($eom) {
                        $this->inbound->push(new TransportMessage($this->serverAddr, $acc));
                        $acc = [];
                    }
                }
            }

            if ($response->pipeline === false) {
                // Server half-closed its response side: clean end of stream.
                $this->connected = false;
                break;
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
