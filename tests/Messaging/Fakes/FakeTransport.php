<?php

// Licensed to the Gordon under one or more agreements.
// Gordon licenses this file to you under the MIT license.

declare(strict_types=1);

namespace Vertex\Tests\Messaging\Fakes;

use Swoole\Coroutine\Channel;
use Vertex\Messaging\Envelope;
use Vertex\Transport\PeerConnectionEvent;
use Vertex\Transport\PeerConnectionState;
use Vertex\Transport\TransportInterface;
use Vertex\Transport\TransportMessage;

/**
 * In-memory transport for MessagingChannel tests: no sockets, no network. Sent
 * envelopes are decoded and recorded on {@see $sent} so a test can assert what
 * the channel put on the wire; inbound envelopes are injected via {@see deliver()}
 * to drive the channel's receive loop. {@see drop()} simulates a peer disconnect
 * (invariant #4) by raising a Disconnected event.
 */
final class FakeTransport implements TransportInterface
{
    /** @var list<Envelope> every envelope passed to send(), decoded */
    public array $sent = [];

    private readonly Channel $inbound;

    /** @var list<callable(PeerConnectionEvent): void> */
    private array $peerHandlers = [];

    /** Optional hook run inside send(), e.g. to make a send fail. @var null|callable(Envelope): void */
    public $onSend = null;

    public function __construct(private readonly string $peer = 'fake-peer')
    {
        $this->inbound = new Channel(256);
    }

    public function name(): string
    {
        return 'fake';
    }

    public function send(string $target, array $frames): void
    {
        $envelope = Envelope::decode($frames);
        $this->sent[] = $envelope;
        if ($this->onSend !== null) {
            ($this->onSend)($envelope);
        }
    }

    public function receive(): Channel
    {
        return $this->inbound;
    }

    public function onPeerConnectionChanged(callable $handler): void
    {
        $this->peerHandlers[] = $handler;
        // Replay Connected at subscription time, mirroring a live transport.
        $handler(new PeerConnectionEvent($this->peer, PeerConnectionState::Connected));
    }

    public function close(): void
    {
        $this->inbound->close();
    }

    /** Inject an inbound envelope as if the peer sent it. */
    public function deliver(Envelope $envelope): void
    {
        $this->inbound->push(new TransportMessage($this->peer, $envelope->encode()));
    }

    /** Simulate the read loop concluding the peer is gone (raises Disconnected). */
    public function drop(): void
    {
        $event = new PeerConnectionEvent($this->peer, PeerConnectionState::Disconnected);
        foreach ($this->peerHandlers as $handler) {
            $handler($event);
        }
    }
}
