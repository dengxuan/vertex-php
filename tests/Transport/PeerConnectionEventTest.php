<?php

// Licensed to the Gordon under one or more agreements.
// Gordon licenses this file to you under the MIT license.

declare(strict_types=1);

namespace Vertex\Tests\Transport;

use PHPUnit\Framework\TestCase;
use Vertex\Transport\PeerConnectionEvent;
use Vertex\Transport\PeerConnectionState;

/**
 * The peer-connection event value type the messaging layer subscribes to for
 * disconnect detection (it replaces the old "inbound channel closed" signal so
 * the channel survives a reconnecting transport). The live reconnect
 * orchestration in GrpcClientTransport (connect → read → disconnect → backoff →
 * reconnect) is exercised against a real server; the backoff math is covered by
 * ReconnectPolicyTest. These pin the event/state shape both sides agree on.
 */
final class PeerConnectionEventTest extends TestCase
{
    public function testCarriesPeerAndState(): void
    {
        $event = new PeerConnectionEvent('127.0.0.1:50051', PeerConnectionState::Connected);

        $this->assertSame('127.0.0.1:50051', $event->peer);
        $this->assertSame(PeerConnectionState::Connected, $event->state);
    }

    public function testStatesAreDistinct(): void
    {
        $this->assertNotSame(PeerConnectionState::Connected, PeerConnectionState::Disconnected);
    }
}
