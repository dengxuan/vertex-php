<?php

// Licensed to the Gordon under one or more agreements.
// Gordon licenses this file to you under the MIT license.

declare(strict_types=1);

namespace Vertex\Transport;

/**
 * Emitted when a transport's connection to a peer changes liveness. The
 * messaging layer subscribes (see TransportInterface::onPeerConnectionChanged)
 * to react — e.g. fail pending RPC invokes when a peer goes away. Mirrors
 * vertex-dotnet's PeerConnectionEvent.
 */
final class PeerConnectionEvent
{
    public function __construct(
        public readonly string $peer,
        public readonly PeerConnectionState $state,
    ) {
    }
}
