<?php

// Licensed to the Gordon under one or more agreements.
// Gordon licenses this file to you under the MIT license.

declare(strict_types=1);

namespace Vertex\Transport;

/**
 * Liveness of a transport's connection to a peer. Mirrors vertex-dotnet's
 * PeerConnectionState. Only the transport's read loop concludes Disconnected
 * (transport-contract.md invariant #4); a failed send never does (#2).
 */
enum PeerConnectionState
{
    case Connected;
    case Disconnected;
}
