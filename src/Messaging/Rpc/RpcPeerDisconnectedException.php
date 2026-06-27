<?php

// Licensed to the Gordon under one or more agreements.
// Gordon licenses this file to you under the MIT license.

declare(strict_types=1);

namespace Vertex\Messaging\Rpc;

/**
 * Thrown to every still-pending invoke() when the transport's read loop
 * reports the peer gone (invariant #4) and the disconnect grace period
 * elapses without the response arriving. The grace period (200ms, matching
 * vertex-dotnet) gives an in-flight RESPONSE already on the wire a chance to
 * land and complete the call normally before we fail it.
 */
final class RpcPeerDisconnectedException extends RpcException
{
}
