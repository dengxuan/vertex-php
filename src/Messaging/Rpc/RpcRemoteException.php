<?php

// Licensed to the Gordon under one or more agreements.
// Gordon licenses this file to you under the MIT license.

declare(strict_types=1);

namespace Vertex\Messaging\Rpc;

/**
 * Thrown by invoke() when the peer's RPC handler failed and replied with an
 * error RESPONSE: a topic prefixed with "!" whose payload is a UTF-8 error
 * message (spec/wire-format.md §2.4). The exception message is that remote
 * error text verbatim — there is no structured exception transfer on the wire.
 */
final class RpcRemoteException extends RpcException
{
}
