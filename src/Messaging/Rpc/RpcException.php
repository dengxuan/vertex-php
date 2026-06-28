<?php

// Licensed to the Gordon under one or more agreements.
// Gordon licenses this file to you under the MIT license.

declare(strict_types=1);

namespace Vertex\Messaging\Rpc;

/**
 * Base type for every failure surfaced to an RPC caller (invoke()). Lets
 * callers catch all RPC-layer failures with one `catch (RpcException)` while
 * still distinguishing the specific cases below when they care.
 *
 * Mirrors the .NET caller-side exception trio (RpcTimeoutException,
 * RpcRemoteException, RpcPeerDisconnectedException) so behavior is the same
 * across languages — see spec/wire-format.md §2 and the vertex-dotnet
 * MessagingChannel.
 */
abstract class RpcException extends \RuntimeException
{
}
