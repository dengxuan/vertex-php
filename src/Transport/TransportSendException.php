<?php

// Licensed to the Gordon under one or more agreements.
// Gordon licenses this file to you under the MIT license.

declare(strict_types=1);

namespace Vertex\Transport;

/**
 * Thrown by a transport's send() when one message cannot be put on the wire —
 * because the peer is disconnected (e.g. mid-reconnect), the write failed, or
 * the transport is closed. Mirrors vertex-dotnet's TransportSendException.
 *
 * Per invariant #2 this fails only the one send; it never tears down the
 * connection or trips a Disconnected event. When reconnect is enabled, a caller
 * that hits this during a backoff window may retry after the peer reconnects.
 */
final class TransportSendException extends \RuntimeException
{
}
