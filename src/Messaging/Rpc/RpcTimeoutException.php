<?php

// Licensed to the Gordon under one or more agreements.
// Gordon licenses this file to you under the MIT license.

declare(strict_types=1);

namespace Vertex\Messaging\Rpc;

/**
 * Thrown by invoke() when no RESPONSE for the request's request_id arrives
 * within the call's timeout (default 30s, matching vertex-dotnet). The request
 * may still have been received and handled by the peer — a timeout says only
 * that the reply did not arrive in time, not that nothing happened.
 */
final class RpcTimeoutException extends RpcException
{
}
