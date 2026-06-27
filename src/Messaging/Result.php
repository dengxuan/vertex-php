<?php

// Licensed to the Gordon under one or more agreements.
// Gordon licenses this file to you under the MIT license.

declare(strict_types=1);

namespace Vertex\Messaging;

/**
 * Internal carrier the receive loop pushes into an invoke()'s pending slot to
 * unblock the waiting caller. Exactly one of three outcomes:
 *
 *   - success:    {@see $payload} holds the serialized response bytes.
 *   - error:      {@see $isError}; {@see $errorMessage} holds the remote text.
 *   - disconnect: {@see $isDisconnect}; the peer went away before responding.
 *
 * Not part of the public API — it never escapes MessagingChannel, which turns
 * each outcome into a return value or a typed RpcException.
 *
 * @internal
 */
final class Result
{
    private function __construct(
        public readonly string $payload,
        public readonly bool $isError,
        public readonly bool $isDisconnect,
        public readonly string $errorMessage,
    ) {
    }

    public static function success(string $payload): self
    {
        return new self($payload, isError: false, isDisconnect: false, errorMessage: '');
    }

    /** @param string $errorText the UTF-8 error message from the error RESPONSE payload */
    public static function error(string $errorText): self
    {
        return new self('', isError: true, isDisconnect: false, errorMessage: $errorText);
    }

    public static function disconnect(): self
    {
        return new self('', isError: false, isDisconnect: true, errorMessage: '');
    }
}
