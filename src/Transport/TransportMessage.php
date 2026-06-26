<?php

// Licensed to the Gordon under one or more agreements.
// Gordon licenses this file to you under the MIT license.

declare(strict_types=1);

namespace Vertex\Transport;

/**
 * One inbound multi-frame message, as the read loop reassembled it from the
 * transport's frame stream. The messaging layer decodes {@see frames} into an
 * {@see \Vertex\Messaging\Envelope}.
 */
final class TransportMessage
{
    /**
     * @param string       $from   logical peer the message came from
     * @param list<string> $frames the reassembled envelope frames (binary-safe)
     */
    public function __construct(
        public readonly string $from,
        public readonly array $frames,
    ) {
    }
}
