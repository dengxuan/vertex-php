<?php

// Licensed to the Gordon under one or more agreements.
// Gordon licenses this file to you under the MIT license.

declare(strict_types=1);

namespace Vertex\Messaging;

/**
 * The decoded view of the 4-frame message the wire carries.
 *
 * Wire layout — exactly 4 frames, strict order (spec/wire-format.md §2):
 *   frame 0: topic       UTF-8 string, no NUL terminator
 *   frame 1: kind        single byte (0=EVENT, 1=REQUEST, 2=RESPONSE)
 *   frame 2: request_id  UTF-8 string ("" for EVENT)
 *   frame 3: payload     opaque serializer bytes
 *
 * This is the language-neutral envelope; it is byte-for-byte the same shape
 * vertex-go's messaging.Envelope and vertex-dotnet's WireFormat produce, which
 * is what lets a PHP client interoperate with a .NET / Go server.
 */
final class Envelope
{
    /** Fire-and-forget publish. request_id MUST be empty. */
    public const KIND_EVENT = 0;
    /** RPC request. request_id MUST be present and unique. */
    public const KIND_REQUEST = 1;
    /** RPC reply. request_id matches the request; error topics start with "!". */
    public const KIND_RESPONSE = 2;

    /** Marks a RESPONSE envelope as carrying a UTF-8 error string (spec §2.4). */
    public const ERROR_TOPIC_PREFIX = '!';

    private const FRAME_COUNT = 4;

    public function __construct(
        public readonly string $topic,
        public readonly int $kind,
        public readonly string $requestId,
        public readonly string $payload,
    ) {
    }

    /**
     * Build the 4 wire frames. Byte order and contents match spec §2.
     *
     * @return list<string> exactly 4 binary-safe frame buffers
     */
    public function encode(): array
    {
        return [
            $this->topic,           // frame 0: topic
            \chr($this->kind),      // frame 1: kind (single byte)
            $this->requestId,       // frame 2: request_id
            $this->payload,         // frame 3: payload
        ];
    }

    /**
     * Parse exactly 4 frames into an Envelope.
     *
     * Per spec §2, fewer than 4 frames is a wire violation and the receiver
     * should drop the message — surfaced here as an exception the read path
     * catches and logs rather than treating as a disconnect (invariant #2).
     *
     * @param list<string> $frames
     *
     * @throws \InvalidArgumentException on a malformed frame set
     */
    public static function decode(array $frames): self
    {
        if (\count($frames) < self::FRAME_COUNT) {
            throw new \InvalidArgumentException(
                \sprintf('vertex messaging: envelope needs %d frames, got %d', self::FRAME_COUNT, \count($frames))
            );
        }
        if (\strlen($frames[1]) !== 1) {
            throw new \InvalidArgumentException('vertex messaging: kind frame must be exactly 1 byte');
        }

        return new self(
            topic: $frames[0],
            kind: \ord($frames[1]),
            requestId: $frames[2],
            payload: $frames[3],
        );
    }
}
