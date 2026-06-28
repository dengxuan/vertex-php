<?php

// Licensed to the Gordon under one or more agreements.
// Gordon licenses this file to you under the MIT license.

declare(strict_types=1);

namespace Vertex\Transport\Grpc;

use Vertex\Transport\TransportMessage;

/**
 * Stateful reassembler turning a stream of gRPC-framed bytes into whole Vertex
 * envelopes. One HTTP/2 DATA chunk may carry several gRPC-framed TransportFrame
 * messages, a partial one, or span an envelope boundary — this holds the
 * leftover bytes ($buffer) and the frames accumulated so far for the current
 * envelope ($acc) across feed() calls, emitting a {@see TransportMessage} each
 * time a 4th frame (end_of_message) completes one.
 *
 * Pure logic, no I/O — the gRPC client's read loop feeds it raw recv() data.
 * Lives apart from the transport so the fiddly partial-frame / multi-frame /
 * boundary cases are unit-testable without a live connection.
 */
final class FrameReassembler
{
    private readonly TransportFrameCodec $codec;

    /**
     * Frames accumulated for the in-progress envelope (reset after each EOM).
     *
     * @var list<string>
     */
    private array $acc = [];

    /** Undecoded leftover bytes carried across feed() calls. */
    private string $buffer = '';

    public function __construct(
        private readonly string $peer,
        ?TransportFrameCodec $codec = null,
    ) {
        $this->codec = $codec ?? new TransportFrameCodec();
    }

    /**
     * Feed one chunk of recv() bytes; return every envelope that became complete.
     * Partial frames and partial envelopes stay buffered for the next call.
     *
     * @return list<TransportMessage> zero or more fully reassembled envelopes
     */
    public function feed(string $data): array
    {
        if ($data === '') {
            return [];
        }

        $this->buffer .= $data;

        $completed = [];
        while (($frame = $this->codec->tryDecodeNext($this->buffer)) !== null) {
            [$payload, $eom] = $frame;
            $this->acc[] = $payload;
            if ($eom) {
                $completed[] = new TransportMessage($this->peer, $this->acc);
                $this->acc = [];
            }
        }

        return $completed;
    }
}
