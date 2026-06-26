<?php

// Licensed to the Gordon under one or more agreements.
// Gordon licenses this file to you under the MIT license.

declare(strict_types=1);

namespace Vertex\Transport\Grpc;

/**
 * Encodes/decodes the gRPC-framed `TransportFrame` messages that carry Vertex
 * envelope frames over the bidi HTTP/2 stream (spec/wire-format.md §4, proto in
 * protos/vertex/transport/grpc/v1/bidi.proto).
 *
 * Two layers, both hand-rolled (no ext-grpc, no generated stub):
 *
 *   1. The protobuf message body — TransportFrame has exactly two fields:
 *        field 1  payload         bytes  (wire type 2, LEN)
 *        field 2  end_of_message  bool   (wire type 0, VARINT; omitted if false)
 *   2. The gRPC length prefix — a 5-byte header per message:
 *        1 byte   compressed flag (always 0 here)
 *        4 bytes  big-endian uint32 message length
 *
 * Kept separate from {@see GrpcClientTransport} so this byte-exact logic is
 * unit-testable without a live Swoole HTTP/2 connection.
 */
final class TransportFrameCodec
{
    private const FIELD_PAYLOAD = 1; // bytes
    private const FIELD_EOM = 2;     // bool

    /** Encode one TransportFrame, gRPC-length-prefixed and ready to write. */
    public function encode(string $payload, bool $endOfMessage): string
    {
        return $this->grpcLengthPrefix($this->encodeMessage($payload, $endOfMessage));
    }

    /**
     * Pull one complete gRPC message out of $buffer (mutated in place) and
     * decode it. Returns null if a full message isn't buffered yet, leaving
     * $buffer untouched for the next read.
     *
     * @return array{0: string, 1: bool}|null [payload, endOfMessage] or null
     */
    public function tryDecodeNext(string &$buffer): ?array
    {
        if (\strlen($buffer) < 5) {
            return null;
        }
        $len = \unpack('N', \substr($buffer, 1, 4))[1];
        if (\strlen($buffer) < 5 + $len) {
            return null; // partial message; wait for more bytes
        }
        $message = \substr($buffer, 5, $len);
        $buffer = \substr($buffer, 5 + $len);

        return $this->decodeMessage($message);
    }

    private function grpcLengthPrefix(string $message): string
    {
        return \chr(0) . \pack('N', \strlen($message)) . $message;
    }

    private function encodeMessage(string $payload, bool $eom): string
    {
        // field 1 (payload): tag (1<<3)|2 = 0x0A, length-delimited.
        $out = \chr((self::FIELD_PAYLOAD << 3) | 2)
            . $this->encodeVarint(\strlen($payload))
            . $payload;
        if ($eom) {
            // field 2 (end_of_message): tag (2<<3)|0 = 0x10, varint 1.
            // proto3 omits false, so we only emit the field when true.
            $out .= \chr((self::FIELD_EOM << 3) | 0) . \chr(1);
        }

        return $out;
    }

    /**
     * @return array{0: string, 1: bool} [payload, endOfMessage]
     */
    private function decodeMessage(string $message): array
    {
        $payload = '';
        $eom = false;
        $pos = 0;
        $len = \strlen($message);

        while ($pos < $len) {
            $tag = \ord($message[$pos++]);
            $field = $tag >> 3;
            $wire = $tag & 0x07;

            if ($field === self::FIELD_PAYLOAD && $wire === 2) {
                $n = $this->decodeVarint($message, $pos);
                $payload = \substr($message, $pos, $n);
                $pos += $n;
            } elseif ($field === self::FIELD_EOM && $wire === 0) {
                $eom = $this->decodeVarint($message, $pos) !== 0;
            } else {
                // Unknown field — skip it so a forward-compatible TransportFrame
                // addition doesn't break older clients.
                $pos = $this->skipField($message, $pos, $wire);
            }
        }

        return [$payload, $eom];
    }

    private function encodeVarint(int $value): string
    {
        $out = '';
        while ($value > 0x7F) {
            $out .= \chr(($value & 0x7F) | 0x80);
            $value >>= 7;
        }

        return $out . \chr($value & 0x7F);
    }

    private function decodeVarint(string $buf, int &$pos): int
    {
        $result = 0;
        $shift = 0;
        do {
            $byte = \ord($buf[$pos++]);
            $result |= ($byte & 0x7F) << $shift;
            $shift += 7;
        } while ($byte & 0x80);

        return $result;
    }

    private function skipField(string $buf, int $pos, int $wire): int
    {
        return match ($wire) {
            0 => $this->skipVarint($buf, $pos),       // varint
            1 => $pos + 8,                            // 64-bit
            2 => $this->skipLengthDelimited($buf, $pos), // bytes/string
            5 => $pos + 4,                            // 32-bit
            default => throw new \RuntimeException('vertex grpc: unknown protobuf wire type ' . $wire),
        };
    }

    private function skipVarint(string $buf, int $pos): int
    {
        $this->decodeVarint($buf, $pos);

        return $pos;
    }

    private function skipLengthDelimited(string $buf, int $pos): int
    {
        $n = $this->decodeVarint($buf, $pos);

        return $pos + $n;
    }
}
