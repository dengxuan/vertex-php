<?php

// Licensed to the Gordon under one or more agreements.
// Gordon licenses this file to you under the MIT license.

declare(strict_types=1);

namespace Vertex\Tests\Transport\Grpc;

use PHPUnit\Framework\TestCase;
use Vertex\Transport\Grpc\TransportFrameCodec;

/**
 * Byte-exact conformance for the gRPC TransportFrame framing — the part most
 * likely to break interop, hand-rolled and therefore most in need of pinning.
 * Matches protos/vertex/transport/grpc/v1/bidi.proto: payload=field1 (bytes),
 * end_of_message=field2 (bool).
 */
final class TransportFrameCodecTest extends TestCase
{
    public function testEncodeNonEomBytesAreExact(): void
    {
        $codec = new TransportFrameCodec();
        $encoded = $codec->encode('hi', false);

        // gRPC prefix: 0x00 flag + BE uint32 length, then the protobuf body.
        // body = tag 0x0A (field1, LEN) + varint len(2) + "hi" = 4 bytes.
        $expectedBody = "\x0a\x02hi";
        $expected = "\x00" . \pack('N', \strlen($expectedBody)) . $expectedBody;

        self::assertSame(\bin2hex($expected), \bin2hex($encoded));
    }

    public function testEncodeEomAppendsField2(): void
    {
        $codec = new TransportFrameCodec();
        $encoded = $codec->encode('', true);

        // empty payload: field1 tag 0x0A + varint 0. eom: field2 tag 0x10 + 0x01.
        $expectedBody = "\x0a\x00\x10\x01";
        $expected = "\x00" . \pack('N', \strlen($expectedBody)) . $expectedBody;

        self::assertSame(\bin2hex($expected), \bin2hex($encoded));
    }

    public function testRoundTripPreservesPayloadAndEom(): void
    {
        $codec = new TransportFrameCodec();
        $payload = \random_bytes(300); // forces a 2-byte varint length
        $buffer = $codec->encode($payload, true);

        $frame = $codec->tryDecodeNext($buffer);

        self::assertNotNull($frame);
        self::assertSame($payload, $frame[0]);
        self::assertTrue($frame[1]);
        self::assertSame('', $buffer, 'buffer fully consumed');
    }

    public function testDecodeDefaultsEomToFalseWhenFieldOmitted(): void
    {
        $codec = new TransportFrameCodec();
        $buffer = $codec->encode('data', false); // proto3 omits end_of_message

        $frame = $codec->tryDecodeNext($buffer);

        self::assertNotNull($frame);
        self::assertSame('data', $frame[0]);
        self::assertFalse($frame[1]);
    }

    public function testTryDecodeReturnsNullOnPartialMessage(): void
    {
        $codec = new TransportFrameCodec();
        $full = $codec->encode('hello', true);

        // Feed only the first few bytes — not even the 5-byte prefix complete.
        $partial = \substr($full, 0, 3);
        self::assertNull($codec->tryDecodeNext($partial));
        self::assertSame(\substr($full, 0, 3), $partial, 'partial buffer left intact');

        // Prefix complete but body truncated.
        $partial2 = \substr($full, 0, 6);
        self::assertNull($codec->tryDecodeNext($partial2));
    }

    public function testDecodesMultipleFramesFromOneBuffer(): void
    {
        $codec = new TransportFrameCodec();
        // Four frames of one envelope concatenated in a single buffer, as a
        // single HTTP/2 DATA chunk could deliver them.
        $buffer = $codec->encode('topic', false)
            . $codec->encode("\x00", false)
            . $codec->encode('', false)
            . $codec->encode('payload', true);

        $f0 = $codec->tryDecodeNext($buffer);
        $f1 = $codec->tryDecodeNext($buffer);
        $f2 = $codec->tryDecodeNext($buffer);
        $f3 = $codec->tryDecodeNext($buffer);

        self::assertSame('topic', $f0[0]);
        self::assertFalse($f0[1]);
        self::assertSame("\x00", $f1[0]);
        self::assertSame('', $f2[0]);
        self::assertSame('payload', $f3[0]);
        self::assertTrue($f3[1], 'last frame carries end_of_message');
        self::assertSame('', $buffer);
        self::assertNull($codec->tryDecodeNext($buffer), 'nothing left to decode');
    }
}
