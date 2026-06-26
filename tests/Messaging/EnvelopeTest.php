<?php

// Licensed to the Gordon under one or more agreements.
// Gordon licenses this file to you under the MIT license.

declare(strict_types=1);

namespace Vertex\Tests\Messaging;

use PHPUnit\Framework\TestCase;
use Vertex\Messaging\Envelope;

/**
 * Wire-format conformance for the 4-frame envelope (spec/wire-format.md §2).
 * These assertions pin the exact bytes a PHP client puts on the wire, which is
 * what guarantees interop with the Go / .NET sides.
 */
final class EnvelopeTest extends TestCase
{
    public function testEncodeProducesExactlyFourFramesInSpecOrder(): void
    {
        $env = new Envelope(
            topic: 'vertex.compat.hello.v1.HelloEvent',
            kind: Envelope::KIND_EVENT,
            requestId: '',
            payload: "\x0a\x05hello",
        );

        $frames = $env->encode();

        self::assertCount(4, $frames);
        self::assertSame('vertex.compat.hello.v1.HelloEvent', $frames[0], 'frame 0 = topic');
        self::assertSame("\x00", $frames[1], 'frame 1 = kind byte (EVENT=0)');
        self::assertSame('', $frames[2], 'frame 2 = request_id (empty for EVENT)');
        self::assertSame("\x0a\x05hello", $frames[3], 'frame 3 = payload, opaque bytes');
    }

    public function testKindByteEncodingForEachKind(): void
    {
        self::assertSame("\x00", (new Envelope('t', Envelope::KIND_EVENT, '', ''))->encode()[1]);
        self::assertSame("\x01", (new Envelope('t', Envelope::KIND_REQUEST, 'r1', ''))->encode()[1]);
        self::assertSame("\x02", (new Envelope('t', Envelope::KIND_RESPONSE, 'r1', ''))->encode()[1]);
    }

    public function testEncodeDecodeRoundTrip(): void
    {
        $original = new Envelope(
            topic: 'some.Topic',
            kind: Envelope::KIND_REQUEST,
            requestId: 'abc123def456',
            payload: \random_bytes(64),
        );

        $decoded = Envelope::decode($original->encode());

        self::assertSame($original->topic, $decoded->topic);
        self::assertSame($original->kind, $decoded->kind);
        self::assertSame($original->requestId, $decoded->requestId);
        self::assertSame($original->payload, $decoded->payload);
    }

    public function testDecodePreservesBinarySafePayload(): void
    {
        // Payload with embedded NULs and high bytes must survive untouched.
        $payload = "\x00\xff\x00\x01\x02\xfe";
        $decoded = Envelope::decode(['t', "\x00", '', $payload]);

        self::assertSame($payload, $decoded->payload);
    }

    public function testDecodeRejectsFewerThanFourFrames(): void
    {
        $this->expectException(\InvalidArgumentException::class);
        Envelope::decode(['topic', "\x00", '']); // only 3 frames
    }

    public function testDecodeRejectsMultiByteKindFrame(): void
    {
        $this->expectException(\InvalidArgumentException::class);
        Envelope::decode(['topic', "\x00\x00", '', '']); // kind must be 1 byte
    }

    public function testErrorTopicPrefixConstant(): void
    {
        // Spec §2.4: a response topic starting with "!" marks an error payload.
        self::assertSame('!', Envelope::ERROR_TOPIC_PREFIX);
    }
}
