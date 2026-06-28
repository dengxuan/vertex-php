<?php

// Licensed to the Gordon under one or more agreements.
// Gordon licenses this file to you under the MIT license.

declare(strict_types=1);

namespace Vertex\Tests\Transport\Grpc;

use PHPUnit\Framework\TestCase;
use Vertex\Transport\Grpc\GrpcClientTransport;

/**
 * Connect-time metadata merge: the caller's gRPC metadata (e.g. x-merchant-id /
 * x-signature for connection-level auth) is carried into the bidi request
 * headers, but the framework's reserved/pseudo headers always win. Pins the
 * precedence so an auth-enforcing server gets the credentials and a caller can
 * never break the transport by overriding :authority/content-type/te/user-agent.
 */
final class GrpcClientTransportHeadersTest extends TestCase
{
    public function testCallerMetadataIsCarried(): void
    {
        $headers = GrpcClientTransport::mergeHeaders(
            ['x-merchant-id' => 'm-123', 'x-signature' => 'abc'],
            '127.0.0.1:50051',
        );

        $this->assertSame('m-123', $headers['x-merchant-id']);
        $this->assertSame('abc', $headers['x-signature']);
    }

    public function testReservedHeadersAlwaysPresent(): void
    {
        $headers = GrpcClientTransport::mergeHeaders([], 'host:1234');

        $this->assertSame('application/grpc+proto', $headers['content-type']);
        $this->assertSame('trailers', $headers['te']);
        $this->assertSame('host:1234', $headers[':authority']);
        $this->assertSame('vertex-php/0.1 swoole', $headers['user-agent']);
    }

    public function testCallerCannotOverrideReservedHeaders(): void
    {
        $headers = GrpcClientTransport::mergeHeaders(
            [
                'content-type' => 'evil/type',
                'TE' => 'nope',                 // case-insensitive reserved match
                ':authority' => 'attacker:9999',
                'user-agent' => 'spoof',
                'x-merchant-id' => 'm-1',       // a real one still gets through
            ],
            'real-authority:443',
        );

        $this->assertSame('application/grpc+proto', $headers['content-type']);
        $this->assertSame('trailers', $headers['te']);
        $this->assertSame('real-authority:443', $headers[':authority']);
        $this->assertSame('vertex-php/0.1 swoole', $headers['user-agent']);
        // The non-reserved one survives.
        $this->assertSame('m-1', $headers['x-merchant-id']);
        // The spoofed reserved keys never leaked through under their own casing.
        $this->assertArrayNotHasKey('TE', $headers);
    }

    public function testEmptyMetadataYieldsOnlyReservedHeaders(): void
    {
        $headers = GrpcClientTransport::mergeHeaders([], 'h:1');

        $this->assertSame(
            ['content-type', 'te', ':authority', 'user-agent'],
            array_keys($headers),
        );
    }
}
