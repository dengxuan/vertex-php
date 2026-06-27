<?php

// Licensed to the Gordon under one or more agreements.
// Gordon licenses this file to you under the MIT license.

declare(strict_types=1);

namespace Vertex\Tests\Transport\Grpc;

use PHPUnit\Framework\TestCase;
use Vertex\Transport\Grpc\FrameReassembler;
use Vertex\Transport\Grpc\TransportFrameCodec;
use Vertex\Transport\TransportMessage;

/**
 * Reassembly of gRPC-framed bytes into whole 4-frame Vertex envelopes. These
 * pin the fiddly cases the recv loop must survive — partial frames split across
 * recv() chunks, several frames in one chunk, envelope boundaries mid-chunk —
 * without needing a live Swoole connection.
 */
final class FrameReassemblerTest extends TestCase
{
    private const PEER = 'test-peer';

    private TransportFrameCodec $codec;

    protected function setUp(): void
    {
        $this->codec = new TransportFrameCodec();
    }

    /** Encode the 4 frames of one envelope as a single gRPC-framed byte blob. */
    private function encodeEnvelope(string ...$frames): string
    {
        $last = \count($frames) - 1;
        $blob = '';
        foreach ($frames as $i => $f) {
            $blob .= $this->codec->encode($f, $i === $last);
        }

        return $blob;
    }

    public function testFeedingOneCompleteEnvelopeYieldsOneMessage(): void
    {
        $r = new FrameReassembler(self::PEER, $this->codec);
        $blob = $this->encodeEnvelope('topic', "\x00", 'rid', 'payload');

        $out = $r->feed($blob);

        $this->assertCount(1, $out);
        $this->assertInstanceOf(TransportMessage::class, $out[0]);
        $this->assertSame(self::PEER, $out[0]->from);
        $this->assertSame(['topic', "\x00", 'rid', 'payload'], $out[0]->frames);
    }

    public function testPartialFrameAcrossTwoFeedsReassembles(): void
    {
        $r = new FrameReassembler(self::PEER, $this->codec);
        $blob = $this->encodeEnvelope('t', "\x01", 'r', 'p');

        // Split the blob mid-stream: first half yields nothing, the rest completes it.
        $half = \intdiv(\strlen($blob), 2);
        $this->assertSame([], $r->feed(\substr($blob, 0, $half)));

        $out = $r->feed(\substr($blob, $half));
        $this->assertCount(1, $out);
        $this->assertSame(['t', "\x01", 'r', 'p'], $out[0]->frames);
    }

    public function testTwoEnvelopesInOneChunkYieldTwoMessages(): void
    {
        $r = new FrameReassembler(self::PEER, $this->codec);
        $blob = $this->encodeEnvelope('a', "\x00", '', 'p1')
              . $this->encodeEnvelope('b', "\x00", '', 'p2');

        $out = $r->feed($blob);

        $this->assertCount(2, $out);
        $this->assertSame('a', $out[0]->frames[0]);
        $this->assertSame('b', $out[1]->frames[0]);
    }

    public function testEnvelopeBoundarySplitMidChunk(): void
    {
        $r = new FrameReassembler(self::PEER, $this->codec);
        $first = $this->encodeEnvelope('a', "\x00", '', 'p1');
        $second = $this->encodeEnvelope('b', "\x00", '', 'p2');

        // One chunk carries all of envelope A plus the first byte of envelope B.
        $out = $r->feed($first . \substr($second, 0, 1));
        $this->assertCount(1, $out);
        $this->assertSame('a', $out[0]->frames[0]);

        // The rest of B arrives next and completes it.
        $out = $r->feed(\substr($second, 1));
        $this->assertCount(1, $out);
        $this->assertSame('b', $out[0]->frames[0]);
    }

    public function testByteAtATimeFeedStillReassembles(): void
    {
        $r = new FrameReassembler(self::PEER, $this->codec);
        $blob = $this->encodeEnvelope('topic', "\x02", 'rid', 'payload');

        $messages = [];
        foreach (\str_split($blob, 1) as $byte) {
            foreach ($r->feed($byte) as $m) {
                $messages[] = $m;
            }
        }

        $this->assertCount(1, $messages);
        $this->assertSame(['topic', "\x02", 'rid', 'payload'], $messages[0]->frames);
    }

    public function testEmptyFeedYieldsNothing(): void
    {
        $r = new FrameReassembler(self::PEER, $this->codec);
        $this->assertSame([], $r->feed(''));
    }

    public function testBinarySafePayloadSurvivesReassembly(): void
    {
        $r = new FrameReassembler(self::PEER, $this->codec);
        $payload = "\x00\xff\x0a\x10binary\x00data";
        $blob = $this->encodeEnvelope('t', "\x00", '', $payload);

        $out = $r->feed($blob);

        $this->assertCount(1, $out);
        $this->assertSame($payload, $out[0]->frames[3]);
    }
}
