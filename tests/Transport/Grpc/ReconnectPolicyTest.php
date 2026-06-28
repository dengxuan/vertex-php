<?php

// Licensed to the Gordon under one or more agreements.
// Gordon licenses this file to you under the MIT license.

declare(strict_types=1);

namespace Vertex\Tests\Transport\Grpc;

use PHPUnit\Framework\TestCase;
use Vertex\Transport\Grpc\ReconnectPolicy;

/**
 * Exponential-backoff-with-jitter math for the reconnect policy. The jitter
 * draw is injected so the curve is deterministic under test; these pin the same
 * 1s → 2s → 4s → … → 30s cap, ±20% behavior vertex-dotnet's ReconnectPolicy
 * produces, so a PHP client backs off on the same schedule as a .NET one.
 */
final class ReconnectPolicyTest extends TestCase
{
    public function testDefaultsMatchDotnet(): void
    {
        $p = ReconnectPolicy::default();

        $this->assertTrue($p->enabled);
        $this->assertSame(1.0, $p->initialBackoff);
        $this->assertSame(30.0, $p->maxBackoff);
        $this->assertSame(2.0, $p->multiplier);
        $this->assertSame(0.2, $p->jitter);
    }

    public function testDisabledTurnsReconnectOff(): void
    {
        $this->assertFalse(ReconnectPolicy::disabled()->enabled);
    }

    public function testBackoffDoublesEachAttemptWithJitterPinnedToCenter(): void
    {
        // rand=0.5 → offset = (0.5*2-1)*range = 0 → no jitter, exact base.
        $p = ReconnectPolicy::default();

        $this->assertEqualsWithDelta(1.0, $p->backoffFor(1, 0.5), 1e-9);
        $this->assertEqualsWithDelta(2.0, $p->backoffFor(2, 0.5), 1e-9);
        $this->assertEqualsWithDelta(4.0, $p->backoffFor(3, 0.5), 1e-9);
        $this->assertEqualsWithDelta(8.0, $p->backoffFor(4, 0.5), 1e-9);
    }

    public function testBackoffIsCappedAtMaxBackoff(): void
    {
        $p = ReconnectPolicy::default();

        // 2^10 = 1024s uncapped → must clamp to 30s (jitter pinned to center).
        $this->assertEqualsWithDelta(30.0, $p->backoffFor(11, 0.5), 1e-9);
    }

    public function testJitterPerturbsWithinFraction(): void
    {
        $p = ReconnectPolicy::default(); // attempt 2 → base 2.0, ±20% → [1.6, 2.4]

        // rand=1.0 → +full range = base*(1+jitter) = 2.4
        $this->assertEqualsWithDelta(2.4, $p->backoffFor(2, 1.0), 1e-9);
        // rand=0.0 → -full range = base*(1-jitter) = 1.6
        $this->assertEqualsWithDelta(1.6, $p->backoffFor(2, 0.0), 1e-9);
    }

    public function testBackoffNeverNegative(): void
    {
        // Pathological jitter=1.0 with rand=0.0 would give base-base=0, not below.
        $p = new ReconnectPolicy(jitter: 1.0);

        $this->assertGreaterThanOrEqual(0.0, $p->backoffFor(1, 0.0));
    }

    public function testJitterFractionClampedToZeroOne(): void
    {
        // jitter > 1 is clamped to 1, so the band is [0, 2*base], never wider.
        $p = new ReconnectPolicy(initialBackoff: 5.0, jitter: 5.0);

        $this->assertEqualsWithDelta(10.0, $p->backoffFor(1, 1.0), 1e-9); // base*(1+1)
        $this->assertEqualsWithDelta(0.0, $p->backoffFor(1, 0.0), 1e-9);  // base*(1-1)
    }
}
