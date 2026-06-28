<?php

// Licensed to the Gordon under one or more agreements.
// Gordon licenses this file to you under the MIT license.

declare(strict_types=1);

namespace Vertex\Transport\Grpc;

/**
 * gRPC transport reconnect policy: exponential backoff with jitter. The PHP
 * mirror of vertex-dotnet's ReconnectPolicy, with the same defaults so a PHP
 * client reconnects on the same curve as a .NET one.
 *
 * Backoff for attempt N (1-based) is
 *   min(maxBackoff, initialBackoff * multiplier^(N-1))
 * then perturbed by ±jitter to avoid a thundering herd. Default curve:
 * 1s → 2s → 4s → … → 30s cap, ±20%.
 */
final class ReconnectPolicy
{
    /**
     * @param bool  $enabled        reconnect after the underlying stream drops. When
     *                              false, one disconnect ends the transport: the recv
     *                              loop exits, receive() closes, and send() throws.
     * @param float $initialBackoff seconds to wait before the first reconnect
     * @param float $maxBackoff     ceiling on the backoff delay, seconds
     * @param float $multiplier     growth factor per failed attempt
     * @param float $jitter         jitter fraction in [0, 1] (±this proportion)
     */
    public function __construct(
        public readonly bool $enabled = true,
        public readonly float $initialBackoff = 1.0,
        public readonly float $maxBackoff = 30.0,
        public readonly float $multiplier = 2.0,
        public readonly float $jitter = 0.2,
    ) {
    }

    /** Default: 1s → 2s → 4s → … → 30s cap, ±20% jitter. */
    public static function default(): self
    {
        return new self();
    }

    /** No reconnect: one disconnect ends the transport (tests / one-shot scripts). */
    public static function disabled(): self
    {
        return new self(enabled: false);
    }

    /**
     * Backoff delay (seconds) before the given 1-based attempt, jitter applied.
     *
     * @param float $rand a uniform sample in [0, 1) (injectable so the jitter is
     *                    testable; defaults to a fresh random draw)
     */
    public function backoffFor(int $attempt, ?float $rand = null): float
    {
        $base = $this->initialBackoff * ($this->multiplier ** ($attempt - 1));
        $base = \min($this->maxBackoff, $base);

        $jitterFraction = \max(0.0, \min(1.0, $this->jitter));
        $range = $base * $jitterFraction;
        $sample = $rand ?? (\random_int(0, \PHP_INT_MAX) / \PHP_INT_MAX);
        $offset = ($sample * 2 - 1) * $range;

        return \max(0.0, $base + $offset);
    }
}
