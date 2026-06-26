<?php

// Licensed to the Gordon under one or more agreements.
// Gordon licenses this file to you under the MIT license.

declare(strict_types=1);

namespace Vertex\Serialization;

/**
 * Turns a business message into payload bytes and back. The serializer also
 * owns the topic↔type mapping, because the canonical topic (spec §2.1) is
 * derived from the message type (for protobuf: the descriptor's full name).
 */
interface SerializerInterface
{
    /** Serialize a message object to payload bytes (envelope frame 3). */
    public function serialize(object $message): string;

    /**
     * Deserialize payload bytes into an instance of $type.
     *
     * @template T of object
     * @param class-string<T> $type
     * @return T
     */
    public function deserialize(string $payload, string $type): object;

    /**
     * Canonical wire topic for a message (spec §2.1). For protobuf this is the
     * descriptor full name, e.g. "vertex.compat.hello.v1.HelloEvent".
     */
    public function topicFor(object $message): string;
}
