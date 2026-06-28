<?php

// Licensed to the Gordon under one or more agreements.
// Gordon licenses this file to you under the MIT license.

declare(strict_types=1);

namespace Vertex\Tests\Messaging\Fakes;

use Vertex\Serialization\SerializerInterface;

/**
 * Trivial serializer for messaging-layer tests: it works on {@see Msg} value
 * objects, encoding the body as raw UTF-8 and using a fixed per-type topic.
 * Lets MessagingChannel be tested without pulling in protobuf descriptors —
 * the wire bytes themselves are covered by EnvelopeTest and the compat suite.
 */
final class FakeSerializer implements SerializerInterface
{
    /** @var array<class-string, string> message class → canonical topic */
    private array $topics = [];

    /** Map a message class to the topic it serializes to/from. */
    public function register(string $class, string $topic): void
    {
        $this->topics[$class] = $topic;
    }

    public function serialize(object $message): string
    {
        \assert($message instanceof Msg);

        return $message->body;
    }

    public function deserialize(string $payload, string $type): object
    {
        \assert(\is_a($type, Msg::class, allow_string: true));

        return new $type($payload);
    }

    public function topicFor(object $message): string
    {
        return $this->topics[$message::class] ?? $message::class;
    }
}
