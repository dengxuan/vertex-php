<?php

// Licensed to the Gordon under one or more agreements.
// Gordon licenses this file to you under the MIT license.

declare(strict_types=1);

namespace Vertex\Serialization;

use Google\Protobuf\Internal\DescriptorPool;
use Google\Protobuf\Internal\Message;

/**
 * Serializes google/protobuf generated messages. The wire payload is the
 * standard protobuf binary encoding; the canonical topic is the descriptor's
 * full name (spec §2.1), so a PHP-published HelloEvent lands on the exact same
 * topic — "vertex.compat.hello.v1.HelloEvent" — that the Go and .NET sides use.
 *
 * The topic is normally derived by reflecting the generated descriptor. That
 * relies on the Internal DescriptorPool API, which has shifted across protobuf
 * runtime majors, so callers may also {@see register()} an explicit class→topic
 * mapping — that path needs no reflection and is the robust option if a runtime
 * upgrade ever moves the descriptor API.
 */
final class ProtobufSerializer implements SerializerInterface
{
    /** @var array<class-string, string> memoized / explicitly-registered class → full proto name */
    private array $topicCache = [];

    /**
     * Pin a class → canonical topic mapping explicitly, bypassing descriptor
     * reflection. Useful when the protobuf runtime's Internal descriptor API
     * isn't available, or to avoid the one-time reflection cost.
     *
     * @param class-string $class
     */
    public function register(string $class, string $topic): void
    {
        $this->topicCache[$class] = $topic;
    }

    public function serialize(object $message): string
    {
        if (!$message instanceof Message) {
            throw new \InvalidArgumentException(
                'vertex: ProtobufSerializer requires a google/protobuf Message, got ' . $message::class
            );
        }

        return $message->serializeToString();
    }

    public function deserialize(string $payload, string $type): object
    {
        if (!\is_subclass_of($type, Message::class)) {
            throw new \InvalidArgumentException(
                'vertex: ProtobufSerializer can only deserialize google/protobuf Messages, got ' . $type
            );
        }

        /** @var Message $message */
        $message = new $type();
        $message->mergeFromString($payload);

        return $message;
    }

    public function topicFor(object $message): string
    {
        $class = $message::class;
        if (isset($this->topicCache[$class])) {
            return $this->topicCache[$class];
        }

        $pool = DescriptorPool::getGeneratedPool();
        $descriptor = $pool->getDescriptorByClassName($class);
        if ($descriptor === null) {
            throw new \InvalidArgumentException(
                'vertex: no protobuf descriptor registered for ' . $class
                . ' — autoload the generated *.php metadata, or call '
                . ProtobufSerializer::class . '::register() to pin the topic explicitly.'
            );
        }

        return $this->topicCache[$class] = $descriptor->getFullName();
    }
}
