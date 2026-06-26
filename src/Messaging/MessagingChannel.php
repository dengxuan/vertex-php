<?php

// Licensed to the Gordon under one or more agreements.
// Gordon licenses this file to you under the MIT license.

declare(strict_types=1);

namespace Vertex\Messaging;

use Swoole\Coroutine;
use Vertex\Serialization\SerializerInterface;
use Vertex\Transport\TransportInterface;

/**
 * Client-side messaging layer on top of a {@see TransportInterface}. Stage 1
 * scope: Publish (fire-and-forget events) and event Subscribe. RPC
 * (Invoke / HandleRequest) is deferred to a later stage — see the repo README.
 *
 * Construct with a connected transport and a serializer, then publish() typed
 * messages or subscribe() to inbound events. A background receive coroutine
 * drains the transport and dispatches.
 */
final class MessagingChannel
{
    /** @var array<string, list<callable(object): void>> topic → handlers */
    private array $subscribers = [];

    /** @var array<string, class-string> topic → message class for decode */
    private array $topicTypes = [];

    private bool $closed = false;

    public function __construct(
        private readonly string $name,
        private readonly TransportInterface $transport,
        private readonly SerializerInterface $serializer,
    ) {
        // Invariant #1 lives in the transport's read loop; here we only consume
        // the already-routed inbound channel and fan out to handlers.
        Coroutine::create(fn () => $this->receiveLoop());
    }

    public function name(): string
    {
        return $this->name;
    }

    /**
     * Publish one event, fire-and-forget (envelope KIND_EVENT, empty
     * request_id). The topic is derived from the message type via the
     * serializer, so it matches the Go/.NET canonical topic automatically.
     *
     * @param string $target ignored for single-peer client transports (pass "")
     */
    public function publish(object $message, string $target = ''): void
    {
        $topic = $this->serializer->topicFor($message);
        $payload = $this->serializer->serialize($message);

        $envelope = new Envelope(
            topic: $topic,
            kind: Envelope::KIND_EVENT,
            requestId: '',
            payload: $payload,
        );

        $this->transport->send($target, $envelope->encode());
    }

    /**
     * Subscribe a handler to a topic. $messageType is the generated protobuf
     * class the payload decodes to; the canonical topic is derived from it so
     * callers don't hand-write topic strings.
     *
     * @template T of object
     * @param class-string<T>     $messageType
     * @param callable(T): void   $handler
     */
    public function subscribe(string $messageType, callable $handler): void
    {
        // Derive the topic from a throwaway instance of the type, mirroring the
        // publish side so subscribe/publish always agree on the topic string.
        $probe = new $messageType();
        $topic = $this->serializer->topicFor($probe);

        $this->topicTypes[$topic] = $messageType;
        $this->subscribers[$topic][] = $handler;
    }

    public function close(): void
    {
        if ($this->closed) {
            return;
        }
        $this->closed = true;
        $this->transport->close();
    }

    /**
     * Drain the transport's inbound channel and dispatch each envelope to the
     * registered handlers. Runs in its own coroutine; exits when the inbound
     * channel closes (transport shutdown).
     */
    private function receiveLoop(): void
    {
        $inbound = $this->transport->receive();
        while (!$this->closed) {
            $msg = $inbound->pop();
            if ($msg === false) {
                // Channel closed → transport is gone. Clean loop exit.
                break;
            }

            try {
                $envelope = Envelope::decode($msg->frames);
            } catch (\InvalidArgumentException) {
                // Malformed frame set: drop it (spec §2). A single bad message
                // is not a disconnect (invariant #2) — keep looping.
                continue;
            }

            if ($envelope->kind === Envelope::KIND_EVENT) {
                $this->dispatchEvent($envelope);
            }
            // KIND_REQUEST / KIND_RESPONSE handling lands with the RPC stage.
        }
    }

    private function dispatchEvent(Envelope $envelope): void
    {
        $handlers = $this->subscribers[$envelope->topic] ?? [];
        if ($handlers === []) {
            return;
        }
        $type = $this->topicTypes[$envelope->topic];
        $message = $this->serializer->deserialize($envelope->payload, $type);

        foreach ($handlers as $handler) {
            // Each handler in its own coroutine so one slow subscriber doesn't
            // head-of-line block the others or the receive loop (invariant #1's
            // spirit carried up into the messaging layer).
            Coroutine::create(fn () => $handler($message));
        }
    }
}
