<?php

// Licensed to the Gordon under one or more agreements.
// Gordon licenses this file to you under the MIT license.

declare(strict_types=1);

namespace Vertex\Messaging;

use Swoole\Coroutine;
use Swoole\Coroutine\Channel;
use Vertex\Messaging\Rpc\RpcPeerDisconnectedException;
use Vertex\Messaging\Rpc\RpcRemoteException;
use Vertex\Messaging\Rpc\RpcTimeoutException;
use Vertex\Serialization\SerializerInterface;
use Vertex\Transport\TransportInterface;

/**
 * Client-side messaging layer on top of a {@see TransportInterface}. Supports
 * the full Stage-1 + Stage-2 surface: fire-and-forget events (publish /
 * subscribe) and request/response RPC (invoke / handle), matching the
 * vertex-dotnet MessagingChannel so a PHP peer interoperates with a .NET one.
 *
 * Construct with a connected transport and a serializer, then publish() / invoke()
 * typed messages, or subscribe() / handle() to serve inbound traffic. A
 * background receive coroutine drains the transport and demuxes by message kind.
 */
final class MessagingChannel
{
    /** invoke()'s default per-call timeout, seconds. Matches vertex-dotnet's 30s. */
    private const DEFAULT_INVOKE_TIMEOUT = 30.0;

    /**
     * Grace period (seconds) between the read loop reporting the peer gone and
     * failing still-pending invokes, letting an in-flight RESPONSE already on
     * the wire land first. Matches vertex-dotnet's 200ms.
     */
    private const PEER_DISCONNECT_GRACE = 0.2;

    /** @var array<string, list<callable(object): void>> topic → event handlers */
    private array $subscribers = [];

    /** @var array<string, class-string> topic → message class for event decode */
    private array $topicTypes = [];

    /**
     * Registered RPC handlers, keyed by request topic.
     *
     * @var array<string, array{requestType: class-string, handler: callable(object): object}>
     */
    private array $handlers = [];

    /**
     * In-flight invoke() calls, keyed by request_id. Each value is a 1-capacity
     * channel the receive loop pushes the matching RESPONSE into (a Result
     * tuple, see dispatchResponse()), unblocking the waiting caller coroutine.
     *
     * @var array<string, Channel>
     */
    private array $pending = [];

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
     * Invoke an RPC and await the typed response (envelope KIND_REQUEST →
     * matching KIND_RESPONSE). Blocks the calling coroutine until the response
     * arrives, the timeout elapses, or the peer disconnects.
     *
     * @template T of object
     * @param object          $request      the request message
     * @param class-string<T> $responseType the expected response class
     * @param string          $target       ignored for single-peer client transports
     * @param float|null      $timeout      seconds to await the response (default 30s)
     * @return T
     *
     * @throws RpcTimeoutException          no response within $timeout
     * @throws RpcRemoteException           peer's handler failed (error RESPONSE)
     * @throws RpcPeerDisconnectedException peer went away before responding
     * @throws \RuntimeException            transport send failure / channel closed
     */
    public function invoke(
        object $request,
        string $responseType,
        string $target = '',
        ?float $timeout = null,
    ): object {
        if ($this->closed) {
            throw new \RuntimeException('vertex messaging: channel closed');
        }

        $topic = $this->serializer->topicFor($request);
        $payload = $this->serializer->serialize($request);
        $requestId = $this->newRequestId();

        // Register the slot BEFORE sending, so a fast response can never race
        // ahead of the pending entry and be dropped as "unknown requestId".
        $slot = new Channel(1);
        $this->pending[$requestId] = $slot;

        try {
            $envelope = new Envelope(
                topic: $topic,
                kind: Envelope::KIND_REQUEST,
                requestId: $requestId,
                payload: $payload,
            );
            $this->transport->send($target, $envelope->encode());

            // Block on the slot. Channel::pop returns false on timeout (and the
            // value pushed by dispatchResponse() otherwise). A disconnect pushes
            // a Result with isDisconnect set (see receiveLoop's drain path).
            $result = $slot->pop($timeout ?? self::DEFAULT_INVOKE_TIMEOUT);
        } finally {
            unset($this->pending[$requestId]);
        }

        if ($result === false) {
            throw new RpcTimeoutException(\sprintf(
                'vertex messaging: RPC "%s" timed out after %.3fs on channel "%s"',
                $topic,
                $timeout ?? self::DEFAULT_INVOKE_TIMEOUT,
                $this->name,
            ));
        }

        /** @var Result $result */
        if ($result->isDisconnect) {
            throw new RpcPeerDisconnectedException(\sprintf(
                'vertex messaging: peer disconnected before responding to RPC "%s"',
                $topic,
            ));
        }
        if ($result->isError) {
            throw new RpcRemoteException($result->errorMessage);
        }

        return $this->serializer->deserialize($result->payload, $responseType);
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

    /**
     * Register an RPC handler for a request type. The handler receives the
     * decoded request and returns the response message; the channel sends it
     * back as a matching KIND_RESPONSE. A thrown handler becomes an error
     * RESPONSE (topic prefixed "!", payload the exception message).
     *
     * @template TReq of object
     * @template TRes of object
     * @param class-string<TReq>     $requestType
     * @param callable(TReq): TRes   $handler
     */
    public function handle(string $requestType, callable $handler): void
    {
        $probe = new $requestType();
        $topic = $this->serializer->topicFor($probe);

        $this->handlers[$topic] = [
            'requestType' => $requestType,
            'handler' => $handler,
        ];
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
     * Drain the transport's inbound channel and demux each envelope by kind.
     * Runs in its own coroutine; exits when the inbound channel closes, which
     * per invariant #4 is the sole disconnect signal — at which point pending
     * invokes are failed after a short grace period.
     */
    private function receiveLoop(): void
    {
        $inbound = $this->transport->receive();
        while (!$this->closed) {
            $msg = $inbound->pop();
            if ($msg === false) {
                // Channel closed → the read loop reported the peer gone. This
                // is the only place a disconnect is concluded (invariant #4).
                $this->failPendingOnDisconnect();
                break;
            }

            try {
                $envelope = Envelope::decode($msg->frames);
            } catch (\InvalidArgumentException) {
                // Malformed frame set: drop it (spec §2). A single bad message
                // is not a disconnect (invariant #2) — keep looping.
                continue;
            }

            switch ($envelope->kind) {
                case Envelope::KIND_EVENT:
                    $this->dispatchEvent($envelope);
                    break;
                case Envelope::KIND_REQUEST:
                    // Each request in its own coroutine: a slow handler must not
                    // head-of-line block the receive loop (invariant #1).
                    Coroutine::create(fn () => $this->dispatchRequest($msg->from, $envelope));
                    break;
                case Envelope::KIND_RESPONSE:
                    $this->dispatchResponse($envelope);
                    break;
                    // Unknown kinds are dropped (spec §2.2). Envelope::decode only
                    // yields 0..255; values ≥3 simply match no case here.
            }
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
            // head-of-line block the others or the receive loop (invariant #1).
            Coroutine::create(fn () => $handler($message));
        }
    }

    /**
     * Handle one inbound KIND_REQUEST: decode, run the registered handler, and
     * reply with a KIND_RESPONSE of matching request_id. A missing handler or a
     * thrown handler becomes an error response (topic "!"-prefixed, payload the
     * UTF-8 message). Runs in its own coroutine (see receiveLoop).
     */
    private function dispatchRequest(string $peer, Envelope $request): void
    {
        $registration = $this->handlers[$request->topic] ?? null;
        if ($registration === null) {
            $this->sendError(
                $peer,
                $request->topic,
                $request->requestId,
                \sprintf('no RPC handler registered for "%s" on channel "%s"', $request->topic, $this->name),
            );
            return;
        }

        try {
            $reqMessage = $this->serializer->deserialize($request->payload, $registration['requestType']);
            $response = ($registration['handler'])($reqMessage);

            $responseEnvelope = new Envelope(
                topic: $request->topic,
                kind: Envelope::KIND_RESPONSE,
                requestId: $request->requestId,
                payload: $this->serializer->serialize($response),
            );
            $this->transport->send($peer, $responseEnvelope->encode());
        } catch (\Throwable $e) {
            // A handler failure (or send failure of the success reply) fails
            // only this one request (invariant #2). Report it to the caller as
            // an error response; never disturb the connection.
            $this->sendError($peer, $request->topic, $request->requestId, $e->getMessage());
        }
    }

    /**
     * Complete the pending invoke() matching this RESPONSE's request_id. An
     * unknown request_id (already timed out / failed) is dropped.
     */
    private function dispatchResponse(Envelope $response): void
    {
        $slot = $this->pending[$response->requestId] ?? null;
        if ($slot === null) {
            return;
        }

        $isError = \str_starts_with($response->topic, Envelope::ERROR_TOPIC_PREFIX);
        $slot->push($isError
            ? Result::error($response->payload) // payload is the UTF-8 error text
            : Result::success($response->payload));
    }

    /**
     * Build and send an error RESPONSE: topic prefixed with "!" and a UTF-8
     * error message as the payload (spec §2.4). Best-effort — a failure to
     * deliver the error must not disturb the connection (invariant #2).
     */
    private function sendError(string $peer, string $topic, string $requestId, string $message): void
    {
        try {
            $envelope = new Envelope(
                topic: Envelope::ERROR_TOPIC_PREFIX . $topic,
                kind: Envelope::KIND_RESPONSE,
                requestId: $requestId,
                payload: $message,
            );
            $this->transport->send($peer, $envelope->encode());
        } catch (\Throwable) {
            // Swallow: the caller will time out, which is the correct outcome
            // when we cannot even deliver the error.
        }
    }

    /**
     * Peer is gone. After a short grace period (letting any in-flight RESPONSE
     * already on the wire complete its call first), fail every still-pending
     * invoke with RpcPeerDisconnectedException. push() into an already-completed
     * slot is harmless: that caller has unset its entry, so it's no longer here.
     */
    private function failPendingOnDisconnect(): void
    {
        Coroutine::sleep(self::PEER_DISCONNECT_GRACE);
        foreach ($this->pending as $slot) {
            $slot->push(Result::disconnect());
        }
    }

    /**
     * A 128-bit random request_id as 32 lowercase hex chars (no hyphens),
     * matching vertex-dotnet's Guid.ToString("N"). Uniqueness scope is
     * per-sender-per-peer (spec §2.3).
     */
    private function newRequestId(): string
    {
        return \bin2hex(\random_bytes(16));
    }
}
