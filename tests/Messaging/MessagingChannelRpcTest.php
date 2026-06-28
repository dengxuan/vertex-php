<?php

// Licensed to the Gordon under one or more agreements.
// Gordon licenses this file to you under the MIT license.

declare(strict_types=1);

namespace Vertex\Tests\Messaging;

use PHPUnit\Framework\TestCase;
use Swoole\Coroutine;
use Vertex\Messaging\Envelope;
use Vertex\Messaging\MessagingChannel;
use Vertex\Messaging\Rpc\RpcPeerDisconnectedException;
use Vertex\Messaging\Rpc\RpcRemoteException;
use Vertex\Messaging\Rpc\RpcTimeoutException;
use Vertex\Tests\Messaging\Fakes\FakeSerializer;
use Vertex\Tests\Messaging\Fakes\FakeTransport;
use Vertex\Tests\Messaging\Fakes\ReqMsg;
use Vertex\Tests\Messaging\Fakes\ResMsg;

/**
 * RPC behavior of MessagingChannel (Stage 2) — invoke()/handle() correlation,
 * error responses, timeout, and peer-disconnect failure — exercised against an
 * in-memory FakeTransport so the logic is tested without sockets. Wire-byte
 * fidelity is covered by EnvelopeTest; cross-language interop by compat/hello-rpc.
 *
 * Every test runs inside Swoole\Coroutine\run because MessagingChannel spawns
 * its receive loop as a coroutine and invoke() yields on a coroutine Channel.
 */
final class MessagingChannelRpcTest extends TestCase
{
    private const REQ_TOPIC = 'vertex.test.v1.Req';
    private const RES_TOPIC = 'vertex.test.v1.Res';

    private function newSerializer(): FakeSerializer
    {
        $s = new FakeSerializer();
        $s->register(ReqMsg::class, self::REQ_TOPIC);
        $s->register(ResMsg::class, self::RES_TOPIC);

        return $s;
    }

    public function testInvokeReturnsMatchingResponse(): void
    {
        $caught = null;
        $result = null;

        Coroutine\run(function () use (&$result, &$caught): void {
            $transport = new FakeTransport();
            $channel = new MessagingChannel('test', $transport, $this->newSerializer());

            // Responder: wait until the REQUEST has been sent, then reply with a
            // RESPONSE echoing its request_id.
            Coroutine::create(function () use ($transport): void {
                while ($transport->sent === []) {
                    Coroutine::sleep(0.001);
                }
                $req = $transport->sent[0];
                $transport->deliver(new Envelope(
                    topic: self::RES_TOPIC,
                    kind: Envelope::KIND_RESPONSE,
                    requestId: $req->requestId,
                    payload: 'pong',
                ));
            });

            try {
                $result = $channel->invoke(new ReqMsg('ping'), ResMsg::class, timeout: 2.0);
            } catch (\Throwable $e) {
                $caught = $e;
            }
            $channel->close();
        });

        $this->assertNull($caught);
        $this->assertInstanceOf(ResMsg::class, $result);
        $this->assertSame('pong', $result->body);
    }

    public function testInvokeSendsRequestEnvelopeWithUniqueHexRequestId(): void
    {
        $sentReq = null;

        Coroutine\run(function () use (&$sentReq): void {
            $transport = new FakeTransport();
            $channel = new MessagingChannel('test', $transport, $this->newSerializer());

            Coroutine::create(function () use ($transport): void {
                while ($transport->sent === []) {
                    Coroutine::sleep(0.001);
                }
                $req = $transport->sent[0];
                $transport->deliver(new Envelope(self::RES_TOPIC, Envelope::KIND_RESPONSE, $req->requestId, 'ok'));
            });

            $channel->invoke(new ReqMsg('x'), ResMsg::class, timeout: 2.0);
            $sentReq = $transport->sent[0];
            $channel->close();
        });

        $this->assertNotNull($sentReq);
        $this->assertSame(self::REQ_TOPIC, $sentReq->topic);
        $this->assertSame(Envelope::KIND_REQUEST, $sentReq->kind);
        // 32 lowercase hex chars, matching .NET's Guid.ToString("N").
        $this->assertMatchesRegularExpression('/^[0-9a-f]{32}$/', $sentReq->requestId);
    }

    public function testInvokeThrowsRemoteExceptionOnErrorResponse(): void
    {
        $caught = null;

        Coroutine\run(function () use (&$caught): void {
            $transport = new FakeTransport();
            $channel = new MessagingChannel('test', $transport, $this->newSerializer());

            Coroutine::create(function () use ($transport): void {
                while ($transport->sent === []) {
                    Coroutine::sleep(0.001);
                }
                $req = $transport->sent[0];
                // Error response: "!"-prefixed topic, UTF-8 message as payload.
                $transport->deliver(new Envelope(
                    topic: Envelope::ERROR_TOPIC_PREFIX . self::REQ_TOPIC,
                    kind: Envelope::KIND_RESPONSE,
                    requestId: $req->requestId,
                    payload: 'boom',
                ));
            });

            try {
                $channel->invoke(new ReqMsg('ping'), ResMsg::class, timeout: 2.0);
            } catch (\Throwable $e) {
                $caught = $e;
            }
            $channel->close();
        });

        $this->assertInstanceOf(RpcRemoteException::class, $caught);
        $this->assertSame('boom', $caught->getMessage());
    }

    public function testInvokeTimesOutWhenNoResponse(): void
    {
        $caught = null;

        Coroutine\run(function () use (&$caught): void {
            $transport = new FakeTransport();
            $channel = new MessagingChannel('test', $transport, $this->newSerializer());

            try {
                // No responder: the slot pop must time out.
                $channel->invoke(new ReqMsg('ping'), ResMsg::class, timeout: 0.1);
            } catch (\Throwable $e) {
                $caught = $e;
            }
            $channel->close();
        });

        $this->assertInstanceOf(RpcTimeoutException::class, $caught);
    }

    public function testInvokeFailsPendingOnPeerDisconnect(): void
    {
        $caught = null;

        Coroutine\run(function () use (&$caught): void {
            $transport = new FakeTransport();
            $channel = new MessagingChannel('test', $transport, $this->newSerializer());

            // Drop the peer shortly after the request is in flight; the grace
            // period then fires RpcPeerDisconnectedException.
            Coroutine::create(function () use ($transport): void {
                while ($transport->sent === []) {
                    Coroutine::sleep(0.001);
                }
                $transport->drop();
            });

            try {
                $channel->invoke(new ReqMsg('ping'), ResMsg::class, timeout: 5.0);
            } catch (\Throwable $e) {
                $caught = $e;
            }
            // Close so the receive loop's inbound pop() unblocks and the
            // coroutine exits (drop() no longer closes inbound — it only raises
            // the Disconnected event).
            $channel->close();
        });

        $this->assertInstanceOf(RpcPeerDisconnectedException::class, $caught);
    }

    public function testHandleRunsHandlerAndRepliesWithMatchingResponse(): void
    {
        Coroutine\run(function (): void {
            $transport = new FakeTransport();
            $channel = new MessagingChannel('test', $transport, $this->newSerializer());

            $channel->handle(ReqMsg::class, fn (ReqMsg $r): ResMsg => new ResMsg('handled:' . $r->body));

            // Deliver an inbound REQUEST as if a peer invoked us.
            $transport->deliver(new Envelope(self::REQ_TOPIC, Envelope::KIND_REQUEST, 'rid-123', 'hi'));

            // Wait for the channel to send the reply.
            while ($transport->sent === []) {
                Coroutine::sleep(0.001);
            }
            $reply = $transport->sent[0];
            $channel->close();

            $this->assertSame(self::REQ_TOPIC, $reply->topic);
            $this->assertSame(Envelope::KIND_RESPONSE, $reply->kind);
            $this->assertSame('rid-123', $reply->requestId);
            $this->assertSame('handled:hi', $reply->payload);
        });
    }

    public function testHandleMissingHandlerRepliesWithErrorResponse(): void
    {
        Coroutine\run(function (): void {
            $transport = new FakeTransport();
            $channel = new MessagingChannel('test', $transport, $this->newSerializer());

            // No handler registered for this topic.
            $transport->deliver(new Envelope(self::REQ_TOPIC, Envelope::KIND_REQUEST, 'rid-9', 'hi'));

            while ($transport->sent === []) {
                Coroutine::sleep(0.001);
            }
            $reply = $transport->sent[0];
            $channel->close();

            // Error response: "!"-prefixed topic, same request_id, UTF-8 message.
            $this->assertStringStartsWith(Envelope::ERROR_TOPIC_PREFIX, $reply->topic);
            $this->assertSame('rid-9', $reply->requestId);
            $this->assertStringContainsString('no RPC handler', $reply->payload);
        });
    }

    public function testHandleThrowingHandlerRepliesWithErrorMessage(): void
    {
        Coroutine\run(function (): void {
            $transport = new FakeTransport();
            $channel = new MessagingChannel('test', $transport, $this->newSerializer());

            $channel->handle(ReqMsg::class, function (ReqMsg $r): ResMsg {
                throw new \RuntimeException('handler exploded');
            });

            $transport->deliver(new Envelope(self::REQ_TOPIC, Envelope::KIND_REQUEST, 'rid-7', 'hi'));

            while ($transport->sent === []) {
                Coroutine::sleep(0.001);
            }
            $reply = $transport->sent[0];
            $channel->close();

            $this->assertStringStartsWith(Envelope::ERROR_TOPIC_PREFIX, $reply->topic);
            $this->assertSame('rid-7', $reply->requestId);
            $this->assertSame('handler exploded', $reply->payload);
        });
    }
}
