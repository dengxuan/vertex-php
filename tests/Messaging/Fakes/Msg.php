<?php

// Licensed to the Gordon under one or more agreements.
// Gordon licenses this file to you under the MIT license.

declare(strict_types=1);

namespace Vertex\Tests\Messaging\Fakes;

/**
 * A minimal message for messaging-layer tests. The body is the opaque payload
 * {@see FakeSerializer} round-trips. Subclasses ({@see ReqMsg}, {@see ResMsg})
 * give the serializer distinct types to derive distinct topics from, mirroring
 * how real request/response proto types differ.
 */
class Msg
{
    public function __construct(public string $body = '')
    {
    }
}
