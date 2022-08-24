<?php

namespace Amp\Cache\Test;

use Amp\Cache\Cache;
use Amp\PHPUnit\AsyncTestCase;
use function Amp\delay;

abstract class CacheTest extends AsyncTestCase
{
    abstract protected function createCache(): Cache;

    public function testGet(): \Generator
    {
        $cache = $this->createCache();

        $this->assertNull(yield $cache->get("mykey"));

        yield $cache->set("mykey", "myvalue", 10);
        $this->assertSame("myvalue", yield $cache->get("mykey"));

        yield $cache->delete("mykey");
    }

    public function testEntryIsNotReturnedAfterTTLHasPassed(): \Generator
    {
        $cache = $this->createCache();

        yield $cache->set("foo", "bar", 0);
        yield delay(10);
        $this->assertNull(yield $cache->get("foo"));
    }

    public function testEntryIsReturnedWhenOverriddenWithNoTimeout(): \Generator
    {
        $cache = $this->createCache();

        yield $cache->set("foo", "bar", 0);
        yield $cache->set("foo", "bar");

        $this->assertNotNull(yield $cache->get("foo"));
    }

    public function testEntryIsNotReturnedAfterDelete(): \Generator
    {
        $cache = $this->createCache();

        yield $cache->set("foo", "bar");
        yield $cache->delete("foo");

        $this->assertNull(yield $cache->get("foo"));
    }
}
