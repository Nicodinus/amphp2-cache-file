<?php

namespace Amp\Cache\Test;

use Amp\ByteStream\InMemoryStream;
use Amp\ByteStream\InputStream;
use Amp\Cache\Cache;
use Amp\Cache\CacheItem;
use Amp\Emitter;
use Amp\PHPUnit\AsyncTestCase;
use Amp\Promise;
use function Amp\asyncCall;
use function Amp\call;
use function Amp\delay;

abstract class CacheTest extends AsyncTestCase
{
    /**
     * @return Cache
     */
    abstract protected function createCache(): Cache;

    /**
     * @param InputStream $stream
     *
     * @return Promise<string|null>
     */
    protected function buffer(InputStream $stream): Promise
    {
        return call(function () use (&$stream) {
            $buffer = null;
            while (true) {
                /** @var string|null $chunk */
                $chunk = yield $stream->read();
                if ($chunk === null) {
                    break;
                }

                if ($buffer === null) {
                    $buffer = $chunk;
                } else {
                    $buffer .= $chunk;
                }
            }

            return $buffer;
        });
    }

    public function testGet(): \Generator
    {
        $cache = $this->createCache();

        $this->assertNull(yield $cache->get("mykey"));

        yield $cache->set("mykey", "myvalue", 10);
        $this->assertSame("myvalue", yield $cache->get("mykey"));

        yield $cache->delete("mykey");
    }

    public function testGetItem(): \Generator
    {
        $cache = $this->createCache();
        $testKey = \uniqid("key_");
        $testValue = \uniqid("value_");

        // check simple set
        yield $cache->delete($testKey);
        $result = yield $cache->getItem($testKey);
        $this->assertNull($result);

        yield $cache->set($testKey, $testValue, 10);
        /** @var CacheItem $result */
        $result = yield $cache->getItem($testKey);

        $this->assertInstanceOf(CacheItem::class, $result);
        $this->assertSame($testValue, yield $this->buffer($result->getResult()));

        // check set from iterator
        yield $cache->delete($testKey);
        $result = yield $cache->getItem($testKey);
        $this->assertNull($result);

        $emitter = new Emitter();
        asyncCall(function () use (&$emitter, &$testValue) {
            yield $emitter->emit($testValue);
            $emitter->complete();
        });
        yield $cache->set($testKey, $emitter->iterate(), 10);
        /** @var CacheItem $result */
        $result = yield $cache->getItem($testKey);

        $this->assertInstanceOf(CacheItem::class, $result);
        $this->assertFalse($result->isIterable());
        $this->assertTrue($result->getStream() instanceof InputStream);

        /** @var array $result */
        $result = yield $this->buffer($result->getStream());
        $this->assertSame($testValue, $result);

        // check set from stream
        yield $cache->delete($testKey);
        $result = yield $cache->getItem($testKey);
        $this->assertNull($result);

        $stream = new InMemoryStream($testValue);
        yield $cache->set($testKey, $stream, 10);
        /** @var CacheItem $result */
        $result = yield $cache->getItem($testKey);

        $this->assertInstanceOf(CacheItem::class, $result);
        $this->assertTrue($result->isStream());
        $this->assertTrue($result->getStream() instanceof InputStream);

        $result = yield $this->buffer($result->getStream());
        $this->assertSame($testValue, $result);

        // end cleanup
        yield $cache->delete($testKey);
        $result = yield $cache->getItem($testKey);
        $this->assertNull($result);
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
