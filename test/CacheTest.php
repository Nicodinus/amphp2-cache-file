<?php

namespace Amp\Cache\Test;

use Amp\ByteStream\InMemoryStream;
use Amp\ByteStream\InputStream;
use Amp\Cache\Cache;
use Amp\Emitter;
use Amp\PHPUnit\AsyncTestCase;
use Amp\Promise;
use function Amp\asyncCall;
use function Amp\call;
use function Amp\delay;
use function Amp\Iterator\toArray;

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

    public function testGetIterator(): \Generator
    {
        $cache = $this->createCache();

        $result = yield toArray($cache->getIterator("mykey"));
        $this->assertTrue(\sizeof($result) == 0);

        $emitter = new Emitter();

        asyncCall(function () use (&$emitter) {
            yield $emitter->emit("myvalue");
            $emitter->complete();
        });

        yield $cache->setIterator("mykey", $emitter->iterate(), 10);

        $result = yield toArray($cache->getIterator("mykey"));
        $this->assertTrue(\sizeof($result) == 1);
        $result = \array_pop($result);
        $this->assertSame("myvalue", $result);

        yield $cache->delete("mykey");
    }

    public function testGetStream(): \Generator
    {
        $cache = $this->createCache();

        $result = yield $this->buffer($cache->getStream("mykey"));
        $this->assertNull($result);

        yield $cache->setStream("mykey", new InMemoryStream("myvalue"), 10);
        $result = yield $this->buffer($cache->getStream("mykey"));
        $this->assertSame("myvalue", $result);

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
