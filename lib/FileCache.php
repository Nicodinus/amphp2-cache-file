<?php

namespace Amp\Cache;

use Amp\File;
use Amp\Loop;
use Amp\Promise;
use Amp\Sync\KeyedMutex;
use Amp\Sync\LocalKeyedMutex;
use Amp\Sync\Lock;
use function Amp\call;

class FileCache implements Cache
{
    /** @var string */
    private string $directory;

    /** @var KeyedMutex|null */
    private ?KeyedMutex $mutex;

    /** @var string|null */
    private ?string $gcWatcher;

    /**
     * @param string $directory
     * @param KeyedMutex|null $mutex
     */
    public function __construct(string $directory, KeyedMutex $mutex = null)
    {
        $this->directory = \rtrim($directory, "/\\");
        $this->mutex = $mutex;

        $gcWatcher = \Closure::fromCallable([$this, '_watchExpiredCache']);

        // trigger once, so short running scripts also GC and don't grow forever
        Loop::defer($gcWatcher);

        $this->gcWatcher = Loop::repeat(300000, $gcWatcher);
        Loop::unreference($this->gcWatcher);
    }

    /**
     * @return void
     */
    public function __destruct()
    {
        if (!empty($this->gcWatcher)) {
            Loop::cancel($this->gcWatcher);
            $this->gcWatcher = null;
        }
    }

    /**
     * Calculates filename for cache by $key.
     *
     * @param string $key
     *
     * @return string
     */
    protected function getFilename(string $key): string
    {
        return \hash('sha256', $key) . '.cache';
    }

    /**
     * @return \Generator
     */
    protected function _watchExpiredCache(): \Generator
    {
        try {
            $files = yield File\listFiles($this->directory);

            foreach ($files as $file) {
                if (\strlen($file) !== 70 || \substr($file, -\strlen('.cache')) !== '.cache') {
                    continue;
                }

                /** @var Lock $lock */
                $lock = yield $this->getMutex()->acquire($file);
                \assert($lock instanceof Lock);

                try {
                    /** @var File\File $handle */
                    $handle = yield File\openFile($this->directory . '/' . $file, 'r');
                    $ttl = yield $handle->read(4);

                    if ($ttl === null || \strlen($ttl) !== 4) {
                        yield $handle->close();
                        continue;
                    }

                    $ttl = \unpack('Nttl', $ttl)['ttl'];
                    if ($ttl < \time()) {
                        yield File\deleteFile($this->directory . '/' . $file);
                    }
                } catch (\Throwable $e) {
                    // ignore
                } finally {
                    $lock->release();
                }
            }
        } catch (\Throwable $e) {
            // ignore
        }
    }

    /**
     * @return KeyedMutex
     */
    public function getMutex(): KeyedMutex
    {
        if (empty($this->mutex)) {
            $this->mutex = new LocalKeyedMutex();
        }

        return $this->mutex;
    }

    /** @inheritdoc */
    public function get(string $key): Promise
    {
        return call(function () use ($key) {
            $filename = $this->getFilename($key);

            /** @var Lock $lock */
            $lock = yield $this->getMutex()->acquire($filename);
            \assert($lock instanceof Lock);

            try {
                $cacheContent = yield File\read($this->directory . '/' . $filename);

                if (\strlen($cacheContent) < 4) {
                    return null;
                }

                $ttl = \unpack('Nttl', \substr($cacheContent, 0, 4))['ttl'];
                if ($ttl < \time()) {
                    yield File\deleteFile($this->directory . '/' . $filename);
                    return null;
                }

                $value = \substr($cacheContent, 4);

                \assert(\is_string($value));

                return $value;
            } catch (\Throwable $e) {
                return null;
            } finally {
                $lock->release();
            }
        });
    }

    /** @inheritdoc */
    public function set(string $key, $value, int $ttl = null): Promise
    {
        if ($value === null) {
            throw new CacheException('Cannot store NULL in ' . self::class);
        }

        if ($ttl < 0) {
            throw new \Error("Invalid cache TTL ({$ttl}); integer >= 0 or null required");
        }

        return call(function () use ($key, $value, $ttl) {
            $filename = $this->getFilename($key);

            /** @var Lock $lock */
            $lock = yield $this->getMutex()->acquire($filename);
            \assert($lock instanceof Lock);

            if ($ttl === null) {
                $ttl = \PHP_INT_MAX;
            } else {
                $ttl = \time() + $ttl;
            }

            $encodedTtl = \pack('N', $ttl);

            try {
                yield File\write($this->directory . '/' . $filename, $encodedTtl . $value);
            } finally {
                $lock->release();
            }
        });
    }

    /** @inheritdoc */
    public function delete(string $key): Promise
    {
        return call(function () use ($key) {
            $filename = $this->getFilename($key);

            /** @var Lock $lock */
            $lock = yield $this->getMutex()->acquire($filename);
            \assert($lock instanceof Lock);

            try {
                return yield File\deleteFile($this->directory . '/' . $filename);
            } finally {
                $lock->release();
            }
        });
    }
}
