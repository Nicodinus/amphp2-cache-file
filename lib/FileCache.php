<?php

namespace Amp\Cache;

use Amp\ByteStream;
use Amp\Failure;
use Amp\File;
use Amp\Loop;
use Amp\Promise;
use Amp\Sync\KeyedMutex;
use Amp\Sync\LocalKeyedMutex;
use Amp\Sync\Lock;
use function Amp\call;

class FileCache implements Cache
{
    const TTL_TYPE_NONE = 0x00;
    const TTL_TYPE_UINT8_ME = 0x01;
    const TTL_TYPE_UINT16_ME = 0x02;
    const TTL_TYPE_UINT32_ME = 0x03;
    const TTL_TYPE_UINT64_ME = 0x04;

    /** @var string */
    private string $directory;

    /** @var File\Filesystem */
    private File\Filesystem $filesystem;

    /** @var KeyedMutex|null */
    private ?KeyedMutex $mutex;

    /** @var string|null */
    private ?string $gcWatcher;

    /**
     * @param string $directory
     * @param KeyedMutex|null $mutex
     * @param File\Filesystem|null $filesystem
     */
    public function __construct(string $directory, KeyedMutex $mutex = null, File\Filesystem $filesystem = null)
    {
        $this->directory = \rtrim($directory, "/\\");
        $this->mutex = $mutex;
        $this->filesystem = $filesystem ?? File\filesystem();

        $gcWatcher = \Closure::fromCallable([$this, '_gcWatcherCallback']);

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

        $gcWatcher = \Closure::fromCallable([$this, '_gcWatcherCallback']);
        Loop::defer($gcWatcher);
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
     * @return string|null
     */
    protected function getFileSignature(): ?string
    {
        return 'amp2-cache-file';
    }

    /**
     * @return \Generator
     */
    protected function _gcWatcherCallback(): \Generator
    {
        try {
            $files = yield $this->filesystem->listFiles($this->directory);

            foreach ($files as $file) {
                if (\strlen($file) !== 70 || \substr($file, -\strlen('.cache')) !== '.cache') {
                    continue;
                }

                $path = $this->directory . DIRECTORY_SEPARATOR . $file;

                /** @var Lock $lock */
                $lock = yield $this->getMutex()->acquire($file);

                try {
                    /** @var File\File|null $fh */
                    $fh = yield $this->openCacheFileAtPath($path);
                    if (!$fh) {
                        continue;
                    }
                } catch (\Throwable $e) {
                    if (true === yield $this->filesystem->isFile($path)) {
                        yield $this->filesystem->deleteFile($path);
                    }
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

    /**
     * Returns Amp\File\File when cache file is valid and not expired. Deletes only if cache is expired.
     *
     * @param string $path
     *
     * @return Promise<File\File|null>|Failure<CacheException>
     */
    protected function openCacheFileAtPath(string $path): Promise
    {
        return call(function () use (&$path) {
            /** @var File\File $fh */
            $fh = yield $this->filesystem->openFile($path, 'rb');

            if ($fh->eof()) {
                yield $fh->close();
                throw new CacheException("Invalid cache file (empty) at `{$path}`");
            }

            $checkSignature = $this->getFileSignature();
            $checkSignatureLength = $checkSignature !== null ? \strlen($checkSignature) : 0;
            if ($checkSignatureLength > 0) {
                $signature = yield $fh->read($checkSignatureLength);
                if ($signature === null || $signature !== $checkSignature) {
                    yield $fh->close();
                    throw new CacheException("Invalid cache file (signature mismatch) at `{$path}`");
                }
            }

            $expiredAtType = yield $fh->read(1);
            if ($expiredAtType === null) {
                yield $fh->close();
                throw new CacheException("Invalid cache file (expired_at_type empty) at `{$path}`");
            }
            $expiredAtType = \unpack('C', $expiredAtType)[1];

            if ($expiredAtType === self::TTL_TYPE_NONE) {
                return $fh;
            }

            switch ($expiredAtType) {
                case self::TTL_TYPE_UINT8_ME:
                    $expiredAt = yield $fh->read(1);
                    if ($expiredAt === null) {
                        yield $fh->close();
                        throw new CacheException("Invalid cache file (expired_at empty) at `{$path}`");
                    }
                    $expiredAt = \unpack('C', $expiredAt)[1];
                    break;
                case self::TTL_TYPE_UINT16_ME:
                    $expiredAt = yield $fh->read(2);
                    if ($expiredAt === null) {
                        yield $fh->close();
                        throw new CacheException("Invalid cache file (expired_at empty) at `{$path}`");
                    }
                    $expiredAt = \unpack('S', $expiredAt)[1];
                    break;
                case self::TTL_TYPE_UINT32_ME:
                    $expiredAt = yield $fh->read(4);
                    if ($expiredAt === null) {
                        yield $fh->close();
                        throw new CacheException("Invalid cache file (expired_at empty) at `{$path}`");
                    }
                    $expiredAt = \unpack('L', $expiredAt)[1];
                    break;
                case self::TTL_TYPE_UINT64_ME:
                    $expiredAt = yield $fh->read(8);
                    if ($expiredAt === null) {
                        yield $fh->close();
                        throw new CacheException("Invalid cache file (expired_at empty) at `{$path}`");
                    }
                    $expiredAt = \unpack('Q', $expiredAt)[1];
                    break;
                default:
                    yield $fh->close();
                    throw new CacheException("Invalid expired_at type `{$expiredAtType}` at cache file `{$path}`");
            }

            if (\hrtime(true) > $expiredAt) {
                yield $fh->close();
                yield $this->filesystem->deleteFile($path);
                return null;
            }

            return $fh;
        });
    }

    /** @inheritdoc */
    public function get(string $key): Promise
    {
        return call(function () use ($key) {
            $filename = $this->getFilename($key);
            $path = $this->directory . DIRECTORY_SEPARATOR . $filename;

            if (false === yield $this->filesystem->isFile($path)) {
                return null;
            }

            /** @var Lock $lock */
            $lock = yield $this->getMutex()->acquire($filename);

            try {
                /** @var File\File|null $fh */
                $fh = yield $this->openCacheFileAtPath($path);
                if (!$fh) {
                    return null;
                }

                $buffer = yield ByteStream\buffer($fh);
                if (\strlen($buffer) < 1) {
                    yield $fh->close();
                    yield $this->filesystem->deleteFile($path);
                    return null;
                }
                return $buffer;
            } catch (\Throwable $exception) {
                if (true === yield $this->filesystem->isFile($path)) {
                    yield $this->filesystem->deleteFile($path);
                }
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

        if (!\is_string($value)) {
            throw new CacheException('Cannot store non-string value in ' . self::class);
        }

        if ($ttl < 0) {
            throw new \Error("Invalid cache TTL ({$ttl}); integer >= 0 or null required");
        }

        return call(function () use ($key, $value, $ttl) {
            $filename = $this->getFilename($key);
            $path = $this->directory . DIRECTORY_SEPARATOR . $filename;

            if (false === yield $this->filesystem->isDirectory($this->directory)) {
                yield $this->filesystem->createDirectoryRecursively($this->directory, 0700);
            }

            /** @var Lock $lock */
            $lock = yield $this->getMutex()->acquire($filename);

            if ($ttl === null) {
                $expiredAt = \pack('C', self::TTL_TYPE_NONE);
            } else {
                $ttl = \hrtime(true) + $ttl * 1e+9;
                if ($ttl <= 0xFF) {
                    $expiredAt = \pack('CC', self::TTL_TYPE_UINT8_ME, $ttl);
                } elseif ($ttl > 0xFF && $ttl <= 0xFFFF) {
                    $expiredAt = \pack('CS', self::TTL_TYPE_UINT16_ME, $ttl);
                } elseif ($ttl > 0xFFFF && $ttl <= 0xFFFFFFFF) {
                    $expiredAt = \pack('CL', self::TTL_TYPE_UINT32_ME, $ttl);
                } else {
                    $expiredAt = \pack('CQ', self::TTL_TYPE_UINT64_ME, $ttl);
                }
            }

            $checkSignature = $this->getFileSignature();
            $checkSignatureLength = $checkSignature !== null ? \strlen($checkSignature) : 0;
            if ($checkSignatureLength > 0) {
                $expiredAt = $this->getFileSignature() . $expiredAt;
            }

            try {
                yield $this->filesystem->write($path, $expiredAt . $value);
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
            $path = $this->directory . DIRECTORY_SEPARATOR . $filename;

            if (false === yield $this->filesystem->isFile($path)) {
                return null;
            }

            /** @var Lock $lock */
            $lock = yield $this->getMutex()->acquire($filename);

            try {
                return yield $this->filesystem->deleteFile($path);
            } finally {
                $lock->release();
            }
        });
    }
}
