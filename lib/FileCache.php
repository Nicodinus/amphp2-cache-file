<?php

namespace Amp\Cache;

use Amp\ByteStream;
use Amp\Failure;
use Amp\File;
use Amp\Iterator;
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

    const DATA_TYPE_STRING = 0x00;
    const DATA_TYPE_STREAM = 0x01;

    //

    /** @var string */
    private string $directory;

    /** @var File\Filesystem */
    private File\Filesystem $filesystem;

    /** @var KeyedMutex|null */
    private ?KeyedMutex $mutex;

    /** @var string|null */
    private ?string $gcWatcher;

    //

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
     * @inheritDoc
     */
    public function exist(string $key): Promise
    {
        return call(function () use (&$key) {
            $filename = $this->getFilename($key);
            $path = $this->directory . DIRECTORY_SEPARATOR . $filename;

            /** @var Lock $lock */
            $lock = yield $this->getMutex()->acquire($filename);

            try {
                if (false === yield $this->filesystem->isFile($path)) {
                    return false;
                }

                /** @var File\File|null $fh */
                $fh = yield $this->openCacheFileAtPath($path);
                if (!$fh) {
                    return false;
                }

                $fh->close();
                return true;
            } finally {
                $lock->release();
            }
        });
    }

    /**
     * @inheritDoc
     */
    public function get(string $key): Promise
    {
        return $this->_get($key);
    }

    /**
     * @inheritDoc
     */
    public function set(string $key, $value, int $ttl = null): Promise
    {
        if ($value === null) {
            throw new CacheException('Cannot store NULL in ' . self::class);
        }

        return $this->_set($key, $value, $ttl);
    }

    /**
     * @inheritDoc
     */
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

    //

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
     * @param ByteStream\InputStream $stream
     *
     * @return Promise<string|null>
     */
    protected function buffer(ByteStream\InputStream $stream): Promise
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

    /**
     * @param string $key
     *
     * @return Promise<mixed|null>
     *
     * @see FileCache::get()
     */
    protected function _get(string $key): Promise
    {
        return call(function () use (&$key) {
            $filename = $this->getFilename($key);
            $path = $this->directory . DIRECTORY_SEPARATOR . $filename;

            /** @var Lock $lock */
            $lock = yield $this->getMutex()->acquire($filename);
            $canReleaseLock = true;

            /** @var File\File|null $fh */
            $fh = null;
            $canCloseFile = true;

            try {
                if (false === yield $this->filesystem->isFile($path)) {
                    return null;
                }

                $fh = yield $this->openCacheFileAtPath($path);
                if (!$fh) {
                    return null;
                }

                $dataType = yield $fh->read(1);
                if ($dataType === null) {
                    throw new CacheException("Invalid cache file (data_type empty) at `{$path}`");
                }
                $dataType = \unpack('C', $dataType)[1];

                if ($dataType === self::DATA_TYPE_STREAM) {
                    $chunk = yield $fh->read();
                    if ($chunk === null) {
                        throw new CacheException("Invalid cache file (data empty) at `{$path}`");
                    }

                    $canReleaseLock = false;
                    $canCloseFile = false;

                    $reader = new class implements ByteStream\InputStream {
                        /** @var File\File|null */
                        public ?File\File $fh = null;

                        /** @var Lock|null */
                        public ?Lock $lock = null;

                        /** @var string|null */
                        public ?string $append = null;

                        //

                        /**
                         * @inheritDoc
                         */
                        public function read(): Promise
                        {
                            return call(function () {
                                if (!$this->fh) {
                                    return null;
                                }

                                if ($this->append) {
                                    $data = $this->append;
                                    $this->append = null;
                                    return $data;
                                }

                                $data = yield $this->fh->read();
                                if ($data === null) {
                                    $this->fh->close();
                                    $this->fh = null;

                                    if ($this->lock) {
                                        $this->lock->release();
                                        $this->lock = null;
                                    }
                                }

                                return $data;
                            });
                        }
                    };

                    $reader->fh = &$fh;
                    $reader->lock = &$lock;
                    $reader->append = $chunk;

                    return $reader;
                }

                $result = yield $this->buffer($fh);
                if ($result === null) {
                    throw new CacheException("Invalid cache file (data empty) at `{$path}`");
                }
                return $result;
            } catch (\Throwable $exception) {
                if ($fh && $canCloseFile) {
                    yield $fh->close();
                    $fh = null;
                }

                if (true === yield $this->filesystem->isFile($path)) {
                    yield $this->filesystem->deleteFile($path);
                }

                return null;
            } finally {
                /** @psalm-suppress RedundantCondition */
                if ($lock && $canReleaseLock) {
                    $lock->release();
                    $lock = null;
                }

                /** @psalm-suppress RedundantCondition */
                if ($fh && $canCloseFile) {
                    yield $fh->close();
                    $fh = null;
                }
            }
        });
    }

    /**
     * @param string $key
     * @param mixed $value
     * @param int|null $ttl
     *
     * @return Promise<void>
     *
     * @see FileCache::set()
     */
    protected function _set(string $key, $value, int $ttl = null): Promise
    {
        if ($ttl < 0) {
            $ttl = null;
        }

        return call(function () use (&$key, &$value, &$ttl) {
            $filename = $this->getFilename($key);
            $path = $this->directory . DIRECTORY_SEPARATOR . $filename;

            /** @var Lock $lock */
            $lock = yield $this->getMutex()->acquire($filename);

            try {
                if (false === yield $this->filesystem->isDirectory($this->directory)) {
                    yield $this->filesystem->createDirectoryRecursively($this->directory, 0700);
                }

                if ($ttl === null) {
                    $expiredAtRawString = \pack('C', self::TTL_TYPE_NONE);
                } else {
                    $ttl = \hrtime(true) + $ttl * 1e+9;
                    if ($ttl <= 0xFF) {
                        $expiredAtRawString = \pack('CC', self::TTL_TYPE_UINT8_ME, $ttl);
                    } elseif ($ttl > 0xFF && $ttl <= 0xFFFF) {
                        $expiredAtRawString = \pack('CS', self::TTL_TYPE_UINT16_ME, $ttl);
                    } elseif ($ttl > 0xFFFF && $ttl <= 0xFFFFFFFF) {
                        $expiredAtRawString = \pack('CL', self::TTL_TYPE_UINT32_ME, $ttl);
                    } else {
                        $expiredAtRawString = \pack('CQ', self::TTL_TYPE_UINT64_ME, $ttl);
                    }
                }

                $checkSignature = $this->getFileSignature();
                $checkSignatureLength = $checkSignature !== null ? \strlen($checkSignature) : 0;
                if ($checkSignatureLength > 0) {
                    $expiredAtRawString = $this->getFileSignature() . $expiredAtRawString;
                }

                $dataType = self::DATA_TYPE_STRING;
                if ($value instanceof Iterator) {
                    $dataType = self::DATA_TYPE_STREAM;
                } elseif ($value instanceof ByteStream\InputStream) {
                    $dataType = self::DATA_TYPE_STREAM;
                }
                $dataTypeRawString = \pack('C', $dataType);

                /** @var File\File $fh */
                $fh = yield $this->filesystem->openFile($path, 'wb');

                try {
                    yield $fh->write($expiredAtRawString . $dataTypeRawString);

                    if ($value instanceof Iterator) {
                        while (true === yield $value->advance()) {
                            yield $fh->write($value->getCurrent());
                        }
                    } elseif ($value instanceof ByteStream\InputStream) {
                        while (($chunk = yield $value->read()) !== null) {
                            yield $fh->write($chunk);
                        }
                    } else {
                        yield $fh->write($value);
                    }
                } finally {
                    $fh->close();
                }
            } finally {
                $lock->release();
            }
        });
    }
}
