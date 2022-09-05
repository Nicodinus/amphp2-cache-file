<?php

namespace Amp\Cache\Internal;

use Amp\ByteStream\InputStream;
use Amp\Promise;
use function Amp\call;

/**
 * @internal
 */
class PromiseReaderStream implements InputStream
{
    /** @var Promise<InputStream|null>|null */
    private ?Promise $promise;

    /** @var InputStream|null */
    private ?InputStream $stream;

    //

    /**
     * @param Promise<InputStream|null> $promise
     */
    public function __construct(Promise $promise)
    {
        $this->promise = $promise;
        $this->stream = null;
    }

    /**
     * @inheritDoc
     */
    public function read(): Promise
    {
        return call(function () {
            if ($this->stream === null) {
                if ($this->promise !== null) {
                    $this->stream = yield $this->promise;
                    $this->promise = null;
                }
            }

            if ($this->stream === null) {
                return null;
            }

            try {
                $chunk = yield $this->stream->read();
                if ($chunk === null) {
                    $this->stream = null;
                }

                return $chunk;
            } catch (\Throwable $exception) {
                $this->stream = null;
                throw $exception;
            }
        });
    }
}
