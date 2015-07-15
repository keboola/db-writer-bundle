<?php

namespace Keboola\DbWriterBundle\Test\Mock;
class StorageApiService extends \Keboola\Syrup\Service\StorageApi\StorageApiService
{
    /**
     * @var \PHPUnit_Framework_Comparator_MockObject
     */
    private $stub;

    public function getClient()
    {
        if ($this->client == null) {
            if ($this->request == null) {
                throw new NoRequestException();
            }

            if (!$this->request->headers->has('X-StorageApi-Token')) {
                throw new UserException('Missing StorageAPI token');
            }

            if ($this->request->headers->has('X-StorageApi-Url')) {
                $this->storageApiUrl = $this->request->headers->get('X-StorageApi-Url');
            }

            $this->stub->__construct(
                [
                    'token' => $this->request->headers->get('X-StorageApi-Token'),
                    'url' => $this->storageApiUrl,
                    'userAgent' => explode('/', $this->request->getPathInfo())[1],
                ]);

            $this->client = $this->stub;

            if ($this->request->headers->has('X-KBC-RunId')) {
                $kbcRunId = $this->client->generateRunId($this->request->headers->get('X-KBC-RunId'));
            } else {
                $kbcRunId = $this->client->generateRunId();
            }

            $this->client->setRunId($kbcRunId);
        }

        return $this->client;
    }

    public function setStorageApiStub($stub)
    {
        $this->stub = $stub;

    }
}
