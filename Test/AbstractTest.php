<?php
/**
 * Created by Miroslav Čillík <miro@keboola.com>
 * Date: 15/04/15
 * Time: 11:53
 */

namespace Keboola\DbWriterBundle\Test;

use Symfony\Bundle\FrameworkBundle\Test\WebTestCase;
use Keboola\DbWriterBundle\Writer\Configuration;
use Keboola\StorageApi\Client as SapiClient;
use Symfony\Bundle\FrameworkBundle\Client;
use Symfony\Component\DependencyInjection\ContainerInterface;

class AbstractTest extends WebTestCase
{
    /** @var SapiClient */
    protected $storageApi;

    /** @var Client */
    protected static $client;

    protected $componentName = 'wr-db';

    /** @var ContainerInterface */
    protected $container;

    /** @var Configuration */
    protected $configuration;

    protected $writerId = 'test';

    protected function setUp($driver = null)
    {
        self::$client = static::createClient();
        $this->container = self::$client->getContainer();

        $sapiToken = $this->container->getParameter('storage_api.test.token');
        $sapiUrl = $this->container->getParameter('storage_api.test.url');

        self::$client->setServerParameters(
            [
                'HTTP_X-StorageApi-Token' => $sapiToken
            ]);

        $this->storageApi = new SapiClient(
            [
                'token' => $sapiToken,
                'url' => $sapiUrl,
                'userAgent' => $this->componentName
            ]);

        if ($driver != null) {
            $this->configuration = new Configuration($this->componentName . '-' . $driver, $this->storageApi, $driver);
        } else {
            $this->configuration = new Configuration($this->componentName, $this->storageApi);
        }

        // Cleanup
        $sysBucketId = $this->configuration->getSysBucketId($this->writerId);
        if ($this->storageApi->bucketExists($sysBucketId)) {
            $accTables = $this->storageApi->listTables($sysBucketId);
            foreach ($accTables as $table) {
                $this->storageApi->dropTable($table['id']);
            }
            $this->storageApi->dropBucket($sysBucketId);
        }
    }

    protected function createWriter()
    {
        return $this->configuration->createWriter($this->writerId, 'Test Account created by PhpUnit');
    }

    protected function createCredentials($params)
    {
        $this->configuration->setCredentials($this->writerId, $params);
    }

    protected function createTable($tableId, $data)
    {
        $this->configuration->updateTable($this->writerId, $tableId, $data);
    }
}
