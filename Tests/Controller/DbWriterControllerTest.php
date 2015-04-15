<?php

namespace Keboola\DbWriterBundle\Tests\Controller;

use Keboola\DbWriterBundle\Test\AbstractTest;
use Keboola\Syrup\Elasticsearch\JobMapper;
use Keboola\Syrup\Job\Metadata\Job;
use Symfony\Component\HttpFoundation\Response;

class DbWriterControllerTest extends AbstractTest
{
    public function testPostWriterAction()
    {
        self::$client->request(
            'POST', $this->componentName . '/configs',
            [],
            [],
            [],
            json_encode([
                'name' => $this->writerId,
                'description' => 'Test Account created by PhpUnit'
            ])
        );

        $responseJson = self::$client->getResponse()->getContent();
        $response = json_decode($responseJson, true);

        $this->assertEquals('test', $response['id']);
        $this->assertEquals('test', $response['name']);
    }

    public function testGetWritersAction()
    {
        $this->createWriter();

        self::$client->request('GET', $this->componentName . '/configs');

        $responseJson = self::$client->getResponse()->getContent();
        $response = json_decode($responseJson, true);

        $this->assertEquals('test', $response[0]['id']);
        $this->assertEquals('test', $response[0]['name']);

        self::$client->restart();
        self::$client->request('GET', $this->componentName . '/configs/' . $this->writerId);

        $responseJson = self::$client->getResponse()->getContent();
        $response = json_decode($responseJson, true);

        $this->assertEquals('test', $response['id']);
        $this->assertEquals('test', $response['name']);
    }

    public function testDeleteWritersAction()
    {
        $this->createWriter();

        self::$client->request('DELETE', $this->componentName . '/configs/' . $this->writerId);

        /* @var Response $response */
        $response = self::$client->getResponse();

        $writers = $this->configuration->getWriters();

        $this->assertEquals(204, $response->getStatusCode());
        $this->assertEmpty($writers);
    }

    /** Credentials */

    public function testPostCredentialsAction()
    {
        $this->createWriter();

        $testing = $this->container->getParameter('testing');

        self::$client->request(
            'POST', $this->componentName . '/' . $this->writerId . '/credentials',
            [],
            [],
            [],
            json_encode($testing['db'])
        );

        $responseJson = self::$client->getResponse()->getContent();
        $response = json_decode($responseJson, true);

        $this->assertEquals($this->writerId, $response['writerId']);

        $credentials = $this->configuration->getCredentials($this->writerId);

        $this->assertArrayHasKey('host', $credentials);
        $this->assertArrayHasKey('port', $credentials);
        $this->assertArrayHasKey('database', $credentials);
        $this->assertArrayHasKey('user', $credentials);
        $this->assertArrayHasKey('password', $credentials);

        $this->assertNotEmpty($credentials['host']);
        $this->assertNotEmpty($credentials['port']);
        $this->assertNotEmpty($credentials['database']);
        $this->assertNotEmpty($credentials['user']);
        $this->assertNotEmpty($credentials['password']);
    }

    public function testGetCredentialsAction()
    {
        $this->createWriter();
        $testing = $this->container->getParameter('testing');
        $this->configuration->setCredentials($this->writerId, $testing['db']);

        self::$client->request('GET', $this->componentName . '/' . $this->writerId . '/credentials');

        $responseJson = self::$client->getResponse()->getContent();
        $credentials = json_decode($responseJson, true);

        $this->assertArrayHasKey('host', $credentials);
        $this->assertArrayHasKey('port', $credentials);
        $this->assertArrayHasKey('database', $credentials);
        $this->assertArrayHasKey('user', $credentials);
        $this->assertArrayHasKey('password', $credentials);

        $this->assertNotEmpty($credentials['host']);
        $this->assertNotEmpty($credentials['port']);
        $this->assertNotEmpty($credentials['database']);
        $this->assertNotEmpty($credentials['user']);
        $this->assertNotEmpty($credentials['password']);
    }

    /** Tables */

    public function testPostTableAction()
    {
        $this->createWriter();
        $testing = $this->container->getParameter('testing');

        self::$client->request(
            'POST', $this->componentName . '/' . $this->writerId . '/tables/' . $testing['table']['id'],
            [],
            [],
            [],
            json_encode($testing['table'])
        );

        $responseJson = self::$client->getResponse()->getContent();
        $response = json_decode($responseJson, true);

        $this->assertEquals($this->writerId, $response['writerId']);

        $sysBucketId = $this->configuration->getSysBucketId($this->writerId);
        $tableId = $sysBucketId . '.' . str_replace('.', '_', $testing['table']['id']);

        $this->assertEquals($tableId, $response['tableId']);
    }

    public function testGetTablesAction()
    {
        $this->createWriter();
        $testing = $this->container->getParameter('testing');
        $this->configuration->updateTable($this->writerId, $testing['table']['id'], $testing['table']);

        self::$client->request(
            'GET',
            $this->componentName . '/' . $this->writerId . '/tables/' . $testing['table']['id']
        );

        $responseJson = self::$client->getResponse()->getContent();
        $response = json_decode($responseJson, true);

        $this->assertEquals($testing['table']['id'], $response['id']);
        $this->assertEquals($testing['table']['dbName'], $response['name']);
        $this->assertEquals($testing['table']['export'], $response['export']);
        $this->assertNotEmpty($response['lastChange']);
        $this->assertNotEmpty($response['columns']);


        foreach ($response['columns'] as $col) {
            $this->assertNotEmpty($col['name']);
            $this->assertNotEmpty($col['dbName']);
            $this->assertNotEmpty($col['type']);
            $this->assertEquals('IGNORE', $col['type']);
        }
    }

    /** Columns */

    public function testPostColumnsAction()
    {
        $this->createWriter();
        $testing = $this->container->getParameter('testing');
        $this->configuration->updateTable($this->writerId, $testing['table']['id'], $testing['table']);

        self::$client->request(
            'POST',
            $this->componentName . '/' . $this->writerId . '/tables/' . $testing['table']['id'] . '/columns',
            [],
            [],
            [],
            json_encode($testing['columns'])
        );

        $this->assertEquals(200, self::$client->getResponse()->getStatusCode());
    }

    /** Jobs */
    public function testGetJobsAction()
    {
        $this->createWriter();
        $testing = $this->container->getParameter('testing');
        $this->configuration->updateTable($this->writerId, $testing['table']['id'], $testing['table']);

        /** @var JobMapper $jobMapper */
        $jobMapper = $this->container->get('syrup.elasticsearch.current_component_job_mapper');
        $jobMapper->create($this->createJob('run', ['config' => 'test']));

        sleep(2);

        self::$client->request(
            'GET',
            $this->componentName . '/' . $this->writerId . '/jobs'
        );

        $responseJson = self::$client->getResponse()->getContent();
        $response = json_decode($responseJson, true);

        $this->assertNotEmpty($response);
        $this->assertEquals(200, self::$client->getResponse()->getStatusCode());
    }

    protected function createJob($command, $params)
    {
        $jobFactory = $this->container->get('syrup.job_factory');
        $jobFactory->setStorageApiClient($this->storageApi);
        return $jobFactory->create($command, $params);
    }
}
