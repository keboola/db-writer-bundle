<?php

namespace Keboola\DbWriterBundle\Tests\Controller;

use Keboola\DbWriterBundle\Test\AbstractTest;
use Symfony\Component\HttpFoundation\Response;

class DbWriterDriverControllerTest extends AbstractTest
{

    public function setUp()
    {
        parent::setUp('mysql');
    }

    protected function mockSapiClientWithDriver()
    {
        $indexActionResult = array (
            "components" => [
                0 =>
                    array (
                        'id' => 'wr-db',
                        'type' => 'writer',
                        'name' => 'Database',
                        'description' => 'Write data to MySQL or Oracle',
                        'hasUI' => true,
                        'hasRun' => true,
                        'ico32' => '',
                        'ico64' => '',
                        'data' =>
                            array (
                            ),
                        'uri' => 'https://syrup.keboola.com/wr-db',
                        'configurations' => []
                    ),
                1 =>
                    array (
                        'id' => 'wr-db-mysql',
                        'type' => 'writer',
                        'name' => 'Database',
                        'description' => 'Write data to MySQL ',
                        'hasUI' => true,
                        'hasRun' => true,
                        'ico32' => '',
                        'ico64' => '',
                        'data' =>
                            array (
                            ),
                        'uri' => 'https://syrup.keboola.com/wr-db/mysql',
                        'configurations' => []
                    ),
            ]
        );
        $sapiStub = $this->getMockBuilder("\\Keboola\\StorageApi\\Client")
            ->disableOriginalConstructor()
            ->setMethods(["indexAction"])
            ->getMock();
        $sapiStub->expects($this->once())
            ->method("indexAction")
            ->withAnyParameters()
            ->will($this->returnValue($indexActionResult));

        $serviceMock = new \Keboola\DbWriterBundle\Test\Mock\StorageApiService();
        $serviceMock->setStorageApiStub($sapiStub);
        static::$kernel->getContainer()->set("syrup.storage_api", $serviceMock);
    }

    public function testPostWriterDriverAction()
    {
        $this->mockSapiClientWithDriver();
        self::$client->request(
            'POST', $this->componentName . '/mysql/configs',
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

    public function testGetWritersNonExistingDriverAction()
    {
        $this->createWriter();

        self::$client->request('GET', $this->componentName . '/aabb/configs');

        $responseJson = self::$client->getResponse()->getContent();
        $response = json_decode($responseJson, true);
        $this->assertEquals($response["message"], "User error: Driver 'aabb' not found.");
    }

    public function testGetWritersDriverAction()
    {
        $this->mockSapiClientWithDriver();
        $this->createWriter();

        self::$client->request('GET', $this->componentName . '/mysql/configs');

        $responseJson = self::$client->getResponse()->getContent();
        $response = json_decode($responseJson, true);

        $this->assertEquals('test', $response[0]['id']);
        $this->assertEquals('test', $response[0]['name']);
    }

    public function testGetWriterDriverAction()
    {
        $this->mockSapiClientWithDriver();
        $this->createWriter();

        self::$client->request('GET', $this->componentName . '/mysql/configs/' . $this->writerId);

        $responseJson = self::$client->getResponse()->getContent();
        $response = json_decode($responseJson, true);

        $this->assertEquals('test', $response['id']);
        $this->assertEquals('test', $response['name']);
    }

    public function testDeleteWritersDriverAction()
    {
        $this->mockSapiClientWithDriver();
        $this->createWriter();

        self::$client->request('DELETE', $this->componentName . '/mysql/configs/' . $this->writerId);

        /* @var Response $response */
        $response = self::$client->getResponse();

        $writers = $this->configuration->getWriters();

        $this->assertEquals(204, $response->getStatusCode());
        $this->assertEmpty($writers);
    }

    /** Credentials */

    public function testPostCredentialsDriverAction()
    {
        $this->mockSapiClientWithDriver();
        $this->createWriter();

        $testing = $this->container->getParameter('testing');

        self::$client->request(
            'POST', $this->componentName . '/mysql/' . $this->writerId . '/credentials',
            [],
            [],
            [],
            json_encode($testing['mysql']['db'])
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

    public function testGetCredentialsDriverAction()
    {
        $this->mockSapiClientWithDriver();
        $this->createWriter();
        $testing = $this->container->getParameter('testing');
        $this->configuration->setCredentials($this->writerId, $testing['mysql']['db']);

        self::$client->request('GET', $this->componentName . '/mysql/' . $this->writerId . '/credentials');

        $responseJson = self::$client->getResponse()->getContent();
        $credentials = json_decode($responseJson, true);

        $this->assertArrayHasKey('host', $credentials);
        $this->assertArrayHasKey('port', $credentials);
        $this->assertArrayHasKey('database', $credentials);
        $this->assertArrayHasKey('user', $credentials);
        $this->assertArrayHasKey('password', $credentials);
        $this->assertArrayHasKey('allowedTypes', $credentials);

        $this->assertNotEmpty($credentials['host']);
        $this->assertNotEmpty($credentials['port']);
        $this->assertNotEmpty($credentials['database']);
        $this->assertNotEmpty($credentials['user']);
        $this->assertNotEmpty($credentials['password']);
        $this->assertNotEmpty($credentials['allowedTypes']);
    }

    /** Tables */

    public function testPostTableDriverAction()
    {
        $this->mockSapiClientWithDriver();
        $this->createWriter();
        $testing = $this->container->getParameter('testing');

        self::$client->request(
            'POST', $this->componentName . '/mysql/' . $this->writerId . '/tables/' . $testing['table']['id'],
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

    public function testGetTablesDriverAction()
    {
        $this->mockSapiClientWithDriver();
        $this->createWriter();
        $testing = $this->container->getParameter('testing');
        $this->configuration->updateTable($this->writerId, $testing['table']['id'], $testing['table']);

        self::$client->request(
            'GET',
            $this->componentName . '/mysql/' . $this->writerId . '/tables/' . $testing['table']['id']
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

    public function testPostColumnsDriverAction()
    {
        $this->mockSapiClientWithDriver();
        $this->createWriter();
        $testing = $this->container->getParameter('testing');
        $this->configuration->updateTable($this->writerId, $testing['table']['id'], $testing['table']);

        self::$client->request(
            'POST',
            $this->componentName . '/mysql/' . $this->writerId . '/tables/' . $testing['table']['id'] . '/columns',
            [],
            [],
            [],
            json_encode($testing['columns'])
        );

        $this->assertEquals(200, self::$client->getResponse()->getStatusCode());
    }

}
