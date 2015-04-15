<?php

namespace Keboola\DbWriterBundle\Tests\Controller;

use Keboola\DbWriterBundle\Test\AbstractTest;
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

    public function testDeleteConfig()
    {
        $this->createWriter();

        self::$client->request('DELETE', $this->componentName . '/configs/' . $this->writerId);

        /* @var Response $response */
        $response = self::$client->getResponse();

        $writers = $this->configuration->getWriters();

        $this->assertEquals(204, $response->getStatusCode());
        $this->assertEmpty($writers);
    }
}
