<?php
/**
 * Created by Miroslav Čillík <miro@keboola.com>
 * Date: 15/04/15
 * Time: 17:03
 */

namespace Keboola\DbWriterBundle\Tests\Writer;

use Keboola\DbWriterBundle\Job\Executor;
use Keboola\DbWriterBundle\Test\AbstractTest;
use Keboola\Provisioning\Client;
use Keboola\StorageApi\Table;
use Keboola\Syrup\Encryption\Encryptor;
use Keboola\Syrup\Job\Metadata\Job;

class RedshiftWriterTest extends AbstractTest
{
    /**
     * @var Client Provisioning client
     */
    protected $provisioningClient;

    /**
     * @var
     */
    protected $credentials;

    protected function setUp()
    {
        parent::setUp("redshift");
        $token = $this->container->getParameter("storage_api.test.token");
        $this->provisioningClient = new Client("redshift", $token, 'testing');
        $this->credentials = $this->provisioningClient->getCredentials("sandbox")["credentials"];
    }

    protected function tearDown()
    {
        $credentials = $this->provisioningClient->getCredentials("sandbox");
        $this->provisioningClient->dropCredentials($credentials["credentials"]["id"]);
        parent::tearDown();
    }

    public function testRunRedshift()
    {
        $writerData = $this->prepareConfig();
        $this->write($writerData['id'], "redshift");
        $dsn = "pgsql:host={$this->credentials['hostname']};port=5439;dbname={$this->credentials["db"]}";

        // convert errors to PDOExceptions
        $options = [
            \PDO::ATTR_ERRMODE => \PDO::ERRMODE_EXCEPTION
        ];

        $pdo = new \PDO($dsn, $this->credentials["user"], $this->credentials["password"], $options);
        $tables = $pdo->query("SELECT tablename FROM PG_TABLES WHERE schemaname = '{$this->credentials["schema"]}';")
            ->fetchAll();

        $this->assertEquals(
            [
                0 =>
                    [
                        'tablename' => 'dummy',
                        0 => 'dummy',
                    ],
            ],
            $tables);
        $rows = $pdo->query("SELECT COUNT(*) AS rows FROM dummy;")
            ->fetchColumn(0);
        $this->assertEquals(4, $rows);
    }

    protected function prepareConfig()
    {
        $writerData = $this->createWriter();
        $provisioned = $this->provisioningClient->getCredentials("sandbox");
        $credentials = [
            "driver" => "redshift",
            "host" => $provisioned["credentials"]["hostname"],
            "database" => $provisioned["credentials"]["db"],
            "schema" => $provisioned["credentials"]["schema"],
            "user" => $provisioned["credentials"]["user"],
            "password" => $provisioned["credentials"]["password"]
        ];

        $testing = $this->container->getParameter('testing');
        $this->configuration->setCredentials($this->writerId, $credentials);
        $this->configuration->updateTable($this->writerId, $testing['table']['id'], $testing['table']);
        $this->configuration->updateTableColumns($this->writerId, $testing['table']['id'], $testing['columns']);

        return $writerData;
    }

    protected function write($writerId, $driver)
    {
        $tokenData = $this->storageApi->verifyToken();

        /** @var Encryptor $encryptor */
        $encryptor = $this->container->get('syrup.encryptor');

        /** @var Executor $executor */
        $executor = new Executor(
            $this->componentName . "-" . $driver,
            $this->container->get('wr_db.writer_factory'),
            $this->container->get("logger"),
            $this->container->get("syrup.temp"));

        $executor->setStorageApi($this->storageApi);

        $executor->execute(
            new Job(
                [
                    'id' => $this->storageApi->generateId(),
                    'runId' => $this->storageApi->generateId(),
                    'project' => [
                        'id' => $tokenData['owner']['id'],
                        'name' => $tokenData['owner']['name']
                    ],
                    'token' => [
                        'id' => $tokenData['id'],
                        'description' => $tokenData['description'],
                        'token' => $encryptor->encrypt($this->storageApi->getTokenString())
                    ],
                    'component' => $this->componentName . "-" . $driver,
                    'command' => 'run',
                    'params' => [
                        'writer' => $writerId
                    ],
                    'process' => [
                        'host' => gethostname(),
                        'pid' => getmypid()
                    ],
                    'createdTime' => date('c')
                ]));
    }
}
