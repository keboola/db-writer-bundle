<?php
/**
 * Created by Miroslav Čillík <miro@keboola.com>
 * Date: 15/04/15
 * Time: 17:03
 */

namespace Keboola\DbWriterBundle\Tests\Writer;

use Keboola\DbWriterBundle\Job\Executor;
use Keboola\DbWriterBundle\Test\AbstractTest;
use Keboola\Syrup\Encryption\Encryptor;
use Keboola\Syrup\Job\Metadata\Job;

class WriterTest extends AbstractTest
{
    public function testRunMySQL()
    {
        $writerData = $this->prepareConfig('mysql');
        $this->write($writerData['id']);
    }

    public function testRunOracle()
    {
        $writerData = $this->prepareConfig('oracle');
        $testing = $this->container->getParameter('testing');
        $this->configuration->updateTable($this->writerId, $testing['table']['id'], [
            'dbName' => 'keboola.dummy'
        ]);
        $this->write($writerData['id']);

        $dbParams = $testing['oracle']['db'];
        $dbString = '//' . $dbParams['host'] . ':' . $dbParams['port'] . '/' . $dbParams['database'];
        $conn = oci_connect($dbParams['user'], $dbParams['password'], $dbString, 'AL32UTF8');

        $stid = oci_parse($conn, "SELECT * FROM keboola.dummy");
        oci_execute($stid);

        while ($res = oci_fetch_array($stid)) {
            var_dump($res);
        }
    }

    protected function prepareConfig($driver)
    {
        $writerData = $this->createWriter();
        $testing = $this->container->getParameter('testing');
        $this->configuration->setCredentials($this->writerId, $testing[$driver]['db']);
        $this->configuration->updateTable($this->writerId, $testing['table']['id'], $testing['table']);
        $this->configuration->updateTableColumns($this->writerId, $testing['table']['id'], $testing['columns']);

        return $writerData;
    }

    protected function write($writerId)
    {
        $tokenData = $this->storageApi->verifyToken();

        /** @var Encryptor $encryptor */
        $encryptor = $this->container->get('syrup.encryptor');

        /** @var Executor $executor */
        $executor = $this->container->get('wr_db.job_executor');

        $executor->setStorageApi($this->storageApi);

        $executor->execute(new Job([
            'id'          => $this->storageApi->generateId(),
            'runId'       => $this->storageApi->generateId(),
            'project'     => [
                'id'   => $tokenData['owner']['id'],
                'name' => $tokenData['owner']['name']
            ],
            'token'       => [
                'id'          => $tokenData['id'],
                'description' => $tokenData['description'],
                'token'       => $encryptor->encrypt($this->storageApi->getTokenString())
            ],
            'component'   => $this->componentName,
            'command'     => 'run',
            'params'      => [
                'writer' => $writerId
            ],
            'process'     => [
                'host' => gethostname(),
                'pid'  => getmypid()
            ],
            'createdTime' => date('c')
        ]));
    }
}
