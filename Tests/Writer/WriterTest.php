<?php
/**
 * Created by Miroslav Čillík <miro@keboola.com>
 * Date: 15/04/15
 * Time: 17:03
 */

namespace Keboola\DbWriterBundle\Tests\Writer;

use Keboola\DbWriterBundle\Test\AbstractTest;
use Keboola\DbWriterBundle\Writer\Writer;

class WriterTest extends AbstractTest
{
    public function testRun()
    {
        $this->createWriter();
        $testing = $this->container->getParameter('testing');
        $this->configuration->setCredentials($this->writerId, $testing['db']);
        $this->configuration->updateTable($this->writerId, $testing['table']['id'], $testing['table']);
        $this->configuration->updateTableColumns($this->writerId, $testing['table']['id'], $testing['columns']);

        $writer = new Writer(
            $this->configuration,
            $this->container->get('logger'),
            $this->container->get('temp')
        );

        $writer->run([
            'config' => 'test'
        ]);
    }
}
