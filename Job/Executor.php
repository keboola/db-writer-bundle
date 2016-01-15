<?php
/**
 * Created by Miroslav Čillík <miro@keboola.com>
 * Date: 05/09/14
 * Time: 12:52
 */

namespace Keboola\DbWriterBundle\Job;

use Keboola\DbWriterBundle\Monolog\Processor\DbWriterProcessor;
use Keboola\DbWriterBundle\Writer\Configuration;
use Keboola\DbWriterBundle\Writer\WriterFactory;
use Keboola\DbWriterBundle\Writer\WriterInterface;
use Keboola\StorageApi\ClientException;
use Keboola\StorageApi\Options\GetFileOptions;
use Keboola\Syrup\Exception\UserException;
use Keboola\Temp\Temp;
use Monolog\Logger;
use Keboola\Syrup\Job\Executor as BaseExecutor;
use Keboola\Syrup\Job\Metadata\Job;

class Executor extends BaseExecutor
{
    protected $componentName;

    /** @var Logger */
    protected $logger;

    /** @var Temp */
    protected $temp;

    public function __construct($componentName, WriterFactory $writerFactory, Logger $logger, Temp $temp)
    {
        $this->componentName = $componentName;
        $this->logger = $logger;
        $this->temp = $temp;
        $this->writerFactory = $writerFactory;
    }

    /**
     * @param $id
     */
    protected function getComponent($id)
    {
        // Check list of components
        $components = $this->storageApi->indexAction();
        foreach ($components["components"] as $c) {
            if ($c["id"] == $id) {
                $component = $c;
            }
        }
        if (!isset($component)) {
            throw new UserException("Component '{$id}' not found.");
        }

        return $component;
    }


    public function execute(Job $job)
    {
        $options = $job->getParams();
        $driver = 'generic';

        if (isset($options['component'])) {
            $component = $this->getComponent($options['component']);
            $processor = new DbWriterProcessor($component['id']);
            $this->logger->pushProcessor([$processor, 'processRecord']);

            // get driver from componentName
            if ($options['component'] != $this->componentName) {
                $driver = str_replace($this->componentName . '-', '', $options['component']);
            }
            // replace componentName
            $this->componentName = $options['component'];
        }

        $writerId = null;
        if (isset($options['config'])) {
            $writerId = $options['config'];
        } else {
            if (isset($options['writer'])) {
                $writerId = $options['writer'];
            }
        }

        if ($writerId == null) {
            throw new UserException("Parameter 'config' or 'writer' must be specified");
        }

        $configuration = new Configuration($this->componentName, $this->storageApi, $driver);
        $writerConfig = $configuration->getSysBucket($writerId);
        $tables = $configuration->getSysTables($writerId);

        if (!isset($writerConfig['db'])) {
            throw new UserException('Missing DB credentials');
        }

        /** @var WriterInterface $writer */
        $writer = $this->writerFactory->get($writerConfig['db']);

        if (isset($options['table'])) {
            $sysTableName = $configuration->getWriterTableName($options['table']);

            if (!isset($tables[$sysTableName])) {
                throw new UserException(sprintf("Table '%s' not found", $sysTableName));
            }

            $tables = [
                $tables[$sysTableName]
            ];
        }

        $uploaded = [];
        foreach ($tables as $table) {
            $ignoreExport = false;
            if (isset($options['table'])) {
                $ignoreExport = true;
            }
            if (!$writer->isTableValid($table, $ignoreExport)) {
                $this->logger->warning(sprintf("Table '%s' not exported", $table["tableId"]));
                continue;
            }

            $sourceTableId = $table['tableId'];
            $outputTableName = $table['dbName'];

            $colNames = [];
            foreach ($table['items'] as $item) {
                if ($item['type'] != 'IGNORE') {
                    $colNames[] = $item['name'];
                }
            }

            if ($writer->isAsync()) {
                try {
                    $job = $this->storageApi->exportTableAsync(
                        $sourceTableId,
                        [
                            'columns' => $colNames,
                            'gzip' => true
                        ]);
                    $fileInfo = $this->storageApi->getFile(
                        $job["file"]["id"],
                        (new GetFileOptions())->setFederationToken(true));
                } catch (ClientException $e) {
                    throw new UserException(
                        "Error exporting table from StorageAPI", $e, [
                        'message' => $e->getMessage()
                    ]);
                }
                $writer->drop($outputTableName);
                $writer->create($table);
                $writer->writeAsync($fileInfo, $outputTableName);

            } else {

                $sourceFilename = $this->temp->createTmpFile(null, true);
                try {
                    $this->storageApi->exportTable(
                        $sourceTableId,
                        $sourceFilename,
                        [
                            'columns' => $colNames
                        ]);
                } catch (ClientException $e) {
                    throw new UserException(
                        "Error exporting table from StorageAPI", $e, [
                        'message' => $e->getMessage()
                    ]);
                }

                $writer->drop($outputTableName);
                $writer->create($table);
                $writer->write($sourceFilename, $outputTableName, $table);
            }

            $uploaded[] = $sourceTableId;
        }

        return [
            'status' => 'ok',
            'uploaded' => $uploaded
        ];
    }
}
