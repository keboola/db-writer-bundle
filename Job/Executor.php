<?php
/**
 * Created by Miroslav Čillík <miro@keboola.com>
 * Date: 05/09/14
 * Time: 12:52
 */

namespace Keboola\DbWriterBundle\Job;

use Keboola\DbWriterBundle\Writer\ConfigurationFactory;
use Keboola\DbWriterBundle\Writer\Writer;
use Keboola\DbWriterBundle\Writer\WriterFactory;
use Keboola\DbWriterBundle\Writer\WriterInterface;
use Keboola\StorageApi\ClientException;
use Keboola\Syrup\Exception\UserException;
use Keboola\Temp\Temp;
use Monolog\Logger;
use Keboola\Syrup\Job\Executor as BaseExecutor;
use Keboola\Syrup\Job\Metadata\Job;

class Executor extends BaseExecutor
{
	/** @var ConfigurationFactory  */
	protected $configurationFactory;

	/** @var Logger */
	protected $logger;

	/** @var Temp */
	protected $temp;

	public function __construct(ConfigurationFactory $configurationFactory, WriterFactory $writerFactory, Logger $logger, Temp $temp)
	{
		$this->configurationFactory = $configurationFactory;
		$this->logger = $logger;
		$this->temp = $temp;
        $this->writerFactory = $writerFactory;
	}

	public function execute(Job $job)
	{
        $options = $job->getParams();

        $writerId = null;
        if (isset($options['config'])) {
            $writerId = $options['config'];
        } else if (isset($options['writer'])) {
            $writerId = $options['writer'];
        }

        if ($writerId == null) {
            throw new UserException('Parameter "config" or "writer" must be specified');
        }

        $configuration = $this->configurationFactory->get($this->storageApi);
        $writerConfig = $configuration->getSysBucket($writerId);
        $tables = $configuration->getSysTables($writerId);

        if (!isset($writerConfig['db'])) {
            throw new UserException('Missing DB credentials');
        }

        /** @var WriterInterface $writer */
		$writer = $this->writerFactory->get($writerConfig['db']);

        if (isset($params['table'])) {
            $sysTableName = $configuration->getWriterTableName($params['table']);
            if (isset($tables[$sysTableName])) {
                $tables = [
                    $tables[$sysTableName]
                ];
            }
        }

        $uploaded = [];
        foreach ($tables as $table) {

            if (!$writer->isTableValid($table)) {
                //@todo: create warning event in SAPI
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

            $sourceFilename = $this->temp->createTmpFile(null, true);

            try {
                $this->storageApi->exportTable($sourceTableId, $sourceFilename, [
                    'columns' => $colNames
                ]);
            } catch (ClientException $e) {
                throw new UserException("Error exporting table from StorageAPI", $e, [
                    'message' => $e->getMessage()
                ]);
            }

            $writer->drop($outputTableName);
            $writer->create($table);
            $writer->write($sourceFilename, $outputTableName, $table);

            $uploaded[] = $sourceTableId;
        }

        return [
            'status'    => 'ok',
            'uploaded'  => $uploaded
        ];
	}
}
