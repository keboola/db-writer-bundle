<?php
/**
 * Created by Miroslav Čillík <miro@keboola.com>
 * Date: 05/09/14
 * Time: 12:52
 */

namespace Keboola\DbWriterBundle\Job;

use Keboola\DbWriterBundle\Writer\ConfigurationFactory;
use Keboola\DbWriterBundle\Writer\Writer;
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

	public function __construct(ConfigurationFactory $configurationFactory, Logger $logger, Temp $temp)
	{
		$this->configurationFactory = $configurationFactory;
		$this->logger = $logger;
		$this->temp = $temp;
	}

	public function execute(Job $job)
	{
		$configuration = $this->configurationFactory->get($this->storageApi);
		$writer = new Writer($configuration, $this->logger, $this->temp);
		return $writer->run($job->getParams());
	}
}
