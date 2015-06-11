<?php
/**
 * Created by Miroslav Čillík <miro@keboola.com>
 * Date: 05/09/14
 * Time: 12:53
 */

namespace Keboola\DbWriterBundle\Writer;

use Keboola\StorageApi\Client as SapiClient;
use Keboola\Syrup\Exception\ApplicationException;
use Keboola\Temp\Temp;
use Monolog\Logger;
use Keboola\Syrup\Exception\UserException;

abstract class Writer implements WriterInterface
{
	/** @var Configuration */
	protected $configuration;

	/** @var Logger */
	protected $logger;

	/** @var Temp */
	protected $temp;

	/** @var SapiClient */
	protected $storageApi;

	protected $db;

	public function __construct($dbParams, Logger $logger)
	{
		$this->logger = $logger;

        try {
            $this->db = $this->createConnection($dbParams);
        } catch (\Exception $e) {
            if (strstr(strtolower($e->getMessage()), 'could not find driver')) {
                throw new ApplicationException("Missing driver");
            }
            throw new UserException("Error connecting to DB: " . $e->getMessage(), $e);
        }
	}

	public function getConnection()
	{
		return $this->db;
	}
}
