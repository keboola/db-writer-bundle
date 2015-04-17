<?php
/**
 * Created by Miroslav Čillík <miro@keboola.com>
 * Date: 05/09/14
 * Time: 12:53
 */

namespace Keboola\DbWriterBundle\Writer;

use Doctrine\DBAL\Connection;
use Doctrine\DBAL\Driver\PDOSqlsrv\Driver;
use Keboola\DbWriterBundle\Exception\DbException;
use Keboola\StorageApi\Client as SapiClient;
use Keboola\Temp\Temp;
use Monolog\Logger;
use Keboola\Syrup\Exception\UserException;

class Writer
{
	/** @var Configuration */
	protected $configuration;

	/** @var Logger */
	protected $logger;

	/** @var Temp */
	protected $temp;

	/** @var SapiClient */
	protected $storageApi;

	/** @var Connection */
	protected $db;

	public function __construct(Configuration $configuration, Logger $logger, Temp $temp)
	{
		$this->configuration = $configuration;
		$this->logger = $logger;
		$this->temp = $temp;
		$this->storageApi = $configuration->getStorageApi();
	}

	protected function getConnection($dbParams)
	{
		return new Connection([
			'driver'    => isset($dbParams['driver'])?$dbParams['driver']:'pdo_mysql',
			'host'      => $dbParams['host'],
			'port'      => isset($dbParams['port'])?$dbParams['port']:null,
			'dbname'    => $dbParams['database'],
			'user'      => $dbParams['user'],
			'password'  => $dbParams['password'],
			'charset' => 'utf8',
		], new Driver());
	}

	public function run($params)
	{
		$writerId = null;
		if (isset($params['config'])) {
			$writerId = $params['config'];
		} else if (isset($params['writer'])) {
			$writerId = $params['writer'];
		}

		if ($writerId == null) {
			throw new UserException('Parameter "config" or "writer" must be specified');
		}

		$writerConfig = $this->configuration->getSysBucket($writerId);
		$tables = $this->configuration->getSysTables($writerId);

		if (!isset($writerConfig['db'])) {
			throw new UserException('Missing DB credentials');
		}

		$this->db = $this->getConnection($writerConfig['db']);

		if (isset($params['table'])) {
			$sysTableName = $this->configuration->getWriterTableName($params['table']);
			if (isset($tables[$sysTableName])) {
				$tables = [
					$tables[$sysTableName]
				];
			}
		}

		$uploaded = [];

		foreach ($tables as $table) {

			if (!$this->isTableValid($table)) {
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
			$this->storageApi->exportTable($sourceTableId, $sourceFilename, [
                'columns' => $colNames
            ]);

			$this->dropDbTable($outputTableName);

			$this->createDbTable($table);

			$this->loadToDbTable($sourceFilename, $outputTableName);

			$uploaded[] = $sourceTableId;
		}

		return [
			'status'    => 'ok',
			'uploaded'  => $uploaded
		];
	}

	protected function dropDbTable($tableName)
	{
		$this->runSql("DROP TABLE IF EXISTS `{$tableName}`;");
	}

	protected function createDbTable(array $table)
	{
		$columns = $table['items'];

		$query = "CREATE TABLE `{$table['dbName']}` (";

		foreach ($columns as $k => $col) {

			$type = strtoupper($col['type']);

			if ($type == 'IGNORE') {
				continue;
			}

			if (!empty($col['size'])) {
				$type .= "({$col['size']})";
			}

			$null = $col['null']?'NULL':'NOT NULL';

			$default = empty($col['default'])?'':$col['default'];
			if ($type == 'TEXT') {
				$default = '';
			}

			$query .= "`{$col['dbName']}` $type $null $default";

			$query .= ',';
		}

		$query = substr($query, 0, -1);

		$query .= ");";

		$this->runSql($query);
	}

	protected function loadToDbTable($sourceFilename, $outputTableName)
	{
		$fieldDelimiter = '\"';
		$lineDelimiter = '\n';

		$query = "LOAD DATA LOCAL INFILE '{$sourceFilename}'"
			. " REPLACE INTO TABLE `{$outputTableName}`"
			. " COLUMNS TERMINATED BY ','"
			. " OPTIONALLY ENCLOSED BY '" . $fieldDelimiter . "'"
			. " ESCAPED BY ''"
			. " LINES TERMINATED BY '" . $lineDelimiter . "'"
			. " IGNORE 1 LINES"
			. ";"
		;

		$this->runSql($query);
	}

	protected function runSql($query)
	{
		$errorFilename = $this->getErrorFilename();

		$dbConfig = $this->db->getParams();

		$command = 'mysql -u ' . escapeshellarg($dbConfig['user'])
			. ' -P ' . escapeshellarg($dbConfig['port'])
			. ' -p' . escapeshellarg($dbConfig['password'])
			. ' -h ' . escapeshellarg($dbConfig['host'])
			. ' ' . escapeshellarg($dbConfig['dbname'])
			. ' -e "' . escapeshellcmd($query) .'"'
			. ' --quick 2> ' . $errorFilename;

		$result = $this->exec($command);

		if ($result != "" || file_exists($errorFilename) && filesize($errorFilename) > 0) {
			$error = $result;
			if ($error == '') {
				$error = trim(file_get_contents($errorFilename));
			}
			throw new UserException("SQL export error: " . $error);
		}
	}

	protected function exec($command)
	{
		$password_pattern = "/\-p'[^']+'|\-P'[^']+'/";
		$logged_command = preg_replace($password_pattern,"PASSWORD",$command);
		$this->logger->debug("Executing command " . $logged_command);

		$command = str_replace("\\(", "(", $command);
		$command = str_replace("\\)", ")", $command);
		$command = str_replace("\\;", ";", $command);

		return exec($command);
	}

	private function getErrorFilename()
	{
		return $this->temp->createTmpFile('.csv.error', true);
	}

	private function isTableValid(array $table)
	{
		if (!count($table['items'])) {
			return false;
		}

		if (!isset($table['dbName'])) {
			return false;
		}

		if (!isset($table['tableId'])) {
			return false;
		}

		if (!isset($table['export']) || $table['export'] == false)
		{
			return false;
		}

		$ignoredCnt = 0;
		foreach ($table['items'] as $column) {
			if ($column['type'] == 'IGNORE') {
				$ignoredCnt++;
			}
		}

		if ($ignoredCnt == count($table['items'])) {
			return false;
		}

		return true;
	}
}
