<?php
/**
 * Created by Miroslav Čillík <miro@keboola.com>
 * Date: 19/03/14
 * Time: 12:27
 */

namespace Keboola\DbWriterBundle;


use Doctrine\DBAL\Driver\Mysqli\MysqliConnection;
use Keboola\DbWriterBundle\Exception\DbException;
use Symfony\Component\HttpKernel\Exception\HttpException;
use Syrup\ComponentBundle\Component\Component;
use Doctrine\DBAL\Connection;
use Doctrine\DBAL\Driver\PDOSqlsrv\Driver;
use Keboola\Csv\CsvFile;

class DbWriter extends Component
{
	protected $_name = 'db';
	protected $_prefix = 'wr';

	protected $errorFilename = '';

	protected function _process($config, $params)
	{
		if (empty($config)) {
			throw new HttpException(400, "Configuration not found or bad access permissions.");
		}

		$items = $config['items'];

		if (isset($params['account'])) {
			$items = array($items[$params['account']]);
		}

		foreach ($items as $conn) {
			if (isset($conn['db'])) {
				$this->_db = $this->getConnection($conn['db']);

				foreach ($conn['items'] as $table) {
					$this->write($table['input'], $table['output']);
				}
			}
		}


	}

	protected function getConnection($dbParams)
	{
		$dbal = new Driver();
		return new Connection(array(
			'driver'    => isset($dbParams['driver'])?$dbParams['driver']:'pdo_mysql',
			'host'      => $dbParams['host'],
			'port'      => isset($dbParams['port'])?$dbParams['port']:null,
			'dbname'    => $dbParams['database'],
			'user'      => $dbParams['user'],
			'password'  => $dbParams['password'],
			'charset' => 'utf8',
		), $dbal);
	}

	protected function write($input, $output)
	{
		$sourceFilename = $this->getTemp()->createTmpFile(null, true);

		$errorFilename = $this->getTemp()->createTmpFile('.csv.error', true);

		$this->_storageApi->exportTable($input, $sourceFilename);

		$fieldDelimiter = '\"';
		$lineDelimiter = '\n';

		$query = "LOAD DATA LOCAL INFILE '{$sourceFilename}'"
			. " INTO TABLE {$output}"
			. " COLUMNS TERMINATED BY ','"
			. " OPTIONALLY ENCLOSED BY '".$fieldDelimiter."'"
			. " ESCAPED BY ''"
			. " LINES TERMINATED BY '".$lineDelimiter."'"
			. " IGNORE 1 LINES"
			. ";"
		;

		$result = $this->exec($this->buildCommand($query, $errorFilename));

		if ($result != "" || file_exists($errorFilename) && filesize($errorFilename) > 0) {
			$error = $result;
			if ($error == '') {
				$error = trim(file_get_contents($errorFilename));
			}
			throw new DbException("SQL export error: " . $error);
		}
	}

	protected function buildCommand($query, $errorFilename)
	{
		$dbConfig = $this->_db->getParams();

		return 'mysql -u ' . escapeshellarg($dbConfig['user'])
			. ' -P ' . escapeshellarg($dbConfig['port'])
			. ' -p' . escapeshellarg($dbConfig['password'])
			. ' -h ' . escapeshellarg($dbConfig['host'])
			. ' ' . escapeshellarg($dbConfig['dbname'])
			. ' -e "' . $query .'"'
			. ' --quick 2> ' . $errorFilename;
	}

	protected function exec($command)
	{
		$password_pattern = "/\-p'[^']+'|\-P'[^']+'/";
		$logged_command = preg_replace($password_pattern,"PASSWORD",$command);
		$this->_log->debug("Executing command " . $logged_command);

		return exec($command);
	}

}
