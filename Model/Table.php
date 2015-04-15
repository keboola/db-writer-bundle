<?php
/**
 * Created by Miroslav Čillík <miro@keboola.com>
 * Date: 03/09/14
 * Time: 17:10
 */

namespace Keboola\DbWriterBundle\Model;

use Keboola\DbWriterBundle\Writer\Configuration;
use Keboola\StorageApi\Table as StorageApiTable;

class Table extends StorageApiTable
{
	protected $header = ['name', 'dbName', 'type', 'size', 'null', 'default'];

	protected $tableId;

	protected $dbName;

	/** @var Configuration */
	protected $configuration;

	public function __construct(Configuration $configuration, $writerId, $tableId)
	{
		$this->configuration = $configuration;
		$storageApi = $this->configuration->getStorageApi();
		$sysBucket = $this->configuration->getSysBucketId($writerId);
		$this->tableId = $tableId;
		$tableName = $configuration->getWriterTableName($tableId);

		parent::__construct($storageApi, $sysBucket . '.' . $tableName);
	}

	public function setDbName($dbName)
	{
		$this->dbName = $dbName;
	}

	public function getDbName()
	{
		return $this->dbName;
	}

	public function getAttribute($key)
	{
		if (isset($this->attributes[$key])) {
			return $this->attributes[$key];
		}
		return null;
	}

    public static function supportedTypes()
    {
        return [
            'IGONRE',
            'VARCHAR',
            'TEXT',
            'DECIMAL',
            'INT',
            'BIGINT',
            'TIMESTAMP',
            'DATE',
            'DATETIME'
        ];
    }
}
