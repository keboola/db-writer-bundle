<?php
/**
 * Created by Miroslav Čillík <miro@keboola.com>
 * Date: 25/03/14
 * Time: 11:56
 */

namespace Keboola\DbWriterBundle\Model;

use Keboola\DbWriterBundle\Exception\ConfigurationException;
use Keboola\StorageApi\Client as StorageApi;
use Keboola\StorageApi\Table;

class Manager
{
	/** @var StorageApi */
	protected $storageApi;

	protected $componentName;

	protected $sys_prefix = 'sys.c-';

	public function __construct(StorageApi $storageApi, $componentName)
	{
		$this->storageApi = $storageApi;
		$this->componentName = $componentName;
	}

	public function getStorageApi()
	{
		return $this->storageApi;
	}

	public function init()
	{
		try {
			$this->storageApi->createBucket($this->componentName, 'sys', 'DB Writer');
		} catch (\Exception $e) {
			// bucket exists
		}

		return $this;
	}

	public function getSysBucketId()
	{
		return 'sys.c-' . $this->componentName;
	}

	public function addAccount($name, $desc = null)
	{
		$accountId = $this->nameToId($name);
		$tableId = $this->getSysBucketId() . '.' . $accountId;

		if ($this->storageApi->tableExists($tableId)) {
			throw new ConfigurationException('Account `' . $accountId . '` already exists. Choose different name.');
		}

		$table = $this->getAccountTable($accountId);
		$table->setAttribute('accountId', $accountId);
		$table->setAttribute('name', $name);
		if ($desc != null) {
			$table->setAttribute('desc', $desc);
		}
		$table->save();
	}

	public function addRow($accountId, $input, $output)
	{
		$table = $this->getAccountTable($accountId);
		$table->setFromArray(array(array(
			'id'        => $this->storageApi->generateId(),
			'input'     => $input,
			'output'    => $output
		)));
		$table->save();
	}

	public function nameToId($name)
	{
		return strtolower(Table::removeSpecialChars($name));
	}

	private function getAccountTable($accountId)
	{
		$tableId = $this->getSysBucketId() . '.' . $accountId;
		$table = new Table($this->storageApi, $tableId, '', 'id', false, ',', '"', true);
		$table->setHeader(array('id','input','output'));
		return $table;
	}

	public function removeAccount($accountId)
	{
		$this->storageApi->dropTable($this->getSysBucketId() . '.' . $accountId);
	}

	public function removeRow($accountId, $rowId)
	{
		$tableId = $this->getSysBucketId() . '.' . $accountId;
		$this->storageApi->deleteTableRows($tableId, array(
			'whereColumn'   => 'id',
			'whereValues'   => $rowId
		));
	}

	public function addCredentials($accountId, $params)
	{
		$allowedParams = array('host', 'port', 'user', 'database');
		$password = $params['password'];
		$params = array_intersect_key($params, array_flip($allowedParams));

		$account = $this->getAccountTable($accountId);

		foreach ($params as $k => $v) {
			$account->setAttribute('db.' . $k, $v);
		}

		// set password as protected
		$this->storageApi->setTableAttribute($account->getId(), 'db.password', $password, true);

		$account->save();
	}
}
