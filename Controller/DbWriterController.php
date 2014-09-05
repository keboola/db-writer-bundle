<?php

namespace Keboola\DbWriterBundle\Controller;

use Keboola\DbWriterBundle\DbWriter;
use Keboola\DbWriterBundle\Exception\ParameterMissingException;
use Keboola\DbWriterBundle\Writer\Configuration;
use Symfony\Component\HttpFoundation\Request;
use Syrup\ComponentBundle\Controller\ApiController;

class DbWriterController extends ApiController
{
	/** @return Configuration */
	protected function getConfiguration()
	{
		return $this->container->get('wr_db.configuration_factory')->get($this->storageApi);
	}

	protected function checkParams($required, $params)
	{
		foreach ($required as $r) {
			if (!isset($params[$r])) {
				throw new ParameterMissingException(sprintf("Parameter %s is missing.", $r));
			}
		}
	}

	/** Writers */

	public function postWriterAction(Request $request)
	{
		$params = $this->getPostJson($request);
		$this->checkParams([
			'name', 'connection'
		], $params);

		$this->checkParams([
			'host', 'database', 'user', 'password'
		], $params['connection']);

		$description = isset($params['description'])?$params['description']:'DB Writer configuration bucket';
		$bucketId = $this->getConfiguration()
			->createWriter($params['name'], $params['connection'], $description);

		return $this->createJsonResponse([
			'writerId'  => $params['name'],
			'bucketId'  => $bucketId
		]);
	}

	public function getWritersAction($id = null)
	{
		if ($id != null) {
			return $this->createJsonResponse($this->getConfiguration()->getWriter($id));
		}
		return $this->createJsonResponse($this->getConfiguration()->getWriters());
	}

	public function deleteWritersAction($id)
	{
		$this->getConfiguration()->deleteWriter($id);
		return $this->createJsonResponse(array(), 204);
	}


	/** Tables */

	public function getTablesAction($writerId, $id = null)
	{
		if ($id == null) {
			return $this->createJsonResponse($this->getConfiguration()->getTables($writerId));
		}

		return $this->createJsonResponse($this->getConfiguration()->getTable($writerId, $id));
	}

	public function postTableAction($writerId, $id, Request $request)
	{
		$params = $this->getPostJson($request);
		$this->checkParams([
			'dbName',
			'export'
		], $params);

		$sysTableId = $this->getConfiguration()->updateTable($writerId, $id, $params);

		return $this->createJsonResponse([
			'writerId'  => $writerId,
			'tableId'   => $sysTableId
		]);
	}

	public function postColumnsAction($writerId, $tableId, Request $request)
	{
		$params = $this->getPostJson($request);

		if (!is_array($params)) {
			throw new ParameterMissingException("Payload must be an array of columns");
		}

		foreach ($params as $param) {
			$this->checkParams([
				'name', 'dbName', 'type', 'size', 'null', 'default'
			], $param);
		}

		$sysTableId = $this->getConfiguration()->updateTableColumns($writerId, $tableId, $params);

		return $this->createJsonResponse([
			'writerId'  => $writerId,
			'tableId'   => $sysTableId
		]);
	}
}
