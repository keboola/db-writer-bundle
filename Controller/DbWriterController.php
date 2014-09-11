<?php

namespace Keboola\DbWriterBundle\Controller;

use Keboola\DbWriterBundle\Exception\ParameterMissingException;
use Keboola\DbWriterBundle\Writer\Configuration;
use Symfony\Component\HttpFoundation\Request;
use Syrup\ComponentBundle\Controller\ApiController;
use Syrup\ComponentBundle\Job\Metadata\Job;
use Syrup\ComponentBundle\Job\Metadata\JobManager;

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
			'name'
		], $params);

		$description = isset($params['description'])?$params['description']:'DB Writer configuration bucket';

		return $this->createJsonResponse($this->getConfiguration()->createWriter($params['name'], $description));
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


	/** Credentials */

	public function postCredentialsAction($writerId, Request $request)
	{
		$params = $this->getPostJson($request);
		$this->checkParams([
			'host', 'port', 'database', 'user', 'password'
		], $params);

		$this->getConfiguration()->setCredentials($writerId, $params);

		return $this->createJsonResponse([
			'writerId'  => $writerId
		]);
	}

	public function getCredentialsAction($writerId)
	{
		return $this->createJsonResponse($this->getConfiguration()->getCredentials($writerId));
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

	/** Jobs */

	public function getJobsAction(Request $request)
	{
		$params = $request->query->all();

		$runId = isset($params['runId'])?$params['runId']:null;
		$query = isset($params['q'])?$params['q']:null;
		$offset = isset($params['offset'])?$params['offset']:0;
		$limit = isset($params['limit'])?$params['limit']:JobManager::PAGING;

		$sapiData = $this->storageApi->getLogData();
		$projectId = $sapiData['owner']['id'];

		$jobs = $this->getJobManager()->getJobs($projectId, $this->componentName, $runId, $query, $offset, $limit);

		$jobs = array_map(function ($item) {
			unset($item['token']['token']);
			return $item;
		}, $jobs);

		return $this->createJsonResponse($jobs);
	}

	public function cancelWaitingJobsAction($writerId)
	{
		$sapiData = $this->storageApi->getLogData();
		$projectId = $sapiData['owner']['id'];

		$query="(status:waiting)AND(writer:$writerId)";

		$jobManager = $this->getJobManager();
		$jobs = $jobManager->getJobs($projectId, $this->componentName, null, $query);

		foreach ($jobs as $item) {
			$job = new Job($item);
			$job->setStatus(Job::STATUS_CANCELLED);
			$jobManager->updateJob($job);
		}

		return $this->createJsonResponse([]);
	}
}
