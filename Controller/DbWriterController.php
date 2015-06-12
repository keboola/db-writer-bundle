<?php

namespace Keboola\DbWriterBundle\Controller;

use Keboola\DbWriterBundle\Exception\ParameterMissingException;
use Keboola\DbWriterBundle\Model\Table;
use Keboola\DbWriterBundle\Writer\Configuration;
use Keboola\Syrup\Elasticsearch\JobMapper;
use Keboola\Syrup\Elasticsearch\Search;
use Keboola\Syrup\Exception\UserException;
use Symfony\Component\HttpFoundation\JsonResponse;
use Symfony\Component\HttpFoundation\Request;
use Keboola\Syrup\Controller\ApiController;
use Keboola\Syrup\Job\Metadata\Job;

class DbWriterController extends ApiController
{
	/** @return Configuration */
	protected function getConfiguration()
	{
		return $this->container->get('wr_db.configuration_factory')->get($this->storageApi);
	}

    /**
     * @param $required
     * @param $params
     */
	protected function checkParams($required, $params)
	{
		foreach ($required as $r) {
			if (!isset($params[$r])) {
				throw new ParameterMissingException(sprintf("Parameter %s is missing.", $r));
			}
		}
	}

    /**
     * @return JobMapper
     */
    protected function getJobMapper()
    {
        return $this->container->get('syrup.elasticsearch.current_component_job_mapper');
    }

    /**
     * @return Search
     */
    protected function getElasticSearch()
    {
        return $this->container->get('syrup.elasticsearch.search');
    }

	/** Writers */

    /**
     * @param Request $request
     * @return JsonResponse
     */
	public function postWriterAction(Request $request)
	{
		$params = $this->getPostJson($request);
		$this->checkParams([
			'name'
		], $params);

		$description = isset($params['description'])?$params['description']:'DB Writer configuration bucket';

		return $this->createJsonResponse(
            $this->getConfiguration()->createWriter($params['name'], $description)
        );
	}

    /**
     * @param null $id
     * @return JsonResponse
     */
	public function getWritersAction($id = null)
	{
		if ($id != null) {
			return $this->createJsonResponse($this->getConfiguration()->getWriter($id));
		}
		return $this->createJsonResponse($this->getConfiguration()->getWriters());
	}

    /**
     * @param $id
     * @return JsonResponse
     */
	public function deleteWritersAction($id)
	{
		$this->getConfiguration()->deleteWriter($id);
		return $this->createJsonResponse([], 204);
	}


	/** Credentials */

    /**
     * @param         $writerId
     * @param Request $request
     * @return JsonResponse
     */
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

    /**
     * @param $writerId
     * @return JsonResponse
     */
	public function getCredentialsAction($writerId)
	{
		return $this->createJsonResponse($this->getConfiguration()->getCredentials($writerId));
	}


	/** Tables */

    /**
     * @param      $writerId
     * @param null $id
     * @return JsonResponse
     */
	public function getTablesAction($writerId, $id = null)
	{
		if ($id == null) {
			return $this->createJsonResponse($this->getConfiguration()->getTables($writerId));
		}

		return $this->createJsonResponse($this->getConfiguration()->getTable($writerId, $id));
	}

    /**
     * @param         $writerId
     * @param         $id
     * @param Request $request
     * @return JsonResponse
     */
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

    /**
     * @param         $writerId
     * @param         $tableId
     * @param Request $request
     * @return JsonResponse
     */
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

    /**
     * @param Request $request
     * @return JsonResponse
     */
	public function getJobsAction(Request $request)
	{
		$params = $request->query->all();

		$runId = isset($params['runId'])?$params['runId']:null;
		$query = isset($params['q'])?$params['q']:null;
		$offset = isset($params['offset'])?$params['offset']:0;
		$limit = isset($params['limit'])?$params['limit']:100;

		$sapiData = $this->storageApi->getLogData();
		$projectId = $sapiData['owner']['id'];

		$jobs = $this->getElasticSearch()->getJobs([
            'component' => $this->componentName,
            'runId' => $runId,
            'query' => $query,
            'projectId' => $projectId,
            'offset' => $offset,
            'limit' => $limit
        ]);

		$jobs = array_map(function ($item) {
			unset($item['token']['token']);
			return $item;
		}, $jobs);

		return $this->createJsonResponse($jobs);
	}

    /**
     * @param $writerId
     * @return JsonResponse
     */
	public function cancelWaitingJobsAction($writerId)
	{
		$sapiData = $this->storageApi->getLogData();
		$projectId = $sapiData['owner']['id'];

		$query="(status:waiting)AND(writer:$writerId)";

		$jobs = $this->getElasticSearch()->getJobs([
            'projectId' => $projectId,
            'component' => $this->componentName,
            'query' => $query
        ]);

        $jobMapper = $this->getJobMapper();
		foreach ($jobs as $item) {
			$job = new Job($item);
			$job->setStatus(Job::STATUS_CANCELLED);
            $jobMapper->update($job);
		}

		return $this->createJsonResponse([]);
	}
}
