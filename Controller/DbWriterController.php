<?php

namespace Keboola\DbWriterBundle\Controller;

use Keboola\DbWriterBundle\Exception\ParameterMissingException;
use Keboola\DbWriterBundle\Writer\Configuration;
use Keboola\DbWriterBundle\Writer\WriterFactory;
use Keboola\Syrup\Elasticsearch\JobMapper;
use Keboola\Syrup\Elasticsearch\Search;
use Keboola\Syrup\Exception\ApplicationException;
use Keboola\Syrup\Exception\UserException;
use Symfony\Component\HttpFoundation\JsonResponse;
use Symfony\Component\HttpFoundation\Request;
use Keboola\Syrup\Controller\ApiController;
use Keboola\Syrup\Job\Metadata\Job;

class DbWriterController extends ApiController
{
    protected $driver = 'generic';

    /** @return Configuration */
    protected function getConfiguration()
    {
        return new Configuration($this->componentName, $this->storageApi, $this->driver);
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

    /**
     * @param $driver
     */
    private function checkDriver($driver)
    {
        if (!in_array($driver, array_keys(WriterFactory::$driversMap))) {
            throw new UserException("Driver '{$driver}' not found.");
        }
    }

    /**
     * Make sure that a given KBC component is valid.
     * @param string $componentName KBC Component name.
     * @throw UserException in case of invalid component.
     */
    private function checkComponent($componentName)
    {
        // Check list of components
        $components = $this->storageApi->indexAction();
        foreach ($components["components"] as $c) {
            if ($c["id"] == $componentName) {
                $component = $c;
                break;
            }
        }

        if (!isset($component)) {
            throw new UserException("Component '$componentName' not found.");
        }
    }

    /**
     * @param Request $request
     */
    public function preExecute(Request $request)
    {
        parent::preExecute($request);
        if ($request->get("driver")) {
            $this->checkDriver($request->get("driver"));
            $component = $this->getParameter("app_name") . '-' . $request->get("driver");
            $this->checkComponent($component);
            $this->componentName = $component;
            $this->driver = $request->get("driver");
        }
    }

    /** Override Run Action */

    /**
     * Override for custom component
     * @param Request $request
     * @param string  $driver
     * @return JsonResponse
     * @throws ApplicationException
     */
    public function runAction(Request $request, $driver = null)
    {
        // Get params from request
        $params = $this->getPostJson($request);

        $params["component"] = $this->componentName;

        // check params against ES mapping
        $this->checkMappingParams($params);

        // Create new job
        $job = $this->createJob('run', $params);

        // Add job to Elasticsearch
        try {
            /** @var JobMapper $jobMapper */
            $jobMapper = $this->container->get('syrup.elasticsearch.current_component_job_mapper');
            $jobId = $jobMapper->create($job);
        } catch (\Exception $e) {
            throw new ApplicationException("Failed to create job", $e);
        }

        // Add job to SQS
        $queueName = 'default';
        $queueParams = $this->container->getParameter('queue');

        if (isset($queueParams['sqs'])) {
            $queueName = $queueParams['sqs'];
        }
        $messageId = $this->enqueue($jobId, $queueName);

        $this->logger->info(
            'Job created',
            [
                'sqsQueue'     => $queueName,
                'sqsMessageId' => $messageId,
                'job'          => $job->getLogData()
            ]);

        // Response with link to job resource
        return $this->createJsonResponse(
            [
                'id'     => $jobId,
                'url'    => $this->getJobUrl($jobId),
                'status' => $job->getStatus()
            ],
            202);
    }


    /** Writers */

    /**
     * @param Request $request
     * @return JsonResponse
     */
    public function postWriterAction(Request $request)
    {
        $params = $this->getPostJson($request);
        $this->checkParams(
            [
                'name'
            ],
            $params);

        $description = isset($params['description']) ? $params['description'] : 'DB Writer configuration bucket';

        return $this->createJsonResponse(
            $this->getConfiguration()
                ->createWriter($params['name'], $description)
        );
    }

    /**
     * @param null $id
     * @return JsonResponse
     */
    public function getWritersAction($id = null)
    {
        if ($id != null) {
            return $this->createJsonResponse(
                $this->getConfiguration()
                    ->getWriter($id));
        }

        return $this->createJsonResponse(
            $this->getConfiguration()
                ->getWriters());
    }

    /**
     * @param $id
     * @return JsonResponse
     */
    public function deleteWritersAction($id)
    {
        $this->getConfiguration()
            ->deleteWriter($id);

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
        $this->checkParams(
            [
                'host', 'port', 'database', 'user', 'password'
            ],
            $params);

        $this->getConfiguration()
            ->setCredentials($writerId, $params);

        return $this->createJsonResponse(
            [
                'writerId' => $writerId
            ]);
    }

    /**
     * @param $writerId
     * @return JsonResponse
     */
    public function getCredentialsAction($writerId)
    {
        return $this->createJsonResponse(
            $this->getConfiguration()->getCredentials($writerId)
        );
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
            return $this->createJsonResponse(
                $this->getConfiguration()
                    ->getTables($writerId));
        }

        return $this->createJsonResponse(
            $this->getConfiguration()
                ->getTable($writerId, $id));
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
        $this->checkParams(
            [
                'dbName',
                'export'
            ],
            $params);

        $sysTableId = $this->getConfiguration()
            ->updateTable($writerId, $id, $params);

        return $this->createJsonResponse(
            [
                'writerId' => $writerId,
                'tableId'  => $sysTableId
            ]);
    }

    public function getConfigTablesAction($writerId, $id)
    {
        $tables = $this->getConfiguration()->getSysTables($writerId);

        if ($id !== null) {
            $tableName = $this->getConfiguration()->getWriterTableName($id);

            return $this->createJsonResponse($this->formatTableResponse($tables[$tableName]));
        }

        return $this->createJsonResponse(array_map(function ($item) {
            return $this->formatTableResponse($item);
        }, $tables));
    }

    private function formatTableResponse($table)
    {
        $tableNameArr = explode('.', $table['id']);

        return [
            'id' => $table['id'],
            'bucket' => $tableNameArr[0] . '.' . $tableNameArr[1],
            'name' => $table['dbName'],
            'export' => $table['export'],
            'lastChange' => $table['lastChange'],
            'columns' => $table['items']
        ];
    }

    public function deleteConfigTableAction($writerId, $id)
    {
        $this->getConfiguration()->deleteTable($writerId, $id);

        return $this->createJsonResponse([], 204);
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
            $this->checkParams(
                [
                    'name', 'dbName', 'type', 'size', 'null', 'default'
                ],
                $param);
        }

        $sysTableId = $this->getConfiguration()
            ->updateTableColumns($writerId, $tableId, $params);

        return $this->createJsonResponse(
            [
                'writerId' => $writerId,
                'tableId'  => $sysTableId
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

        $runId = isset($params['runId']) ? $params['runId'] : null;
        $query = isset($params['q']) ? $params['q'] : null;
        $offset = isset($params['offset']) ? $params['offset'] : 0;
        $limit = isset($params['limit']) ? $params['limit'] : 100;

        $sapiData = $this->storageApi->getLogData();
        $projectId = $sapiData['owner']['id'];

        $jobs = $this->getElasticSearch()
            ->getJobs(
                [
                    'component' => $this->getParameter("app_name"),
                    'runId'     => $runId,
                    'query'     => $query,
                    'projectId' => $projectId,
                    'offset'    => $offset,
                    'limit'     => $limit
                ]);

        $jobs = array_map(
            function ($item) {
                unset($item['token']['token']);

                return $item;
            },
            $jobs);

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

        $query = "(status:waiting)AND(writer:$writerId)";

        $jobs = $this->getElasticSearch()
            ->getJobs(
                [
                    'projectId' => $projectId,
                    'component' => $this->getParameter("app_name"),
                    'query'     => $query
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
