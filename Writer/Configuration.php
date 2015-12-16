<?php
/**
 * Created by Miroslav Čillík <miro@keboola.com>
 * Date: 03/09/14
 * Time: 14:32
 */

namespace Keboola\DbWriterBundle\Writer;

use Keboola\DbWriterBundle\Model\TableFactory;
use Keboola\StorageApi\Client as StorageApi;
use Keboola\StorageApi\ClientException;
use Keboola\StorageApi\Config\Exception;
use Keboola\StorageApi\Config\Reader;
use Keboola\Syrup\Exception\UserException;

class Configuration
{
    /** @var StorageApi */
    protected $storageApi;

    protected $componentName;

    protected $driver = 'generic';

    const SYS_PREFIX = 'sys.c-';

    const IN_PREFIX = 'in.c-';

    protected $tokenExpiration = 172800;

    /** @var  TableFactory */
    protected $tableFactory;

    public function __construct($componentName, $sapi, $driver = 'generic')
    {
        $this->componentName = $componentName;
        $this->storageApi = $sapi;
        $this->driver = $driver;

        $this->tableFactory = new TableFactory($this);
    }

    public function getStorageApi()
    {
        return $this->storageApi;
    }

    public function getSysBucketId($writerId)
    {
        return self::SYS_PREFIX . $this->componentName . '-' . $writerId;
    }

    public function getWriterTableName($tableId)
    {
        return str_replace('.', '_', str_replace('out.', '', $tableId));
    }

    public function getSysBucket($writerId)
    {
        return $this->readBucketConfig($this->getSysBucketId($writerId));
    }

    protected function readBucketConfig($bucketId)
    {
        Reader::$client = $this->storageApi;

        return Reader::read($bucketId);
    }

    protected function getSapiAttribute($attributes, $key)
    {
        foreach ($attributes as $attr) {
            if ($attr['name'] == $key) {
                return $attr['value'];
            }
        }

        return null;
    }

    protected function getOutTables()
    {
        $buckets = $this->storageApi->listBuckets();
        $outTables = [];

        foreach ($buckets as $bucket) {
            if ($bucket['stage'] == StorageApi::STAGE_OUT) {
                $bucketTables = $this->storageApi->listTables($bucket['id']);

                if (count($bucketTables)) {
                    $outTables = array_merge($outTables, $bucketTables);
                }
            }
        }

        return $outTables;
    }

    public function getWriterTables($writerId)
    {
        return $this->storageApi->listTables($this->getSysBucketId($writerId));
    }

    public function createWriter($name, $description = '')
    {
        $bucketName = $this->componentName . '-' . $name;
        $bucketId = self::SYS_PREFIX . $bucketName;
        if (!$this->storageApi->bucketExists($bucketId)) {
            $this->storageApi->createBucket($bucketName, StorageApi::STAGE_SYS, $description);
        }

        $this->storageApi->setBucketAttribute($bucketId, 'writer', 'db');
        $this->storageApi->setBucketAttribute($bucketId, 'driver', $this->driver);
        $this->storageApi->setBucketAttribute($bucketId, 'writerId', $name);

        if (!empty($description)) {
            $this->storageApi->setBucketAttribute($bucketId, 'description', $description);
        }

        return [
            'id' => $name,
            'name' => $name,
            'description' => $description
        ];
    }

    public function setCredentials($writerId, $credentials)
    {
        $bucketId = $this->getSysBucketId($writerId);

        if (!isset($credentials['driver']) || empty($credentials['driver'])) {
            $credentials['driver'] = 'mysql';
        }

        foreach (array_keys($credentials) as $key) {
            $secret = false;
            if ($key == 'password') {
                $secret = true;
            }
            $this->storageApi->setBucketAttribute($bucketId, 'db.' . $key, $credentials[$key], $secret);
        }
    }

    public function getCredentials($writerId)
    {
        $sysBucketConfig = $this->readBucketConfig($this->getSysBucketId($writerId));
        if (!isset($sysBucketConfig['db'])) {
            return null;
        }
        $credentials = $sysBucketConfig['db'];

        $driver = 'mysql';
        if (isset($credentials['driver'])) {
            $driver = $credentials['driver'];
        }

        /** @var WriterInterface $writerClassName */
        $writerClassName = __NAMESPACE__ . '\\' . WriterFactory::$driversMap[$driver];

        $credentials['allowedTypes'] = $writerClassName::getAllowedTypes();

        return $credentials;
    }

    public function getWriters()
    {
        $buckets = $this->storageApi->listBuckets();

        $writerBuckets = array_filter(
            $buckets,
            function ($item) {
                $attrWriter = $this->getSapiAttribute($item['attributes'], 'writer');
                $attrDriver = $this->getSapiAttribute($item['attributes'], 'driver');
                return (
                    $attrWriter == 'db'
                    &&
                    (
                        $this->driver == 'generic' && $attrDriver == null
                        ||
                        $this->driver == $attrDriver
                    )
                );
            });

        $res = [];
        foreach ($writerBuckets as $bucket) {
            $bucketConfig = $this->readBucketConfig($bucket['id']);

            $res[] = [
                'id' => $bucketConfig['writerId'],
                'name' => $bucketConfig['writerId'],
                'description' => isset($bucketConfig['description']) ? $bucketConfig['description'] : ''
            ];
        }

        return $res;
    }

    public function getWriter($id)
    {
        $bucketId = $this->getSysBucketId($id);
        try {
            $bucketConfig = $this->readBucketConfig($bucketId);
        } catch (Exception $e) {
            throw new UserException($e->getMessage(), $e);
        }

        return [
            'id' => $bucketConfig['writerId'],
            'name' => $bucketConfig['writerId'],
            'description' => isset($bucketConfig['description']) ? $bucketConfig['description'] : ''
        ];
    }

    public function deleteWriter($id)
    {
        $bucket = $this->storageApi->getBucket($this->getSysBucketId($id));

        foreach ($bucket['tables'] as $table) {
            $this->storageApi->dropTable($table['id']);
        }

        $this->storageApi->dropBucket($bucket['id']);
    }

    public function getTables($writerId)
    {
        $writerTables = $this->getSysTables($writerId);
        $outTables = $this->getOutTables();

        $tables = [];
        foreach ($outTables as $table) {

            $writerTableName = $this->getWriterTableName($table['id']);
            $configTableExists = isset($writerTables[$writerTableName]);

            //@todo check table attributes - raise configuration exception

            $tableIdArr = explode('.', $table['id']);
            $bucketId = $tableIdArr[0] . '.' . $tableIdArr[1];

            $tables[] = [
                'id' => $table['id'],
                'bucket' => $bucketId,
                'name' => $configTableExists ? $writerTables[$writerTableName]['dbName'] : $table['id'],
                'export' => ($configTableExists && $writerTables[$writerTableName]['export']),
                'lastChange' => $configTableExists ? $writerTables[$writerTableName]['lastChange'] : 'N\A'
            ];
        }

        //@todo cleanup script

        return $tables;
    }

    public function getSysTables($writerId)
    {
        $bucketConfig = $this->readBucketConfig($this->getSysBucketId($writerId));

        if (isset($bucketConfig['items'])) {
            return $bucketConfig['items'];
        }

        return [];
    }

    public function getTable($writerId, $id)
    {
        $outTable = $this->storageApi->getTable($id);

        $sysBucketId = $this->getSysBucketId($writerId);
        $tableName = $this->getWriterTableName($id);

        $bucketConfig = $this->readBucketConfig($sysBucketId);

        $sysTable = null;
        if (isset($bucketConfig['items']) && isset($bucketConfig['items'][$tableName])) {
            $sysTable = $bucketConfig['items'][$tableName];
        }

        //@todo: check systable attributs

        $columns = [];
        foreach ($outTable['columns'] as $col) {

            if (
                isset($sysTable['items'])
                && !empty($sysTable['items'])
                && (($configColumn = $this->getColumnFromConfig($sysTable['items'], $col)) != null)
            ) {
                $columns[] = $configColumn;
            } else {
                $columns[] = [
                    'name' => $col,
                    'dbName' => $col,
                    'type' => 'IGNORE',
                    'size' => '',
                    'null' => 0,
                    'default' => ''
                ];
            }
        }

        return [
            'id' => $id,
            'bucket' => $outTable['bucket']['id'],
            'name' => is_null($sysTable) ? $id : $sysTable['dbName'],
            'export' => is_null($sysTable) ? false : $sysTable['export'],
            'lastChange' => is_null($sysTable) ? 'N/A' : $sysTable['lastChange'],
            'columns' => $columns
        ];
    }

    protected function getColumnFromConfig($items, $key)
    {
        foreach ($items as $item) {
            if ($item['name'] == $key) {
                return $item;
            }
        }

        return null;
    }

    public function updateTable($writerId, $id, $data)
    {
        $table = $this->tableFactory->get($writerId, $id);

        $sysTables = $this->getSysTables($writerId);

        $tableData = [];
        if (isset($sysTables[$table->getName()])) {
            $tableData = $sysTables[$table->getName()]['items'];
        }

        foreach ($data as $k => $v) {
            $table->setAttribute($k, $v);
        }
        $table->setAttribute('tableId', $id);
        $table->setAttribute('lastChange', date('c'));
        $table->setFromArray($tableData);
        $table->save();

        return $table->getId();
    }

    public function updateTableColumns($writerId, $tableId, $params)
    {
        $table = $this->tableFactory->get($writerId, $tableId);
        if (!$this->getStorageApi()
            ->tableExists($table->getId())
        ) {
            $table->setAttribute('tableId', $tableId);
            $table->setAttribute('dbName', $tableId);
            $table->setAttribute('export', 0);
            $table->setAttribute('lastChange', date('c'));
        }

        $table->setFromArray($params);
        $table->save();

        return $table->getId();
    }

    public function deleteTable($writerId, $id)
    {
        $table = $this->tableFactory->get($writerId, $id);

        try {
            $this->storageApi->dropTable($table->getId());
        } catch (ClientException $e) {
            // table already deleted
        }
    }

}
