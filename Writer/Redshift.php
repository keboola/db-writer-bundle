<?php
/**
 * Created by Ondrej Hlavacek <ondra@keboola.com>
 * Date: 13/07/15
 * Time: 14:51
 */

namespace Keboola\DbWriterBundle\Writer;

use Keboola\Csv\CsvFile;
use Keboola\Syrup\Exception\UserException;

class Redshift extends Writer implements WriterInterface
{
    protected static $allowedTypes = [
        'smallint', 'integer', 'bigint', 'decimal', 'real', 'double precision', 'boolean', 'char', 'varchar', 'date', 'timestamp'
    ];

    /** @var \PDO */
    protected $db;

    public function createConnection($dbParams)
    {
        // convert errors to PDOExceptions
        $options = [
            \PDO::ATTR_ERRMODE => \PDO::ERRMODE_EXCEPTION
        ];

        // check params
        foreach (['host', 'database', 'user', 'password', 'schema'] as $r) {
            if (!isset($dbParams[$r])) {
                throw new UserException(sprintf("Parameter %s is missing.", $r));
            }
        }

        $port = isset($dbParams['port']) ? $dbParams['port'] : '5439';
        $dsn = "pgsql:host={$dbParams['host']};port={$port};dbname={$dbParams["database"]}";

        $this->logger->info("Connecting to DSN '" . $dsn . "'...", [
            'options' => $options
        ]);

        $pdo = new \PDO($dsn, $dbParams['user'], $dbParams['password'], $options);
        $pdo->setAttribute(\PDO::MYSQL_ATTR_USE_BUFFERED_QUERY, false);
        $pdo->exec("SET search_path TO \"{$dbParams["schema"]}\";");
        return $pdo;
    }

    public function drop($tableName)
    {
        $this->db->exec("DROP TABLE IF EXISTS \"{$tableName}\";");
    }

    public function create(array $table)
    {
        $sql = "CREATE TABLE \"{$table['dbName']}\" (";

        $columns = $table['items'];
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

            $sql .= "\"{$col['dbName']}\" $type $null $default";
            $sql .= ',';
        }

        $sql = substr($sql, 0, -1);
        $sql .= ");";

        $this->db->exec($sql);
    }

    public function write($sourceFilename, $outputTableName, $table)
    {
        $csv = new CsvFile($sourceFilename);

        $colNames = [];
        foreach ($table['items'] as $item) {
            if ($item['type'] != 'IGNORE') {
                $colNames[] = $item['dbName'];
            }
        }

        $header = array_map(function ($item) {
            return "\"$item\"";
        }, $colNames);

        $csv->getHeader();
        $csv->next();

        while ($csv->current() != null) {
            $questionMarks = [];
            $data = [];
            for ($i=0; $i<1000 && $csv->current() != null; $i++) {
                $questionMarks[] = sprintf('(%s)', $this->getPlaceholders($csv->current()));
                $data = array_merge($data, $csv->current());
                $csv->next();
            }

            $sql = sprintf("INSERT INTO \"{$outputTableName}\" (%s) VALUES %s;", implode(',', $header), implode(',', $questionMarks));

            try {
                $stmt = $this->db->prepare($sql);
                $result = $stmt->execute($data);
            } catch (\PDOException $e) {
                throw new UserException("Query failed: " . $e->getMessage(), $e, [
                    'query' => $sql
                ]);
            }
        }
    }

    public function isTableValid(array $table)
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

    protected function getPlaceholders(array $row)
    {
        $result = [];
        foreach ($row as $r) {
            $result[] = '?';
        }
        return implode(',', $result);
    }

    public static function isTypeValid($type)
    {
        return in_array(strtolower($type), static::$allowedTypes);
    }

    public static function getAllowedTypes()
    {
        return static::$allowedTypes;
    }
}
