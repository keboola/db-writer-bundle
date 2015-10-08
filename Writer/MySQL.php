<?php
/**
 * Created by Miroslav Čillík <miro@keboola.com>
 * Date: 25/05/15
 * Time: 14:51
 */

namespace Keboola\DbWriterBundle\Writer;

use Keboola\Csv\CsvFile;
use Keboola\Syrup\Exception\UserException;

class MySQL extends Writer implements WriterInterface
{
    protected static $allowedTypes = [
        'int', 'smallint', 'bigint',
        'decimal', 'float', 'double',
        'date', 'datetime', 'timestamp',
        'char', 'varchar', 'text', 'blob'
    ];

    /** @var \PDO */
    protected $db;

    public function createConnection($dbParams)
    {
        // convert errors to PDOExceptions
        $options = [
            \PDO::ATTR_ERRMODE => \PDO::ERRMODE_EXCEPTION,
            \PDO::MYSQL_ATTR_LOCAL_INFILE => true
        ];

        // check params
        foreach (['host', 'database', 'user', 'password'] as $r) {
            if (!isset($dbParams[$r])) {
                throw new UserException(sprintf("Parameter %s is missing.", $r));
            }
        }

        $port = isset($dbParams['port']) ? $dbParams['port'] : '3306';
        $dsn = sprintf("mysql:host=%s;port=%s;dbname=%s;charset=utf8", $dbParams['host'], $port, $dbParams['database']);

        $this->logger->info(
            "Connecting to DSN '" . $dsn . "'...",
            [
                'options' => $options
            ]);

        $pdo = new \PDO($dsn, $dbParams['user'], $dbParams['password'], $options);
        $pdo->setAttribute(\PDO::MYSQL_ATTR_USE_BUFFERED_QUERY, false);
        $pdo->exec("SET NAMES utf8;");

        return $pdo;
    }

    public function drop($tableName)
    {
        $this->db->exec("DROP TABLE IF EXISTS `{$tableName}`;");
    }

    public function create(array $table)
    {
        $sql = "CREATE TABLE `{$table['dbName']}` (";

        $columns = $table['items'];
        foreach ($columns as $k => $col) {

            $type = strtoupper($col['type']);
            if ($type == 'IGNORE') {
                continue;
            }

            if (!empty($col['size'])) {
                $type .= "({$col['size']})";
            }

            $null = $col['null'] ? 'NULL' : 'NOT NULL';

            $default = empty($col['default']) ? '' : $col['default'];
            if ($type == 'TEXT') {
                $default = '';
            }

            $sql .= "`{$col['dbName']}` $type $null $default";
            $sql .= ',';
        }

        $sql = substr($sql, 0, -1);
        $sql .= ");";

        $this->db->exec($sql);
    }

    public function write($sourceFilename, $outputTableName, $table)
    {
        $query = "
            LOAD DATA LOCAL INFILE '{$sourceFilename}'
            INTO TABLE `{$outputTableName}`
            FIELDS TERMINATED BY ','
            OPTIONALLY ENCLOSED BY '\"'
            ESCAPED BY ''
            IGNORE 1 LINES
        ";

        try {
            $this->db->exec($query);
        } catch (\PDOException $e) {
            throw new UserException("Query failed: " . $e->getMessage(), $e, [
                'query' => $query
            ]);
        }
    }

    public function isTableValid(array $table, $ignoreExport = false)
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

        if (!$ignoreExport && (!isset($table['export']) || $table['export'] == false)) {
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
