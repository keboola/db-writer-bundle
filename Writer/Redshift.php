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

    /**
     * @var \PDO
     */
    protected $db;

    protected $async = true;

    public function createConnection($dbParams)
    {
        // convert errors to PDOExceptions
        $options = [
            \PDO::ATTR_ERRMODE => \PDO::ERRMODE_EXCEPTION,
            \PDO::ATTR_EMULATE_PREPARES => false
        ];

        // check params
        foreach (['host', 'database', 'user', 'password', 'schema'] as $r) {
            if (!isset($dbParams[$r])) {
                throw new UserException(sprintf("Parameter %s is missing.", $r));
            }
        }

        $port = isset($dbParams['port']) ? $dbParams['port'] : '5439';
        $dsn = "pgsql:host={$dbParams['host']};port={$port};dbname={$dbParams["database"]}";

        $this->logger->info(
            "Connecting to DSN '" . $dsn . "'...",
            [
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

            $null = $col['null'] ? 'NULL' : 'NOT NULL';

            $default = empty($col['default']) ? '' : $col['default'];
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

    public function writeAsync($fileInfo, $table)
    {
        // Generate copy command
        $command = "COPY \"{$table}\"";

        if (isset($fileInfo["isSliced"]) && $fileInfo["isSliced"] === true) {
            $s3key = $fileInfo["s3Path"]["bucket"] . "/" . $fileInfo["s3Path"]["key"] . "manifest";
        } else {
            $s3key = $fileInfo["s3Path"]["bucket"] . "/" . $fileInfo["s3Path"]["key"];
        }

        $command .= " FROM 's3://{$s3key}'"
            . " CREDENTIALS 'aws_access_key_id={$fileInfo["credentials"]["AccessKeyId"]};aws_secret_access_key={$fileInfo["credentials"]["SecretAccessKey"]};token={$fileInfo["credentials"]["SessionToken"]}'"
            . " REGION AS 'us-east-1' DELIMITER ',' CSV QUOTE '\"'"
            . " NULL AS 'NULL' ACCEPTANYDATE TRUNCATECOLUMNS";

        // Sliced files use manifest and no header
        if (isset($fileInfo["isSliced"]) && $fileInfo["isSliced"] === true) {
            $command .= " MANIFEST";
        } else {
            $command .= " IGNOREHEADER 1";
        }

        $command .= " GZIP;";

        try {
            $this->db->exec($command);
        } catch (\PDOException $e) {
            $query = $this->db->query("SELECT * FROM stl_load_errors WHERE query = pg_last_query_id();");
            $result = $query->fetchAll();
            $params = [
                "query" => $command
            ];
            if (count($result)) {
                $message = "Table '{$table}', column '" . trim($result[0]["colname"]) . "', line {$result[0]["line_number"]}: ". trim($result[0]["err_reason"]);
                $params["redshift_errors"] = $result;
            } else {
                $message = "Query failed: " . $e->getMessage();
            }
            throw new UserException($message, $e, $params);
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

        if (!isset($table['export']) || $table['export'] == false) {
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

    public function write($sourceFilename, $outputTableName, $table)
    {
        throw new ApplicationException("Not implemented.");
    }

}
