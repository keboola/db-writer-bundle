<?php
/**
 * Created by Miroslav Čillík <miro@keboola.com>
 * Date: 27/05/15
 * Time: 17:18
 */

namespace Keboola\DbWriterBundle\Writer;

use Keboola\Csv\CsvFile;
use Keboola\Syrup\Exception\UserException;

class Oracle extends Writer implements WriterInterface
{
    protected static $allowedTypes = [
        'char',
        'nchar',
        'varchar2',
        'nvarchar',
        'blob',
        'clob',
        'nclob',
        'bfile',
        'number',
        'binary_float',
        'binary_double',
        'decimal',
        'float',
        'integer',
        'date',
        'timestamp',
        'raw',
        'rowid',
        'urowid'
    ];

    protected $dbParams;

    public function createConnection($dbParams)
    {
        $this->dbParams = $dbParams;
        $dbString = '//' . $dbParams['host'] . ':' . $dbParams['port'] . '/' . $dbParams['database'];

        return oci_connect($dbParams['user'], $dbParams['password'], $dbString, 'AL32UTF8');
    }

    public function write($sourceFilename, $outputTableName, $table)
    {
        $csv = new CsvFile($sourceFilename);

        $header = [];
        foreach ($table['items'] as $item) {
            if ($item['type'] != 'IGNORE') {
                $header[] = $item['dbName'];
            }
        }

        $csv->getHeader();
        $csv->next();

        while ($csv->current() != null) {
            for ($i = 0; $i < 1000 && $csv->current() != null; $i++) {

                $cols = [];
                foreach ($csv->current() as $col) {
                    $cols[] = "'" . $col . "'";
                }

                $sql = sprintf(
                    "INSERT INTO {$outputTableName} (%s) VALUES (%s)",
                    implode(',', $header),
                    implode(',', $cols));

                try {
                    $stmt = oci_parse($this->db, $sql);
                    oci_execute($stmt);
                } catch (\Exception $e) {
                    throw new UserException(
                        "Query failed: " . $e->getMessage(), $e, [
                        'query' => $sql
                    ]);
                }

                $csv->next();
            }
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

    public function drop($tableName)
    {
        try {
            oci_execute(oci_parse($this->db, sprintf("DROP TABLE %s", $tableName)));
        } catch (\Exception $e) {
            // table dont exists
        }
    }

    public function create(array $table)
    {
        $sql = "CREATE TABLE {$table['dbName']} (";

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

            $default = empty($col['default']) ? '' : "DEFAULT '" . $col['default'] . "'";
            if ($type == 'TEXT') {
                $default = '';
            }

            $sql .= "{$col['dbName']} $type $null $default";
            $sql .= ',';
        }

        $sql = substr($sql, 0, -1);
        $sql .= ")";

        oci_execute(oci_parse($this->db, $sql));
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
