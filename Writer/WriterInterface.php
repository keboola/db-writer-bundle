<?php
/**
 * Created by Miroslav Čillík <miro@keboola.com>
 * Date: 25/05/15
 * Time: 14:52
 */

namespace Keboola\DbWriterBundle\Writer;

interface WriterInterface
{
    public function getConnection();

    public function createConnection($dbParams);

    public function write($sourceFilename, $outputTableName, $table);

    public function writeAsync($tableId, $outputTableName);

    public function isTableValid(array $table, $ignoreExport = false);

    public function drop($tableName);

    public function create(array $table);

    public static function getAllowedTypes();

    public function isAsync();
}
