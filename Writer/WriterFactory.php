<?php
/**
 * Created by Miroslav Čillík <miro@keboola.com>
 * Date: 25/05/15
 * Time: 15:19
 */

namespace Keboola\DbWriterBundle\Writer;

use Keboola\Syrup\Exception\UserException;
use Monolog\Logger;

class WriterFactory
{
    public static $driversMap = [
        'mysql' => 'MySQL',
        'oracle' => 'Oracle',
        'pgsql' => 'PgSQL',
        'mssql' => 'MsSQL',
        'redshift' => 'Redshift'
    ];

    /** @var Logger */
    protected $logger;

    public function __construct(Logger $logger)
    {
        $this->logger = $logger;
    }

    public function get($dbParams)
    {
        if (!isset($dbParams['driver'])) {
            $dbParams['driver'] = 'mysql';
        }

        if (!array_key_exists($dbParams['driver'], static::$driversMap)) {
            throw new UserException(sprintf("Driver '%s' is not supported", $dbParams['driver']));
        }

        $className = __NAMESPACE__ . '\\' . static::$driversMap[$dbParams['driver']];

        return new $className($dbParams, $this->logger);
    }

}
