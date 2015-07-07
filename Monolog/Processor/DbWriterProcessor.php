<?php

namespace Keboola\DbWriterBundle\Monolog\Processor;

/**
 * Class DbWriterProcessor implements a simple log processor which changes component
 *  name of events.
 * @package Keboola\DockerBundle\Monolog
 */
class DbWriterProcessor
{
    /**
     * @param  array $record
     * @return array
     */
    public function __invoke(array $record)
    {
        return $this->processRecord($record);
    }


    /**
     * Constructor.
     * @param $componentName string Component name.
     */
    public function __construct($componentName)
    {
        $this->componentName = $componentName;
    }


    /**
     * Process event record.
     *
     * @param array $record Log Event.
     * @return array Log event.
     */
    public function processRecord(array $record)
    {
        $record['component'] = $this->componentName;
        $record['app'] = 'wr-db';
        return $record;
    }
}
