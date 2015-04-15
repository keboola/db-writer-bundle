<?php
/**
 * Created by Miroslav Čillík <miro@keboola.com>
 * Date: 03/09/14
 * Time: 17:31
 */

namespace Keboola\DbWriterBundle\Model;

use Keboola\DbWriterBundle\Writer\Configuration;

class TableFactory
{
	/** @var Configuration  */
	protected $configuration;

	public function __construct(Configuration $configuration)
	{
		$this->configuration = $configuration;
	}

	public function get($writerId, $tableId)
	{
		return new Table($this->configuration, $writerId, $tableId);
	}
}
