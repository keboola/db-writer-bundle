<?php
/**
 * Created by Miroslav Čillík <miro@keboola.com>
 * Date: 03/09/14
 * Time: 14:40
 */

namespace Keboola\DbWriterBundle\Writer;

use Keboola\StorageApi\Client as StorageApi;

class ConfigurationFactory
{
	protected $componentName;
	protected $driver = 'generic';

	public function __construct($componentName, $driver = 'generic')
	{
		$this->componentName = $componentName;
		$this->driver = $driver;
	}

	public function get(StorageApi $sapi)
	{
		return new Configuration($this->componentName, $sapi, $this->driver);
	}

}
