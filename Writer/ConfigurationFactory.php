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

	public function __construct($componentName)
	{
		$this->componentName = $componentName;
	}

	public function get(StorageApi $sapi)
	{
		return new Configuration($this->componentName, $sapi);
	}

}
