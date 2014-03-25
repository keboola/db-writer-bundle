<?php
/**
 * Created by Miroslav Čillík <miro@keboola.com>
 * Date: 14/01/14
 * Time: 12:31
 */

namespace Keboola\DbWriterBundle\Exception;

use Syrup\ComponentBundle\Exception\SyrupComponentException;

class ConfigurationException extends SyrupComponentException
{
	public function __construct($message)
	{
		parent::__construct(400, "Wrong configuration: " . $message);
	}

}
