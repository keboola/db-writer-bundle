<?php
/**
 * Created by Miroslav Čillík <miro@keboola.com>
 * Date: 04/12/13
 * Time: 14:21
 */

namespace Keboola\DbWriterBundle\Exception;

use Keboola\Syrup\Exception\UserException;

class DbException extends UserException
{
	public function __construct($message = null, \Exception $previous = null, $data = [])
	{
		parent::__construct($message, $previous, $data);
	}
}
