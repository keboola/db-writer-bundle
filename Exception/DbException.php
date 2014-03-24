<?php
/**
 * Created by Miroslav Čillík <miro@keboola.com>
 * Date: 04/12/13
 * Time: 14:21
 */

namespace Keboola\DbWriterBundle\Exception;


use Syrup\ComponentBundle\Exception\SyrupComponentException;

class DbException extends SyrupComponentException
{

	public function __construct($message = null, \Exception $previous = null, array $headers = array(), $code = 0)
	{
		parent::__construct(400, $message, $previous, $headers, $code);
	}


}
