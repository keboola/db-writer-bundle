<?php

namespace Keboola\DbWriterBundle\Controller;

use Keboola\DbWriterBundle\DbWriter;
use Syrup\ComponentBundle\Controller\ApiController;

class DbWriterController extends ApiController
{
	/** Configs */

	public function getConfigsAction()
	{
		return $this->createJsonResponse($this->getComponent()->getConfigs());
	}

	public function postConfigsAction()
	{
		$this->getComponent()->createConfig($this->getPostJson($this->getRequest()));

		return $this->createJsonResponse(array(
			'status'      => 'ok'
		));
	}

	public function deleteConfigAction($id)
	{
		$this->getComponent()->deleteConfig($id);

		return $this->createJsonResponse(array(), 204);
	}

	/**
	 * @return DbWriter
	 */
	private function getComponent()
	{
		return $this->component;
	}
}
