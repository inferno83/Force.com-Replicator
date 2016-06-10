<?php
/**
 * Created by PhpStorm.
 * User: WaiJe
 * Date: 6/10/2016
 * Time: 3:31 PM
 */
require_once('Salesforce.php');
//require_once('soapclient/SforceHeaderOptions.php');
//require_once('bulkclient/BulkApiClient.php');

class SalesforceV2 extends Salesforce {
    protected $wsdl_file;
    protected $wsdl_mode;

    function __construct($user, $pass = null, $wsdl_file = null, $wsdl_mode = null)
    {
        $this->wsdl_file = $wsdl_file;
        $this->wsdl_mode = $wsdl_mode;

        parent::__construct($user, $pass);

        if ($this->wsdl_file && !$this->wsdl_mode)
        {
            if (strpos(strtolower($this->wsdl_file), 'enterprise') !== false)
            {
                $this->wsdl_mode = 'enterprise';
            }
            elseif (strpos(strtolower($this->wsdl_file), 'partner') !== false)
            {
                $this->wsdl_mode = 'partner';
            }
        }

        if ($this->wsdl_mode)
        {
            switch ($this->wsdl_mode) {
                case 'enterprise':
                    require_once('soapclient/SforceEnterpriseClient.php');
                    $this->sfdc = new SforceEnterpriseClient();
                    break;
                case 'partner':
                    require_once('soapclient/SforcePartnerClient.php');
                    $this->sfdc = new SforcePartnerClient();
                    break;
            }
        }

        //var_dump($this->user, $this->pass, get_class($this->sfdc), $this->wsdl_file, $this->wsdl_mode);exit();

    }

    protected function login() {

        $this->session = null;
        $interactive = $this->pass === null;

        do {
            try {
                $this->sfdc->createConnection($this->wsdl_file);
                if($this->endpoint != null)
                    $this->sfdc->setEndpoint($this->endpoint);

                if($this->pass === null)
                    $pass = $this::prompt("Password:");
                else
                    $pass = $this->pass;

                $this->session = $this->sfdc->login($this->user, $pass);

            } catch (Exception $e) {
                echo $e->getMessage() . PHP_EOL;
            }
        } while($interactive && $this->session === null);

    }
}