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


    public function getSObjectFields($objectNames) {

        if(!is_array($objectNames))
            $objectNames = array($objectNames);

        try {
            if($this->sfdc === null || !$this->sfdc->getSessionId())
            {
                $this->login();
            }


            /* this is how you find a deleted record */
            /*
            try{
                $deletedResponse = $this->sfdc->getDeleted('Contact', strtotime('2016-06-22 00:00:00'), strtotime('2016-06-22 23:59:59'));
                print_r($deletedResponse);
                exit();
            }catch (Exception $e) {
                echo  $this->sfdc->getLastRequest();
                print_r($e);
            }*/
            //var_dump($this->sfdc->describeSObjects(array('lead')));exit();
            $result = $this->sfdc->describeSObjects($objectNames);

            if(!is_array($result))
                $result = array($result);

            $sObjects = array();
            foreach($result as $sObject)
                $sObjects[$sObject->name] = $sObject->fields;

            return $sObjects;
        } catch (Exception $e) {
            echo $this->sfdc->getLastRequest();
            echo $e->faultstring;
            die;
        }
    }

    /**
     * @param $object
     * @param $fields
     * @param null $startDate
     * @param null $fh
     * @return bool
     * @throws Exception
     */
    public function batchQuery($object, $fields, $startDate=null, $fh=null) {

        $this->initSession();

        $myBulkApiConnection = new BulkApiClient($this->session->serverUrl, $this->session->sessionId);
        $myBulkApiConnection->setLoggingEnabled(false);
        $myBulkApiConnection->setCompressionEnabled(true);

        // create in-memory representation of the job
        $job = new JobInfo();
        $job->setObject($object);
        $job->setOpertion('query');
        $job->setContentType('CSV');
        $job->setConcurrencyMode('Parallel');

        $soql = "SELECT " . implode(',', $fields) . " FROM $object";
        if($startDate != null)
            $soql .= " WHERE LastModifiedDate >= $startDate";
        //$soql .= " ALL ROWS";

        echo 'Creating job...';
        $job = $myBulkApiConnection->createJob($job);
        echo 'ok'.PHP_EOL;

        echo 'Creating batch...';
        $batch = $myBulkApiConnection->createBatch($job, $soql);
        echo 'ok'.PHP_EOL;

        echo 'Closing job...';
        $myBulkApiConnection->updateJobState($job->getId(), 'Closed');
        echo 'ok'.PHP_EOL;

        $sleepTime = 4;
        echo 'Waiting for job to complete...';
        while($batch->getState() == 'Queued' || $batch->getState() == 'InProgress') {
            // poll Salesforce for the status of the batch
            sleep($sleepTime *= 1.1);
            echo ".";
            $batch = $myBulkApiConnection->getBatchInfo($job->getId(), $batch->getId());
        }
        echo 'ok'.PHP_EOL;

        // get status of batches
        echo "Retrieving results...";
        $resultList = $myBulkApiConnection->getBatchResultList($job->getId(), $batch->getId());

        // retrieve queried data
        foreach ($resultList as $resultId)
            $myBulkApiConnection->getBatchResult($job->getId(), $batch->getId(), $resultId, $fh);

        echo 'ok'.PHP_EOL;

        if(isset($fh)) {

            $preview = stream_get_contents($fh,32,0);
            rewind($fh);

            if(strcasecmp($preview, 'Records not found for this query') == 0 || trim($preview) == false)
                // return false if no records returned
                return false;
            else
                return true;
        }
    }
}