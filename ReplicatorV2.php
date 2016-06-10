<?php
/**
 * Created by PhpStorm.
 * User: WaiJe
 * Date: 6/7/2016
 * Time: 9:51 AM
 */
require_once('Replicator.php');
require_once('SalesforceV2.php');

class ReplicatorV2 extends Replicator {
    protected $cliOptions;

    /**
     * @param $argv
     */
    public function __construct($argv)
    {
        $this->cliOptions = getopt("", $this->getOptions());
        if (!sizeof($this->cliOptions))
        {
            // load config the regular way
            $this->loadConfig();
        }
        elseif (sizeof($this->cliOptions))
        {
            $this->loadParameters();
            $this->loadObjectsFromFile();
            $this->validateParameters();
        }

        // initialise Salesforce and database interfaces
        if(!empty($this->config['salesforce']['pass']))
            $pass = $this->config['salesforce']['pass'];
        else
            $pass = null;

        $this->sf = new SalesforceV2(
            $this->config['salesforce']['user'],
            $pass,
            !empty($this->cliOptions['sf_wsdl']) ? $this->cliOptions['sf_wsdl'] : null,
            !empty($this->cliOptions['sf_wsdl_mode']) ? $this->cliOptions['sf_wsdl_mode'] : null
        );
        if(!empty($this->config['salesforce']['endpoint']))
            $this->sf->setEndpoint($this->config['salesforce']['endpoint']);

        $this->db = new StorageDB(
            $this->config['database']['host'],
            $this->config['database']['user'],
            $this->config['database']['pass'],
            $this->config['database']['database']
        );

        // convert all fields and object names to lowercase and add default fields
        $this->config['objects'] = array_change_key_case($this->config['objects'], CASE_LOWER);
        foreach($this->config['objects'] as $objectName => $object) {
            $this->config['objects'][$objectName]['fields'] = array_map('strtolower', $object['fields']);
            array_unshift($this->config['objects'][$objectName]['fields'], 'lastmodifieddate');
            array_unshift($this->config['objects'][$objectName]['fields'], 'id');
        }

        //return parent::__construct();
        //print_r($options);exit();
    }

    protected function loadObjectsFromFile()
    {
        $configContents = file_get_contents($this::CONFIG_FILE);
        $config = json_decode($configContents, true);
        $this->config['objects'] = $config['objects'];
    }

    protected function loadParameters()
    {
        $this->config['salesforce'] = [
            'user' => !empty($this->cliOptions['sf_user']) ? $this->cliOptions['sf_user'] : "",
            'pass' => !empty($this->cliOptions['sf_pass']) ? $this->cliOptions['sf_pass'] : "",
            'endpoint' => !empty($this->cliOptions['sf_endpoint']) ? $this->cliOptions['sf_endpoint'] : ""
        ];
        $this->config['database'] = [
            'type' => 'mysql',
            'host' => !empty($this->cliOptions['db_host']) ? $this->cliOptions['db_host'] : "",
            'user' => !empty($this->cliOptions['db_user']) ? $this->cliOptions['db_user'] : "",
            'pass' => !empty($this->cliOptions['db_pass']) ? $this->cliOptions['db_pass'] : "",
            'database' => !empty($this->cliOptions['db_name']) ? $this->cliOptions['db_name'] : "",
        ];

        //var_dump($this->config, $this->cliOptions);exit();
    }

    /**
     *
     */
    protected function validateParameters()
    {
        if(empty($this->config['salesforce']) || empty($this->config['salesforce']['user']))
        {
            throw new Exception('Missing required Salesforce parameters...');
        }

        $db = $this->config['database'];
        if(empty($db) || empty($db['type']) || empty($db['host']) ||
            empty($db['user']) || !isset($db['pass']) || empty($db['database']))
        {
            throw new Exception('Missing required database parameters...');
        }
    }


    /**
     * @return array
     */
    private function getRequiredOptions()
    {
        return array(
            'sf_user',
            'sf_pass',
            'sf_endpoint',
            'sf_wsdl_mode',
            'sf_wsdl',
            'db_host',
            'db_user',
            'db_pass',
            'db_name',
        );
    }

    /**
     * @return array
     */
    private function getOptionalOptions()
    {
        return array(
            'sf_wsdl'
        );
    }

    /**
     * @return string
     */
    private function getOptions()
    {
        return array_merge(
            array_map(function($option)
                {
                    return $option . ':';
                },
                $this->getRequiredOptions()
            ),
            array_map(function($option)
                {
                    return $option . '::';
                },
                $this->getOptionalOptions()
            )
        );
        $shortOptions = "";
        foreach ($this->getRequiredOptions() as $option) {
            $shortOptions .= $option . ":";
        }

        foreach($this->getOptionalOptions() as $option) {
            $shortOptions .= $option . "::";
        }
        return $shortOptions;
    }

}

$startReplicating = new ReplicatorV2($argv);
$startReplicating->syncData();