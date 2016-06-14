<?php
/**
 * Created by PhpStorm.
 * User: WaiJe
 * Date: 6/7/2016
 * Time: 9:51 AM
 */
require_once('Replicator.php');
require_once('SalesforceV2.php');
require_once('SFObject.php');

class ReplicatorV2 extends Replicator {
    protected $cliOptions;
    protected $objectsAndFields = array();

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
            if (!isset($this->cliOptions['object']))
            {
                $this->loadObjectsFromFile();
            }
            elseif (isset($this->cliOptions['object']))
            {
                $this->loadObjectsFromCLI();
            }
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

    /**
     *
     */
    protected function loadObjectsFromFile()
    {
        $configContents = file_get_contents($this::CONFIG_FILE);
        $config = json_decode($configContents, true);
        $this->config['objects'] = $config['objects'];
    }

    /**
     *
     */
    protected function loadObjectsFromCLI()
    {
        $objects = $this->cliOptions['object'];
        if (!is_array($objects))
        {
            $objects = array($objects);
        }

        foreach ($objects as $object) {
            // capture the fields within
            $matches = array();
            $sourceObject = '';
            $targetTable = '';
            $initialString = $object;
            $fields = array();
            preg_match_all("/\[([^\]]*)\]/", $object, $matches);
            if (isset($matches[0][0])) // first bracket
            {
                $initialString = str_replace($matches[0][0], '', $object);
                $fieldString = $matches[1][0]; // what's inside the array
                $fieldArray = explode(",", $fieldString);
                $fieldName = '';
                $fieldTableName = '';
                //var_dump($matches[1][0], $fieldArray);exit();
                foreach ($fieldArray as $field) {
                    $fieldRenameArray = explode(":", $field);
                    $fieldName = $fieldRenameArray[0];
                    $fieldTableName = isset($fieldRenameArray[1]) ? trim($fieldRenameArray[1]) : $fieldName;
                    $fields[] = new SFObjectFields($fieldName, $fieldTableName);
                }

            }
            $nameArray = explode(':', $initialString);
            $sourceObject = trim($nameArray[0]);
            $targetTable = isset($nameArray[1]) ? trim($nameArray[1]) : $sourceObject;
            $this->objectsAndFields[$sourceObject] = new SFObject($sourceObject, $targetTable, $fields);
        }
        // first
        //print_r($this->objectsAndFields);
    }

    /**
     *
     */
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

        //var_dump($this->config);exit();
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
            'sf_wsdl',
            'force_load',
            'object'
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

    /**
     *
     */
    public function syncData()
    {
        if (!sizeof($this->objectsAndFields))
        {
            return parent::syncData();
        }

        // otherwise syncing data is a little bit different
        // store everything in UTC
        $originalTz = date_default_timezone_get();
        date_default_timezone_set('UTC');

        // make sure schema is synced
        if(!$this->schemaSynced)
        {
            $this->syncSchemaV2();
        }

        // load field types into memory
        if($this->fieldTypes === null)
        {
            foreach (array_keys($this->fieldsToQuery) as $objectName) {
                $object = $this->objectsAndFields[$objectName];
                $this->fieldTypes[$objectName] = $this->db->getFields($object->getTableName());
            }
        }

        // start syncing each object
        foreach($this->objectsAndFields as $objectName => $object) {

            echo "Starting sync - $objectName".PHP_EOL;

            $thisSync = date("Y-m-d H:i:s");
            $previousSync = date("Y-m-d\TH:i:s\Z", strtotime($this->db->getMostRecentSync($object->getTableName())));
            $tableFields = $object->fieldsSFTable();

            // open memory stream for storing csv
            $fh = fopen("php://memory", 'r+');

            $result = $this->sf->batchQuery($objectName, $this->fieldsToQuery[$objectName], $previousSync, $fh);
            if($result === false) {
                echo "No new or modified records found" . PHP_EOL;
                continue;
            }

            $header = fgetcsv($fh, 0, ',', '"', chr(0));
            $fieldCount = count($header);

            $fieldsToConvert = array();
            $fieldsToEscape = array();
            // decide which fields need to be treated before inserting into local db
            foreach ($header as $k => $field) {
                $header[$k] = $field = strtolower($tableFields[$field]);
                $convertFunction = $this->getConvertFunction($this->fieldTypes[$objectName][$field]);

                if($convertFunction != null)
                    $fieldsToConvert[$k] = $convertFunction;

                if($this->isEscapeRequired($this->fieldTypes[$objectName][$field]))
                    $fieldsToEscape[] = $k;
            }

            echo "Begin converting and upserting data...".PHP_EOL;

            $this->db->beginTransaction();
            $this->db->logSyncHistory($object->getTableName(), $thisSync);

            $upsertCsv = array();
            $rowCount = 0;
            //$upsertBatchSize = @$this->config['objects'][$objectName]['options']['upsertBatchSize'];
            $upsertBatchSize = 10000;


            while(($data = fgetcsv($fh, 0, ',', '"', chr(0))) !== false) {

                if(count($data) != $fieldCount) {
                    print_r($data);
                    throw new Exception('error parsing row');
                }

                foreach($fieldsToConvert as $key => $func)
                    $data[$key] = $this->$func($data[$key]);

                foreach($fieldsToEscape as $key)
                    $data[$key] = $this->db->quote($data[$key]);

                $upsertCsv[] = implode(',', $data);
                $rowCount++;
                if($upsertBatchSize && $rowCount >= $upsertBatchSize) {
                    $this->db->upsertCsvValues($object->getTableName(), $header, $upsertCsv, $upsertBatchSize);
                    $upsertCsv = array();
                    $rowCount = 0;
                }
            }

            if($rowCount > 0)
                $this->db->upsertCsvValues($objectName, $header, $upsertCsv, $upsertBatchSize);


            $this->db->commitTransaction();
            fclose($fh);

            echo "Finished sync - $objectName".PHP_EOL.PHP_EOL;
        }

        // revert to original timezone
        date_default_timezone_set($originalTz);
    }


    /**
     * Convert Salesforce object definitions into database tables and create fields as defined by createFieldDefinition().
     */
    public function syncSchemaV2() {
        // determine which sObjects need to be queried and determine fields that need adding

        $objectsToQuery = array();
        $objectsToQuerySF = array();
        $existingFields = array();
        $existingFieldsSF = array();
        $objectSource = array();
        foreach($this->objectsAndFields as $object) {

            $objectTable = $object->getTableName();
            $objectName = $object->getObjectName();
            //$fieldsSFTable
            $objectSource[$objectName] = $object;

            $tableFields = $object->getFieldsTableSF();
            $sfFields = $object->getFieldsSFTable();

            // get existing fields
            if($this->db->tableExists($objectTable))
            {
                $existingFields[$objectName] = $this->db->getFields($objectTable);
                $sfExistingFields = array();
                foreach ($existingFields[$objectName] as $tableField) {
                    $sfExistingFields[] = $sfFields[$tableField];
                }
                $existingFieldsSF[$objectName] = $sfExistingFields;
            }
            else
            {
                $existingFields[$objectName] = array();
                $existingFieldsSF[$objectName] = array();
            }


            $fields = $sfFields;

            // this gets overwritten later for objects with *
            $this->fieldsToQuery[$objectName] = $tableFields;

            if(in_array('*', $fields)) {
                // add all fields from Salesforce

                $objectsToQuery[$objectName] = true;
                $objectsToQuerySF[$objectName] = true;

            }
            elseif (is_array($existingFields[$objectName])) {
                // some fields already exist, check for new ones

                $diff = array_diff($tableFields, array_keys($existingFields[$objectName]));
                if(count($diff) > 0)
                {
                    $objectsToQuery[$objectName] = $diff;
                    $diffSF = array();
                    foreach($diff as $value) {
                        $diffSF[] = $sfFields[$value];
                    }
                    $objectsToQuerySF[$objectName] = $diffSF;
                }

            } else {
                // add all fields from config

                $objectsToQuery[$objectName] = $tableFields;
                $objectsToQuerySF[$objectName] = $sfFields;


            }

        }

        if(count($objectsToQuery) > 0) {

            $objectUpserts = array();

            // query object schemas from Salesforce
            // SalesForce only supports a maximum of 100 queries per call so we need to chunk this...

            $describeBatchSize = @$this->config['salesforce']['describeBatchSize'];
            if (!$describeBatchSize)
                $describeBatchSize = 100;

            $describeBatches = array_chunk( array_keys($objectsToQuery), $describeBatchSize );
            $describeResult = array();

            foreach( $describeBatches as $describeBatch ) {
                // query SalesForce for each batch of tables

                $describeBatchResult = $this->sf->getSObjectFields($describeBatch);
                $describeResult = array_merge($describeResult, $describeBatchResult);

            }

            foreach ($describeResult as $objectName => $fields) {
                $object = $objectSource[$objectName];
                $tableFields = $object->getFieldsTableSF();
                $sfFields = $object->getFieldsSFTable();

                $objectName = strtolower($objectName);
                $allFields = array_map(function($field) {return strtolower($field->name);}, $fields);
                $newFields = array();

                if(is_array($objectsToQuery[$objectName])) {
                    // check if any fields defined in config don't exist in Salesforce

                    $badFields = array_diff($objectsToQuerySF[$objectName], $allFields);
                    if(count($badFields) > 0)
                    {
                        throw new Exception("Invalid field(s): " . implode(', ', $badFields));
                    }

                    // get describe data for fields that need to be added
                    foreach ($fields as $field) {
                        if(in_array(strtolower($field->name), $objectsToQuerySF[$objectName]))
                        {
                            $newFields[] = $field;
                        }
                    }

                }
                elseif (array_key_exists($objectName, $objectsToQuery))
                {
                    // add all fields from Salesforce

                    $this->fieldsToQuery[$objectName] = $allFields;

                    foreach ($fields as $field) {
                        if (!array_key_exists(strtolower($field->name), $existingFieldsSF[$objectName]))
                        {
                            $newFields[] = $field;
                        }
                    }
                }

                // get mysql definition for each field
                $fieldDefs = array();
                foreach ($newFields as $field) {
                    $objectUpserts[$objectName][$field->name] = $this->createFieldDefinition($field);
                }

            }

            // update and create tables
            foreach ($objectUpserts as $objectName => $fieldDefs) {
                $object = $objectSource[$objectName];
                $this->db->upsertObject($object->getTableName(), $fieldDefs);
            }

        }

        $this->schemaSynced = true;
    }

}

$startReplicating = new ReplicatorV2($argv);
$startReplicating->syncData();