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

        //var_dump($this->sf);exit();
        $this->db = new StorageDB(
            $this->config['database']['host'],
            $this->config['database']['user'],
            $this->config['database']['pass'],
            $this->config['database']['database']
        );

        if($this->cliOptions['force_load'] === true && empty($this->cliOptions['object'])) {
            $this->db->query("DROP DATABASE " . $this->config['database']['database']);
            $this->db->query("CREATE DATABASE " . $this->config['database']['database']);
        }

        // convert all fields and object names to lowercase and add default fields
        if (!isset($this->cliOptions['object']))
        {
            $this->config['objects'] = array_change_key_case($this->config['objects'], CASE_LOWER);
            foreach($this->config['objects'] as $objectName => $object) {
                $this->config['objects'][$objectName]['fields'] = array_map('strtolower', $object['fields']);
                array_unshift($this->config['objects'][$objectName]['fields'], 'lastmodifieddate');
                array_unshift($this->config['objects'][$objectName]['fields'], 'id');
            }
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

                $fieldArray[] = 'id:id';
                $fieldArray[] = 'lastmodifieddate:lastmodifieddate';

                //var_dump($matches[1][0], $fieldArray);exit();
                foreach ($fieldArray as $field) {
                    $fieldRenameArray = explode(":", $field);
                    $fieldName = trim($fieldRenameArray[0]);
                    $fieldTableName = isset($fieldRenameArray[1]) ? trim($fieldRenameArray[1]) : $fieldName;
                    $fields[strtolower($fieldName)] = new SFObjectFields($fieldName, $fieldTableName);
                }

            }
            $nameArray = explode(':', $initialString);
            $sourceObject = trim($nameArray[0]);
            $targetTable = isset($nameArray[1]) ? trim($nameArray[1]) : $sourceObject;
            $this->objectsAndFields[strtolower($sourceObject)] =
                new SFObject($sourceObject, $targetTable, $fields, array('id', 'lastmodifieddate'));
        }
        // first
        //print_r($this->objectsAndFields);exit();
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

        //var_dump($this->fieldsToQuery);exit();
        // load field types into memory
        if($this->fieldTypes === null)
        {
            foreach (array_keys($this->fieldsToQuery) as $objectName) {
                $object = $this->objectsAndFields[$objectName];
                //var_dump($object->getTableName());exit();
                $this->fieldTypes[$objectName] = $this->db->getFields($object->getTableName());
            }
        }
        // start syncing each object
        foreach($this->objectsAndFields as $objectName => $object) {

            $objectTable = $object->getTableName();
            echo "Starting sync - $objectName for table: $objectTable".PHP_EOL;

            $thisSync = date("Y-m-d H:i:s");
            $previousSync = date("Y-m-d\TH:i:s\Z", strtotime($this->db->getMostRecentSync($object->getTableName())));

            $fieldsIndexedBySF = $object->getFieldsSFTable();
            if (!sizeof($fieldsIndexedBySF)) // must be trying to query all
            {
                foreach ($this->fieldsToQuery[$objectName] as $field) {
                    $fieldsIndexedBySF[$field] = $field;
                }
            }
            /*if ($objectName == 'direct_deal__c')
            {
                var_dump($fieldsIndexedBySF);exit();
            }*/
            //var_dump($objectName, $this->fieldsToQuery[$objectName]);exit();
            $fieldsIndexedByTable = array_flip($fieldsIndexedBySF);

            // open memory stream for storing csv
            $fh = fopen("php://memory", 'r+');

            $result = $this->sf->batchQuery($objectName, array_flip($this->fieldsToQuery[$objectName]), $previousSync, $fh);

            if($result === false) {
                echo "No new or modified records found" . PHP_EOL;
                continue;
            }

            $header = fgetcsv($fh, 0, ',', '"', chr(0));
            $fieldCount = count($header);
            //var_dump($header, $sfFields, $this->fieldTypes);exit();
            $fieldsToConvert = array();
            $fieldsToEscape = array();
            // decide which fields need to be treated before inserting into local db

            foreach ($header as $k => $field) {
                $header[$k] = $field = strtolower($fieldsIndexedBySF[strtolower($field)]);

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

                    $this->db->upsertCsvValues($objectTable, $header, $upsertCsv, $upsertBatchSize);
                    $upsertCsv = array();
                    $rowCount = 0;
                }
            }

            if($rowCount > 0)
                $this->db->upsertCsvValues($objectTable, $header, $upsertCsv, $upsertBatchSize);


            $this->db->commitTransaction();
            fclose($fh);

            echo "Finished sync - $objectName for table $objectTable".PHP_EOL.PHP_EOL;
        }

        // revert to original timezone
        date_default_timezone_set($originalTz);
    }


    /**
     * @param $tableName
     * @return array
     */
    protected function getExistingFields($tableName)
    {
        if ($this->db->tableExists($tableName))
        {
            return $this->db->getFields($tableName);
        }
        return array();
    }

    /**
     * @param $fieldsIndexedBySf
     * @param $existingFields
     */
    protected function getFieldsToQuery($fieldsIndexedBySf, $existingFields)
    {

        if(in_array('*', $fieldsIndexedBySf) || !sizeof($fieldsIndexedBySf)) {
            // add all fields from Salesforce
            return true;
        }
        elseif (is_array($existingFields)) {
            // some fields already exist, check for new ones

            $diff = array_diff($fieldsIndexedBySf, array_keys($existingFields));

            /*if(count($diff) > 0)
            {
                $objectsToQuery[$objectName] = $diff;
            }*/
            //var_dump($diff);exit();
            return count($diff) > 0 ? $diff : false;
        } elseif (!is_array($existingFields)) {
            // add all fields from config
            return $fieldsIndexedBySf;
        }
        return false;
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
        foreach($this->objectsAndFields as $objectName => $object) {

            $objectTable = $object->getTableName();
            $objectSource[$objectName] = $object;

            $fieldsIndexedBySF = $object->getFieldsSFTable();
            $fieldsIndexedByTable = array_flip($fieldsIndexedBySF);

            // get existing fields
            $existingFields[$objectName] = $this->getExistingFields($objectTable);

            // the below is true when there are no fields specified and therefore we use a *
            if (sizeof($existingFields[$objectName]) && !sizeof($fieldsIndexedBySF))
            {

                foreach ($existingFields[$objectName] as $field => $type) {
                    $fieldsIndexedBySF[$field] = $field;
                }
                $fieldsIndexedByTable = array_flip($fieldsIndexedBySF);
            }

            // this gets overwritten later for objects with *
            $this->fieldsToQuery[$objectName] = $fieldsIndexedBySF;

            $objectsToQuery[$objectName] = $this->getFieldsToQuery($fieldsIndexedBySF, $existingFields[$objectName]);
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

            //var_dump($describeBatches, array_keys($objectsToQuery));exit();

            foreach( $describeBatches as $describeBatch ) {
                // query SalesForce for each batch of tables
                $describeBatchResult = $this->sf->getSObjectFields($describeBatch);
                $describeResult = array_merge($describeResult, $describeBatchResult);

            }

            foreach ($describeResult as $objectName => $fields) {
                $objectName = strtolower($objectName);
                $object = $this->objectsAndFields[$objectName];

                $fieldsIndexedBySF = $object->getFieldsSFTable();
                $fieldsIndexedByTable = array_flip($fieldsIndexedBySF);

                $allFieldsToRetrieve = array_map(function($field) {return strtolower($field->name);}, $fields);
                $allFields = array();
                foreach($allFieldsToRetrieve as $key => $value) {
                    $allFields[$value] = $value;
                }

                $allFieldObjects = array();
                foreach ($fields as $field) {
                    $allFieldObjects[strtolower($field->name)] = $field;
                }

                $newFields = array();

                if (is_array($objectsToQuery[$objectName])) {
                    // check if any fields defined in config don't exist in Salesforce
                    $badFields = array_diff(array_flip($objectsToQuery[$objectName]), $allFields);
                    if(count($badFields) > 0)
                    {
                        throw new Exception("Invalid field(s): " . implode(', ', $badFields));
                    }


                    // get describe data for fields that need to be added
                    foreach ($allFields as $field) {
                        if (in_array($field, array_flip($objectsToQuery[$objectName])))
                        {
                            $newFields[] = $field;
                        }
                    }
                }
                elseif (array_key_exists($objectName, $objectsToQuery))
                {
                    // add all fields from Salesforce
                    //var_dump('here', $objectsToQuery[$objectName]);
                    if ($objectsToQuery[$objectName])
                    {
                        $this->fieldsToQuery[$objectName] = $allFields;

                        foreach ($allFields as $field) {
                            if (isset($fieldsIndexedBySF[$field]))
                            {
                                $newFields[] = $field;
                            }
                        }
                        //var_dump($objectName, $fieldsIndexedBySF);
                        if (!is_array($objectsToQuery[$objectName]) && $objectsToQuery[$objectName] == true)
                        {
                            $newFields = $allFields;
                            foreach ($allFields as $field) {
                                $fieldsIndexedBySF[$field] = $field;
                            }
                        }
                    }

                }

                //var_dump($newFields);exit();
                /*if ($objectName == 'directdeal__c')
                {
                    var_dump($newFields);exit();
                }*/
                // get mysql definition for each field
                $fieldDefs = array();
                foreach ($newFields as $field) {
                    $objectUpserts[$objectName][$fieldsIndexedBySF[$field]] = $this->createFieldDefinition($allFieldObjects[$field]);
                }
            }
            //var_dump($this->objectsAndFields);exit();
            //var_dump($objectUpserts);exit();
            // update and create tables
            foreach ($objectUpserts as $objectName => $fieldDefs) {
                $this->db->upsertObject($object->getTableName(), $fieldDefs);
            }

        }

        $this->schemaSynced = true;
    }

    protected function arrayFlip($array = array())
    {
        $newArray = array();
        foreach ($array as $key => $val) {
            $newArray[$val] = $key;
        }
        return $newArray;
    }

}

$startReplicating = new ReplicatorV2($argv);
$startReplicating->syncData();