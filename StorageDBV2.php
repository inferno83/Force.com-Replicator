<?php

require_once 'StorageDB.php';

class StorageDBV2 extends StorageDB {
	/**
	 *
     */

    function __construct($host, $user, $pass, $database, $batchInsertSize=150000) {

        $this->db = new PDO("mysql:host=$host;dbname=$database", $user, $pass);
        $this->db->setAttribute(PDO::ATTR_ERRMODE, PDO::ERRMODE_EXCEPTION);

        $this->batchInsertSize = $batchInsertSize;
        $this->transactionDepth = 0;

        if(!$this->isInitialised())
            $this->initialise();
    }
    /**
     * @return array
     */
    public function getSyncHistoryFields()
    {
        return array(
            'id' => 'INT NOT NULL AUTO_INCREMENT',
            'object_name' => 'VARCHAR(45) NULL',
            'sync_time' => 'DATETIME NULL',
            'start_time' => 'DATETIME',
            'end_time' => 'DATETIME',
            'total_time_in_minutes' => 'FLOAT',
            'action' => 'VARCHAR(31)'
        );
    }

    /**
     * @return bool
     */
    protected function isInitialised() {
        $isInitialized = true;
        $currentFields = $this->getSyncHistoryFields();
        if (!$this->tableExists($this::SYNC_HISTORY_TABLE))
        {
            $isInitialized = false;
        }
        elseif ($this->tableExists($this::SYNC_HISTORY_TABLE))
        {
            // find the fields
            $fields = $this->getFields($this::SYNC_HISTORY_TABLE);
            foreach ($currentFields as $field => $type) {
                if (!$isInitialized)
                {
                    break;
                }
                if (!isset($fields[$field]))
                {
                    $isInitialized = false;
                }
            }
        }
        return $isInitialized;
    }

    /**
     *
     */
    protected function initialise() {

        $tableExists = $this->tableExists($this::SYNC_HISTORY_TABLE);
        $defaultFields = $this->getSyncHistoryFields();
        $fieldsToAdd = array();

        if ($tableExists)
        {
            $fields = $this->getFields($this::SYNC_HISTORY_TABLE);
            foreach ($fields as $field => $type) {
                if (isset($defaultFields[$field]))
                {
                    unset($defaultFields[$field]);
                }
            }
            // now add the fields
            foreach ($defaultFields as $defaultField => $type) {
                $this->db->query(
                    'ALTER TABLE ' . $this::SYNC_HISTORY_TABLE .
                    ' ADD ' . $defaultField . ' ' . $type
                );
            }
        }
        elseif (!$tableExists)
        {
            $this->db->query('DROP TABLE IF EXISTS ' . $this::SYNC_HISTORY_TABLE);
            foreach($defaultFields as $defaultField => $type) {
                $fieldsToAdd[] = $defaultField . ' ' . $type;
            }
            $this->db->query(
                'CREATE TABLE ' . $this::SYNC_HISTORY_TABLE .
                ' (' . implode(',', $fieldsToAdd) . ', PRIMARY KEY(id))'
            );
        }
    }

    /**
     *
     */
    public function wipeHistoryTable()
	{
		$this->db->query("TRUNCATE TABLE " . StorageDB::SYNC_HISTORY_TABLE);
	}

	/**
	 * @param $table
     */
	public function removeFromHistoryTable($table)
	{
		$tableToRefresh = $this->db->quote($table);
		$this->db->query(
			"DELETE FROM " . StorageDB::SYNC_HISTORY_TABLE . " WHERE object_name = " . $tableToRefresh
		);

	}
}
