<?php

require_once 'StorageDB.php';

class StorageDBV2 extends StorageDB {
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
