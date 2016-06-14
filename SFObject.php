<?php
/**
 * Created by PhpStorm.
 * User: WaiJe
 * Date: 6/13/2016
 * Time: 3:45 PM
 */
class SFObject {
    protected $objectName;
    protected $tableName;
    protected $fields = array();
    protected $fieldsSFTable = array();
    protected $fieldsTableSF = array();

    public function __construct($objectName, $tableName = null, $fields = array(), $defaultFields = array())
    {
        $this->objectName = strtolower($objectName);
        //$this->tableName = $tableName ? strtolower($tableName) : $this->objectName;
        $this->tableName = $tableName ? strtolower($tableName) : $this->objectName;
        $this->fields = $fields;

        foreach($fields as $field) {
            $this->fieldsSFTable[$field->getFieldName()] = $field->getRenamedField();
            $this->fieldsTableSF[$field->getRenamedField()] = $field->getFieldName();
        }
    }

    public function getFieldsSFTable()
    {
        return $this->fieldsSFTable;
    }

    public function getFieldsTableSF()
    {
        return $this->fieldsTableSF;
    }

    public function translateSFField($sfField)
    {
        return isset($this->fieldsSFTable[$sfField]) ? $this->fieldsSFTable[$sfField] : null;
    }

    public function translateTableField($tableField)
    {
        return isset($this->fieldsTableSF[$tableField]) ? $this->fieldsTableSF[$tableField] : null;
    }

    public function getObjectName()
    {
        return $this->objectName;
    }

    public function getTableName()
    {
        return $this->tableName;
    }

    public function getFields()
    {
        return $this->fields;
    }
}

class SFObjectFields {
    protected $fieldName;
    protected $renamedField;

    public function __construct($fieldName, $renamedField)
    {
        $this->fieldName = strtolower($fieldName);
        $this->renamedField = $renamedField ? strtolower($renamedField) : strtolower($fieldName);
    }

    public function getFieldName()
    {
        return $this->fieldName;
    }

    public function getRenamedField()
    {
        return $this->renamedField;
    }
}