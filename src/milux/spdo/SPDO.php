<?php
/**
 * Class for easy DB operations on a shared SPDOConnection
 *
 * @author Michael Lux <michi.lux@gmail.com>
 * @copyright Copyright (c) 2017 Michael Lux
 * @license GNU/GPLv3
 */

namespace milux\spdo;

class SPDO {

    const LEVEL_READ_UNCOMMITTED = 'READ UNCOMMITTED';
    const LEVEL_READ_COMMITTED = 'READ COMMITTED';
    const LEVEL_REPEATABLE_READ = 'REPEATABLE READ';
    const LEVEL_SERIALIZABLE = 'SERIALIZABLE';

    /**
     * @var SPDOConnection shared database connection
     */
    private static $instance = null;

    /**
     * @var SPDOConfig Configuration object
     */
    private static $config = null;

    /**
     * Set the SPDOConfig instance that is used to get the database connection configuration
     *
     * @param SPDOConfig $config A configuration object
     */
    public static function setConfig(SPDOConfig $config) {
        self::$config = $config;
    }

    /**
     * Get the SPDOConfig instance that is currently in use
     *
     * @return SPDOConfig The configuration object
     */
    public static function getConfig() {
        return self::$config;
    }

    /**
     * Creates/returns the shared DB connection
     *
     * @return SPDOConnection the new DB connection
     * @throws SPDOException On initialization error
     */
    public static function getInstance() {
        if (self::$instance === null) {
            if (self::$config === null) {
                throw new SPDOException('No configuration provided, call setConfigClass() first!');
            }
            if (!self::$config instanceof SPDOConfig) {
                throw new SPDOException(self::$config . ' is not an instance of SPDOConfig!');
            }
            self::$instance = self::$config->newSPDOConnection();
        }
        return self::$instance;
    }

    /**
     * Returns whether the returning of insert IDs by insert() and batchInsert() is enabled
     *
     * @return bool Whether insert ID returning is enabled for this connection
     * @throws SPDOException On initialization error
     */
    public function getReturnInsertIds() {
        return self::getInstance()->getReturnInsertIds();
    }

    /**
     * Controls the returning of insert IDs by insert() and batchInsert()
     *
     * @param bool $insertIds Whether insert ID(s) will be returned
     * @throws SPDOException On initialization error
     */
    public function setReturnInsertIds($insertIds = true) {
        self::getInstance()->setReturnInsertIds($insertIds);
    }

    /**
     * Helper to perform some function as transaction.
     *
     * @param callable $function The function to execute as DB transaction
     * @param string $level The explicit transaction isolation level
     *
     * @return mixed Function return value
     * @throws \Exception Re-thrown Exception if transaction fails
     */
    public static function ta(callable $function, $level = null) {
        return self::getInstance()->ta($function, $level);
    }

    /**
     * Shortcut for PDO::beginTransaction();
     *
     * @param string $level The explicit transaction isolation level
     *
     * @return bool success of transaction command
     * @throws SPDOException On SQL error
     */
    public static function begin($level = null) {
        return self::getInstance()->begin($level);
    }

    /**
     * PDO::commit();
     *
     * @return bool success of transaction command
     * @throws SPDOException On initialization error
     */
    public static function commit() {
        return self::getInstance()->commit();
    }

    /**
     * shortcut for PDO::rollBack();
     *
     * @return bool success of transaction command
     * @throws SPDOException On initialization error
     */
    public static function abort() {
        return self::getInstance()->abort();
    }

    /**
     * This function automatically inserts/updates data depending on a set of key columns/values.
     * If one or more row(s) with certain values in certain columns as specified by $keyColumnMap
     * exist in $table, the data of $dataColumnMap is UPDATEd to the values of the latter.
     * Otherwise, $keyColumnMap and $dataColumnMap are combined and INSERTed into $table.
     * If $dataColumnMap is omitted, this function has a "INSERT-if-not-exists" behaviour.
     *
     * @param string $table name of the table to update or insert into
     * @param array $keyColumnMap column-value-map for key columns to test
     * @param array $dataColumnMap [optional] column-value-map for non-key columns
     *
     * @return int|null|SPDOStatement
     * @throws SPDOException On SQL error
     */
    public static function save($table, array $keyColumnMap, array $dataColumnMap = []) {
        return self::getInstance()->save($table, $keyColumnMap, $dataColumnMap);
    }

    /**
     * constructs and performs an UPDATE query on a given table
     *
     * @param string $table name of the table to update
     * @param array $columnValueMap map of column names (keys) and values to set
     * @param string $whereStmt an optional WHERE statement for the update, parameters MUST be bound with &quot;?&quot;
     * @param array $whereParams optional parameters to be passed for the WHERE statement
     *
     * @return SPDOStatement the result statement of the UPDATE query
     * @throws SPDOException On SQL error
     */
    public static function update($table, $columnValueMap, $whereStmt = null, array $whereParams = []) {
        return self::getInstance()->update($table, $columnValueMap, $whereStmt, $whereParams);
    }

    /**
     * Constructs and performs an INSERT query on a given table
     *
     * @param string $table Name of the table to update
     * @param array $columnValueMap Map of column names (keys) and values to insert
     * @param string $insertIdName The name of the column or DB object that is auto-incremented
     *
     * @return int|SPDOStatement Depending on the state of this SPDOConnection instance,
     * an insert ID or the statement object of the performed INSERT is returned
     * @throws SPDOException On SQL error
     */
    public static function insert($table, array $columnValueMap) {
        return self::getInstance()->insert($table, $columnValueMap);
    }

    /**
     * Performs multiple INSERTS into specified columns<br />
     * NOTE: non-array entries in parameter 2 ($columnValuesMap)
     * are automatically expanded to arrays of suitable length!
     *
     * @param string $table name of the INSERT target table
     * @param array $columnValuesMap map of the form "column => array(values)" or "column => value"
     *
     * @return SPDOStatement The statement object used for the INSERTs
     * @throws SPDOException On SQL error or in case of malformed $columnValuesMap
     */
    public static function batchInsert($table, array $columnValuesMap) {
        return self::getInstance()->batchInsert($table, $columnValuesMap);
    }

    /**
     * constructs and performs a DELETE query on a given table
     *
     * @param string $table name of the table to DELETE from
     * @param string $whereClause the WHERE clause of the query
     * @param array $whereParams the parameters for the WHERE query
     *
     * @return SPDOStatement
     * @throws SPDOException On SQL error
     */
    public static function delete($table, $whereClause = null, array $whereParams = []) {
        return self::getInstance()->delete($table, $whereClause, $whereParams);
    }

    /**
     * Counts the rows in a given table, optionally filtered by a WHERE clause
     *
     * @param string $table Name of the table to DELETE from
     * @param string $whereClause The WHERE clause of the query
     * @param array $whereParams The parameters for the WHERE query
     *
     * @return int The number of counted rows
     * @throws SPDOException On SQL error
     */
    public static function count($table, $whereClause = null, array $whereParams = []) {
        return self::getInstance()->count($table, $whereClause, $whereParams);
    }

    /**
     * PDO::query() on common PDO object
     *
     * @param string $sql
     *
     * @return SPDOStatement
     * @throws SPDOException On SQL error
     */
    public static function query($sql) {
        return self::getInstance()->query($sql);
    }

    /**
     * PDO::exec() on common PDO object
     *
     * @param string $sql
     *
     * @return int number of processed lines
     * @throws SPDOException On SQL error
     */
    public static function exec($sql) {
        return self::getInstance()->exec($sql);
    }

    /**
     * PDO::prepare() on common PDO object
     *
     * @param string $sql
     * @param array $driver_options
     *
     * @return SPDOStatement prepared statement
     * @throws SPDOException On SQL error
     */
    public static function prepare($sql, array $driver_options = []) {
        return self::getInstance()->prepare($sql, $driver_options);
    }

    /**
     * Obtain the last insert ID, for a certain object or in general
     *
     * @param string $insertIdName The name of the column or DB object that is auto-incremented
     *
     * @return int The last insert ID
     * @throws SPDOException On initialization error
     */
    public static function lastInsertId($insertIdName = null) {
        return self::getInstance()->lastInsertId($insertIdName);
    }

}
