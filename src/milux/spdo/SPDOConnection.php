<?php
/**
 * Class for easy DB operations on a database connection
 *
 * @author Michael Lux <michi.lux@gmail.com>
 * @copyright Copyright (c) 2017 Michael Lux
 * @license GNU/GPLv3
 */

namespace milux\spdo;

class SPDOConnection {

    protected static $typeMap = [
        'boolean' => \PDO::PARAM_BOOL,
        'integer' => \PDO::PARAM_INT,
        'double' => \PDO::PARAM_STR,
        'string' => \PDO::PARAM_STR,
        'NULL' => \PDO::PARAM_NULL
    ];

    /**
     * Returns an array of PDO types for all elements of the given array
     *
     * @param array $values The values to check for types
     *
     * @return array The PDO data types of the values
     */
    public static function getTypes(array $values) {
        $typeMap = self::$typeMap;
        return array_map(function ($v) use ($typeMap) {
            $type = gettype($v);
            return isset($typeMap[$type]) ? $typeMap[$type] : \PDO::PARAM_STR;
        }, array_values($values));
    }

    /**
     * @var \PDO the PDO object which is encapsulated by this decorator
     */
    protected $pdo = null;
    /**
     * @var SPDOConfig the configuration object for this connection
     */
    protected $configObject = null;
    //whether to enable insert id fetching
    protected $insertIDs = false;

    /**
     * SPDOConnection constructor
     *
     * @param SPDOConfig $configObject the configuration object for this SPDOConnection
     * @param array $options Initial options for the new connection, defaults are:
     * MYSQL_ATTR_INIT_COMMAND - 'SET NAMES utf8'
     * ATTR_PERSISTENT - true
     */
    public function __construct(SPDOConfig $configObject, array $options = []) {
        $this->configObject = $configObject;
        //initialize internal PDO object
        $this->pdo = new \PDO(
            'mysql:host=' . $configObject->getHost() . ';dbname=' . $configObject->getSchema(),
            $configObject->getUser(),
            $configObject->getPassword(),
            $options + [
                \PDO::MYSQL_ATTR_INIT_COMMAND => 'SET NAMES utf8',
                \PDO::ATTR_PERSISTENT => true
            ]
        );
        $this->pdo->setAttribute(\PDO::ATTR_ERRMODE, \PDO::ERRMODE_EXCEPTION);
    }

    /**
     * Returns whether the returning of insert IDs by insert() and batchInsert() is enabled
     *
     * @return bool Whether insert ID returning is enabled for this connection
     */
    public function getReturnInsertIds() {
        return $this->insertIDs;
    }

    /**
     * Controls the returning of insert IDs by insert() and batchInsert()
     *
     * @param bool $insertIds Whether insert ID(s) will be returned
     */
    public function setReturnInsertIds($insertIds = true) {
        $this->insertIDs = $insertIds;
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
    public function ta(callable $function, $level = null) {
        try {
            $this->begin($level);
            $result = $function();
            $this->commit();
            return $result;
        } catch (\Exception $e) {
            $this->abort();
            throw $e;
        }
    }

    /**
     * Shortcut for PDO::beginTransaction();
     *
     * @param string $level The explicit transaction isolation level
     *
     * @return bool success of transaction command
     * @throws SPDOException On SQL error
     */
    public function begin($level = null) {
        $res = $this->pdo->beginTransaction();
        if (isset($level)) {
            $this->query('SET TRANSACTION ISOLATION LEVEL ' . $level);
        }
        return $res;
    }

    /**
     * PDO::commit();
     *
     * @return bool success of transaction command
     */
    public function commit() {
        return $this->pdo->commit();
    }

    /**
     * shortcut for PDO::rollBack();
     *
     * @return bool success of transaction command
     */
    public function abort() {
        return $this->pdo->rollBack();
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
    public function save($table, array $keyColumnMap, array $dataColumnMap = []) {
        //assemble WHERE clause from $keyColumnMap
        $whereClause = implode(' AND ', array_map(function ($c) {
            return $c . ' = ?';
        }, array_keys($keyColumnMap)));
        //extract values from keyColumnMap for WHERE parametrization
        $whereParams = array_values($keyColumnMap);
        //check if row with specified key values exists
        $checkValue = $this->prepare('SELECT COUNT(*) FROM ' . $table . ' WHERE ' . $whereClause)
            ->execute($whereParams)->cell();
        if ($checkValue === '0') {
            //no row(s) found, perform insert with combined map
            return $this->insert($table, $dataColumnMap + $keyColumnMap);
        } else if (!empty($dataColumnMap)) {
            //row(s) found, perform update
            return $this->update($table, $dataColumnMap, $whereClause, $whereParams);
        } else {
            return null;
        }
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
    public function update($table, array $columnValueMap, $whereStmt = null, array $whereParams = []) {
        //assemble set instructions
        $setInstructions = array_map(function ($c) {
            return $c . ' = ?';
        }, array_keys($columnValueMap));
        //"isolate" parameter values
        $params = array_values($columnValueMap);
        //assemble UPDATE sql query
        $sql = 'UPDATE ' . $table . ' SET ' . implode(', ', $setInstructions);
        //append WHERE query, if neccessary
        if (isset($whereStmt)) {
            $sql .= ' WHERE ' . $whereStmt;
            //append WHERE parameters to parameter array
            $params = array_merge($params, array_values($whereParams));
        }
        //prepare, bind values and execute the UPDATE
        return $this->prepare($sql)->bindTyped($params)->execute();
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
    public function insert($table, array $columnValueMap, $insertIdName = null) {
        //prepare, bind values and execute the INSERT
        $stmt = $this->prepare('INSERT INTO ' . $table
            . ' (' . implode(', ', array_keys($columnValueMap)) . ') '
            . 'VALUES (' . implode(', ', array_fill(0, count($columnValueMap), '?')) . ')')
            ->bindTyped($columnValueMap)->execute();
        //return execution result
        return $this->getReturnInsertIds() ? $this->pdo->lastInsertId($insertIdName) : $stmt;
    }

    /**
     * Performs multiple INSERTS into specified columns<br />
     * NOTE: non-array entries in parameter 2 ($columnValuesMap)
     * are automatically expanded to arrays of suitable length!
     *
     * @param string $table Name of the INSERT target table
     * @param array $columnValuesMap Map of the form "column => array(values)" or "column => value"
     *
     * @return SPDOStatement The statement object used for the INSERTs
     * @throws SPDOException On SQL error or in case of malformed $columnValuesMap
     */
    public function batchInsert($table, array $columnValuesMap) {
        //pre-checks of size
        $batchSize = 0;
        foreach ($columnValuesMap as $a) {
            if (is_array($a)) {
                if ($batchSize === 0) {
                    $batchSize = count($a);
                } else {
                    if ($batchSize !== count($a)) {
                        throw new SPDOException('SPDOConnection::batchInsert() called with arrays of unequal length');
                    }
                }
            }
        }
        if ($batchSize === 0) {
            throw new SPDOException('No array was found in $columnValuesMap passed to SPDOConnection::batchInsert()');
        }
        //expand non-array values to arrays of appropriate size
        $columns = array_map(function ($a) use ($batchSize) {
            if (is_array($a)) {
                return array_values($a);
            } else {
                return array_fill(0, $batchSize, $a);
            }
        }, $columnValuesMap);
        // Transpose columns to rows
        array_unshift($columns, null);
        $rows = call_user_func_array('array_map', $columns);
        // Call batchInsertRows()
        return $this->batchInsertRows($table, array_keys($columnValuesMap), $rows, false);
    }

    /**
     * INSERTs multiple rows into given columns
     *
     * @param string $table Name of the INSERT target table
     * @param array $columnNames Array of columns names
     * @param array $rows Array of row arrays
     * @param bool $lengthCheck Whether to check the length of passed row arrays
     *
     * @return SPDOStatement The statement object used for the INSERTs
     * @throws SPDOException On SQL error or in case of malformed $columnValuesMap
     */
    public function batchInsertRows($table, array $columnNames, array $rows, $lengthCheck = true) {
        //pre-checks of size
        $nCols = count($columnNames);
        if ($lengthCheck) {
            foreach ($rows as $a) {
                if ($nCols !== count($a)) {
                    throw new SPDOException('SPDOConnection::batchInsertRows() called with rows of unequal length');
                }
            }
        }
        //construct and prepare insert statement
        $stmt = $this->prepare('INSERT INTO ' . $table
            . ' (' . implode(', ', $columnNames) . ') '
            . 'VALUES (' . implode(', ', array_fill(0, $nCols, '?')) . ')');
        //get sample data types by applying reset() to first array
        $types = self::getTypes(reset($rows));
        $batchSize = count($rows);
        for ($i = 0; $i < $batchSize; $i++) {
            $bindCounter = 1;
            //bind all values
            foreach ($rows[$i] as $cell) {
                $stmt->bindValue($bindCounter, $cell, $types[$bindCounter]);
                $bindCounter++;
            }
            //execute insert
            $stmt->execute();
        }
        //return statement
        return $stmt;
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
    public function delete($table, $whereClause = null, array $whereParams = []) {
        $sql = 'DELETE FROM ' . $table;
        if (isset($whereClause)) {
            $sql .= ' WHERE ' . $whereClause;
        }
        return $this->prepare($sql)->execute($whereParams);
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
    public function count($table, $whereClause = null, array $whereParams = []) {
        $sql = 'SELECT COUNT(*) FROM ' . $table;
        if (isset($whereClause)) {
            $sql .= ' WHERE ' . $whereClause;
        }
        return (int) $this->prepare($sql)->execute($whereParams)->cell();
    }

    /**
     * PDO::query() on common PDO object
     *
     * @param string $sql
     *
     * @return SPDOStatement
     * @throws SPDOException On SQL error
     */
    public function query($sql) {
        try {
            return $this->configObject->newSPDOStatement($this->pdo->query($this->configObject->preProcess($sql)));
        } catch (\PDOException $e) {
            throw new SPDOException($e);
        }
    }

    /**
     * PDO::exec() on common PDO object
     *
     * @param string $sql
     *
     * @return int number of processed lines
     * @throws SPDOException On SQL error
     */
    public function exec($sql) {
        try {
            return $this->pdo->exec($this->configObject->preProcess($sql));
        } catch (\PDOException $e) {
            throw new SPDOException($e);
        }
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
    public function prepare($sql, array $driver_options = []) {
        try {
            return $this->configObject->newSPDOStatement($this->pdo->prepare($this->configObject->preProcess($sql), $driver_options));
        } catch (\PDOException $e) {
            throw new SPDOException($e);
        }
    }

    /**
     * Obtain the last insert ID, for a certain object or in general
     *
     * @param string $insertIdName The name of the column or DB object that is auto-incremented
     *
     * @return int The last insert ID
     */
    public function lastInsertId($insertIdName = null) {
        return $this->pdo->lastInsertId($insertIdName);
    }

}