<?php
/**
 * Trait providing a more efficient batchInsert() method.
 * The database must support "INSERT ON DUPLICATE KEY UPDATE".
 *
 * @author Michael Lux <michi.lux@gmail.com>
 * @copyright Copyright (c) 2017 Michael Lux
 * @license GNU/GPLv3
 */

namespace milux\spdo\traits;


use milux\spdo\SPDOConnection;
use milux\spdo\SPDOException;
use milux\spdo\SPDOStatement;

trait MultiRowInsertSupport {

    /**
     * Returns whether the returning of insert IDs by insert() and batchInsert() is enabled
     *
     * @return bool Whether insert ID returning is enabled for this connection
     */
    public abstract function getReturnInsertIds();

    /**
     * PDO::prepare() on common PDO object
     *
     * @param string $sql
     * @param array $driver_options
     *
     * @return SPDOStatement prepared statement
     * @throws SPDOException
     */
    public abstract function prepare($sql, array $driver_options = []);

    /**
     * @var int Maximum number of rows combine for one INSERT,
     * defaults to 1000
     */
    private $maxInsertRows = 1000;

    /**
     * @param int $maxRows New maximum number of rows combine for one INSERT,
     * defaults to 1000
     */
    public function setMaxInsertRows($maxRows) {
        $this->maxInsertRows = $maxRows;
    }

    private function buildInsertSQL($table, $columnNames, $rowString, $nRows) {
        return 'INSERT INTO ' . $table . ' (' . implode(', ', $columnNames) . ') '
            . 'VALUES ' . implode(', ', array_fill(0, $nRows, $rowString));
    }

    /**
     * Do multiple INSERTS into specified columns<br />
     * NOTE: non-array entries in parameter 2 ($columnValuesMap)
     * are automatically expanded to arrays of suitable length!
     *
     * @param string $table name of the INSERT target table
     * @param array $columnValuesMap map of the form "column => array(values)" or "column => value"
     *
     * @throws SPDOException in case of malformed $columnValuesMap
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
        } else {
            //expand non-array values to arrays of appropriate size
            foreach ($columnValuesMap as &$a) {
                if (!is_array($a)) {
                    $a = array_fill(0, $batchSize, $a);
                }
            }
        }
        // Build string for insertion of one row
        $rowString = '(' . implode(', ', array_fill(0, count($columnValuesMap), '?')) . ')';
        $columnNames = array_keys($columnValuesMap);
        // Transpose parameter matrix
        array_unshift($columnValuesMap, null);
        $paramRows = call_user_func_array('array_map', array_values($columnValuesMap));

        $rowOffset = 0;
        if ($batchSize > $this->maxInsertRows) {
            $stmt = $this->prepare($this->buildInsertSQL($table, $columnNames, $rowString, $this->maxInsertRows));
            while ($batchSize > $this->maxInsertRows) {
                // Concatenate all row arrays that are to be processed in this round
                $stmt->bindTyped(call_user_func_array('array_merge',
                    array_map('array_values', array_slice($paramRows, $rowOffset, $this->maxInsertRows))))
                    ->execute();
                $rowOffset += $this->maxInsertRows;
                $batchSize -= $this->maxInsertRows;
            }
        }
        // Handle the remaining rows
        $this->prepare($this->buildInsertSQL($table, $columnNames, $rowString, $batchSize))
            // Concatenate all remaining arrays that are to be processed in this last round
            ->bindTyped(call_user_func_array('array_merge',
                array_map('array_values', array_slice($paramRows, $rowOffset, $this->maxInsertRows))))
            ->execute();
    }

}