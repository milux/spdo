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
        // Build string for insertion of one row
        $rowString = '(' . implode(', ', array_fill(0, $nCols, '?')) . ')';
        // Infer data types from first row
        $colTypes = SPDOConnection::getTypes(reset($rows));
        // Insert all rows, no more than maxInsertRows at a time
        $batchSize = count($rows);
        $rowOffset = 0;
        if ($batchSize > $this->maxInsertRows) {
            $stmt = $this->prepare($this->buildInsertSQL($table, $columnNames, $rowString, $this->maxInsertRows));
            while ($batchSize > $this->maxInsertRows) {
                // Concatenate all row arrays that are to be processed in this round
                $stmt->bindTyped(call_user_func_array('array_merge',
                    array_map('array_values', array_slice($rows, $rowOffset, $this->maxInsertRows))), $colTypes)
                    ->execute();
                $rowOffset += $this->maxInsertRows;
                $batchSize -= $this->maxInsertRows;
            }
        }
        // Handle the remaining rows
        return $this->prepare($this->buildInsertSQL($table, $columnNames, $rowString, $batchSize))
            // Concatenate all remaining arrays that are to be processed in this last round
            ->bindTyped(call_user_func_array('array_merge',
                array_map('array_values', array_slice($rows, $rowOffset))), $colTypes)
            ->execute();
    }

}