<?php
/**
 * Trait providing a more efficient save() method.
 * The database must support "INSERT ON DUPLICATE KEY UPDATE".
 *
 * @author Michael Lux <michi.lux@gmail.com>
 * @copyright Copyright (c) 2017 Michael Lux
 * @license GNU/GPLv3
 */

namespace milux\spdo\traits;


use milux\spdo\SPDOException;
use milux\spdo\SPDOStatement;

trait OnDuplicateKeyUpdateSupport {

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
     * This function automatically inserts/updates data depending on a set of key columns/values.
     * If one or more row(s) with certain values in certain columns as specified by $keyColumnMap
     * exist in $table, the data of $dataColumnMap is UPDATEd to the values of the latter.
     * Otherwise, $keyColumnMap and $dataColumnMap are combined and INSERTed into $table.
     * If $dataColumnMap is omitted, this function has a "INSERT-if-not-exists" behaviour.
     *
     * @param string $table name of the table to update or insert into
     * @param array $keyColumnMap column-value-map for key columns to test
     * @param array $dataColumnMap [optional] column-value-map for non-key columns
     * @return int|null|SPDOStatement
     * @throws SPDOException on SQL error
     */
    public function save($table, array $keyColumnMap, array $dataColumnMap = []) {
        // combined INSERT map
        $columnValueMap = $dataColumnMap + $keyColumnMap;
        // alternative UPDATE instructions
        $setInstructions = array_map(function ($c) {
            return $c . ' = VALUES(' . $c . ')';
        }, array_keys($dataColumnMap));
        // prepare, bind values and execute the INSERT
        return $this->prepare('INSERT INTO ' . $table
            . ' (' . implode(', ', array_keys($columnValueMap)) . ') '
            . 'VALUES (' . implode(', ', array_fill(0, count($columnValueMap), '?')) . ') '
            . 'ON DUPLICATE KEY UPDATE ' . implode(', ', $setInstructions))
            ->bindTyped($columnValueMap)->execute();
    }

}