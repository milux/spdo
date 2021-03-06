<?php
/**
 * Class representing parepared statements or results.
 *
 * @author Michael Lux <michi.lux@gmail.com>
 * @copyright Copyright (c) 2017 Michael Lux
 * @license GNU/GPLv3
 */

namespace milux\spdo;

class SPDOStatement {

    /**
     * @var \PDOStatement
     */
    protected $statement;
    // Nesting level (groups) for advanced data handling
    protected $nesting = 0;
    // Data structures for advanced data handling
    protected $availableColumns = null;
    protected $data = null;
    // Marker for call to transform()
    protected $transformed = false;
    // Buffer and index for iterating over columns of a line
    protected $line = null;
    protected $nCell = 0;

    public function __construct($statement) {
        $this->statement = $statement;
    }

    /**
     * Returns the nested \PDOStatement, must be called before and data-retrieving method
     * After calling this method, the SPDOStatement becomes invalid and must not be used anymore
     *
     * @return \PDOStatement
     * @throws SPDOException
     */
    public function toPDOStatement() {
        if (isset($this->data)) {
            throw new SPDOException('Cannot return PDOStatement, this SPDOStatement has already been initialized!');
        }
        $statement = $this->statement;
        $this->statement = null;
        return $statement;
    }

    /**
     * Helper function to bind an array of values to this statement
     *
     * @param array $toBind Parameters to bind
     * @param array $types [optional] Known PDO types for binding, eliminates type inference
     * Size of $toBind can be a multiple of the size of $types, types will be assigned in rotating fashion
     *
     * @return SPDOStatement
     */
    public function bindTyped(array $toBind, $types = null) {
        $bindCount = 0;
        if (!isset($types)) {
            $types = SPDOConnection::getTypes($toBind);
        }
        $aKeys = array_keys($toBind);
        if ($aKeys === array_keys($aKeys)) {
            $typesLen = count($types);
            foreach ($toBind as $v) {
                $type = $types[$bindCount % $typesLen];
                $this->bindValue(++$bindCount, $v, $type);
            }
        } else {
            foreach ($toBind as $k => $v) {
                $type = $types[$k];
                if ($k[0] !== ':') {
                    $k = ':' . $k;
                }
                $this->bindValue($k, $v, $type);
            }
        }
        return $this;
    }

    /**
     * modified execute() which returns the underlying PDOStatement object on success,
     * thus making the execute command "chainable"
     *
     * @param mixed $argument [optional] might be an array or
     * the first of an arbitrary number of parameters for binding
     * @return SPDOStatement $this
     * @throws SPDOException
     */
    public function execute($argument = null) {
        if (isset($this->data)) {
            //reset the statement object if necessary
            $this->nesting = 0;
            $this->availableColumns = null;
            $this->data = null;
            $this->transformed = false;
            $this->line = null;
        }
        try {
            if (!isset($argument)) {
                $this->statement->execute();
            } elseif (is_array($argument)) {
                $this->statement->execute($argument);
            } else {
                $this->statement->execute(func_get_args());
            }
            return $this;
        } catch (\PDOException $e) {
            throw new SPDOException($e);
        }
    }

    /**
     * Returns the number of rows affected by the last execution
     *
     * @return int
     */
    public function rowCount() {
        return $this->statement->rowCount();
    }

    /**
     * Ensures that data is available for processing
     */
    public function init() {
        if (!isset($this->data)) {
            //fetch data for further processing
            $this->data = $this->statement->fetchAll(\PDO::FETCH_ASSOC);
            //check for empty result
            if (!empty($this->data)) {
                //save column names as keys and values of lookup array
                $this->availableColumns = array_combine(array_keys($this->data[0]), array_keys($this->data[0]));
            }
        }
    }

    /**
     * Helper function to immerse into the nested structure until data dimension after group()
     *
     * @param callback $callback the callback to apply at the innermost dimension
     * @param int $level [optional] the levels to immerse before the callback is applied,
     * defaults to the number of previous group operations
     * (e.g. the total number of array elements passed to group())
     * @return array modified copy of the internal data structure
     */
    public function immerse($callback, $level = null) {
        $this->init();
        //check for empty result
        if (empty($this->data)) {
            return $this->data;
        }
        if (!isset($level)) {
            $level = $this->nesting;
        }
        //recursive immersion closure
        $immerse = function ($data, $callback, $level) use (&$immerse) {
            if ($level === 0) {
                /** @noinspection PhpParamsInspection */
                return $callback($data);
            } else {
                foreach ($data as &$d) {
                    $d = $immerse($d, $callback, $level - 1);
                }
                return $data;
            }
        };
        return $immerse($this->data, $callback, $level);
    }

    /**
     * Groups data into subarrays by given column name(s),
     * generating nested map (array) structures.
     *
     * @param array $groups
     * @return SPDOStatement $this
     * @throws SPDOException
     */
    public function group(array $groups) {
        $this->init();
        //check for empty result
        if (empty($this->data)) {
            return $this;
        }
        if ($this->statement->columnCount() <= $this->nesting + count($groups)) {
            throw new SPDOException('Cannot do more than ' . ($this->statement->columnCount() - 1)
                . ' group operations for ' . $this->statement->columnCount() . ' columns.'
                . ' Use getUnique() or immerse() with custom callback retrieve flat structure!');
        }
        if ($this->transformed) {
            throw new SPDOException('Cannot safely group transformed elements, transform() must be called after group!');
        }
        $cols = $this->availableColumns;
        foreach ($groups as $g) {
            if (!isset($cols[$g])) {
                throw new SPDOException('Grouping column ' . $g . ' not available!');
            } else {
                unset($cols[$g]);
            }
        }
        $this->data = $this->immerse(function ($data) use ($groups) {
            //recursive closure for grouping
            $groupClosure = function ($data, array $groups) use (&$groupClosure) {
                $group = array_shift($groups);
                $result = [];
                foreach ($data as $rec) {
                    if (!isset($rec[$group])) {
                        throw new SPDOException($group . ': ' . json_encode($rec));
                    }
                    $key = $rec[$group];
                    if (!isset($result[$key])) {
                        $result[$key] = [];
                    }
                    unset($rec[$group]);
                    $result[$key][] = $rec;
                }
                //recursion: direcly iterate over the grouped maps with further groups
                if (!empty($groups)) {
                    foreach ($result as &$d) {
                        $d = $groupClosure($d, $groups);
                    }
                }
                return $result;
            };
            return $groupClosure($data, $groups);
        });
        //correct available columns after grouping
        $this->availableColumns = $cols;
        //increase nesting level
        $this->nesting += count($groups);
        //return $this for method chaining
        return $this;
    }

    public function filter($callback) {
        $this->init();
        //check for empty result
        if (empty($this->data)) {
            return $this;
        }
        $this->data = $this->immerse(function ($data) use ($callback) {
            return array_values(array_filter($data, $callback));
        });
        return $this;
    }

    /**
     * Sets the PHP data type of specified columns
     *
     * @param array $typeMap map of column names (keys) and types to set (values)
     * @return SPDOStatement $this
     * @throws SPDOException
     */
    public function cast($typeMap) {
        $this->init();
        //check for empty result
        if (empty($this->data)) {
            return $this;
        }
        foreach (array_keys($typeMap) as $c) {
            if (!isset($this->availableColumns[$c])) {
                throw new SPDOException('Casting column ' . $c . ' not available!');
            }
        }
        $this->data = $this->immerse(function ($data) use ($typeMap) {
            foreach ($data as &$d) {
                foreach ($typeMap as $c => $t) {
                    settype($d[$c], $t);
                }
            }
            return $data;
        });
        return $this;
    }

    /**
     * Applies callbacks to specified columns<br />
     * ATTENTION: Modifying a column to a non-primitive type and using it for grouping,
     * reducing, etc. can cause undefined behaviour!
     *
     * @param array $callbackMap map of column names (keys) and callbacks to apply (values)
     * @return SPDOStatement $this
     * @throws SPDOException
     */
    public function mod($callbackMap) {
        $this->init();
        //check for empty result
        if (empty($this->data)) {
            return $this;
        }
        foreach (array_keys($callbackMap) as $c) {
            if (!isset($this->availableColumns[$c])) {
                throw new SPDOException('Casting column ' . $c . ' not available!');
            }
        }
        $this->data = $this->immerse(function ($data) use ($callbackMap) {
            foreach ($data as &$d) {
                foreach ($callbackMap as $co => $cb) {
                    $d[$co] = call_user_func($cb, $d[$co]);
                }
            }
            return $data;
        });
        return $this;
    }

    /**
     * Tranforms the innermost dimension elements (initially maps)
     * by tranforming them with the given callback function.<br />
     * ATTENTION: group(), getObjects() and getUnique(true) cannot be used after this operation!
     *
     * @param callback $callback callback accepting exactly one element
     * @return SPDOStatement $this
     */
    public function transform($callback) {
        $this->transformed = true;
        $this->data = $this->immerse(function ($data) use ($callback) {
            foreach ($data as &$d) {
                $d = $callback($d);
            }
            return $data;
        });
        return $this;
    }

    /**
     * Get next cell of data set
     *
     * @param bool $reset setting this parameter to true will reset the array pointers
     * (required for first call)
     * @return mixed
     * @throws SPDOException
     */
    public function cell($reset = false) {
        if ($this->nesting > 0) {
            throw new SPDOException('Cannot iterate cells after group()!');
        }
        if ($this->transformed) {
            throw new SPDOException('Cannot safely iterate cells after transform()!');
        }
        // If not initialized, we must call reset() to obtain first line
        if (!isset($this->data)) {
            $reset = true;
        }
        $this->init();
        if ($reset || empty($this->line) || count($this->line) === $this->nCell) {
            // Call reset() or next(), which return false on empty array or end of array, respectively
            $row = $reset ? reset($this->data) : next($this->data);
            if (empty($row)) {
                // No line left or no data at all
                return false;
            } else {
                // Reset the pointer of the line
                $this->line = array_values($row);
                $this->nCell = 1;
                return $this->line[0];
            }
        }
        return $this->line[$this->nCell++];
    }

    /**
     * Get next row of data set
     *
     * @param bool $reset setting this parameter to true will reset the array pointer
     * (required for first call)
     * @return mixed
     * @throws SPDOException
     */
    public function row($reset = false) {
        if ($this->nesting > 0) {
            throw new SPDOException('Cannot iterate rows after group()!');
        }
        if ($this->line !== null) {
            throw new SPDOException('Cannot iterate rows while iterating cells!');
        }
        // If not initialized, we must call reset() to obtain first line
        if (!isset($this->data)) {
            $reset = true;
        }
        $this->init();
        return $reset ? reset($this->data) : next($this->data);
    }

    /**
     * Get next row of data set as
     *
     * @param bool $reset setting this parameter to true will reset the array pointer
     * (required for first call)
     * @return mixed
     * @throws SPDOException
     */
    public function rowObject($reset = false) {
        if ($this->transformed) {
            throw new SPDOException('Cannot safely cast transformed rows, use transform() to cast!');
        }
        $row = $this->row($reset);
        return $row === false ? false : (object)$row;
    }

    /**
     * Returns the manipulated data as hold in this statement.
     * The innermost dimension usually consists of maps (assoc. arrays).
     * This is different if transform() was called on this statement with non-array callback return type.
     *
     * @param bool $reduce whether to reduce one-element-arrays to their value
     * @return array manipulated data as hold in this statement object
     */
    public function get($reduce = true) {
        $this->init();
        if (!$this->transformed && $this->statement->columnCount() === $this->nesting + 1 && $reduce) {
            return $this->immerse(function ($data) {
                //reduce 1-element-maps to their value
                foreach ($data as &$cell) {
                    $cell = reset($cell);
                }
                return $data;
            });
        } else {
            return $this->data;
        }
    }

    public function getUnique($reduce = true) {
        if (!$this->transformed && $this->statement->columnCount() === $this->nesting + 1 && $reduce) {
            return $this->immerse(function ($data) {
                //reduce 1-element-maps inside 1-element-arrays to their value
                if (count($data) === 1) {
                    $data = reset($data);
                    return reset($data);
                } else {
                    throw new SPDOException('Unique fetch failed, map with more than one element was found!');
                }
            });
        } else {
            //reduce 1-element-arrays to their value
            return $this->immerse(function ($data) {
                if (count($data) === 1) {
                    return reset($data);
                } else {
                    throw new SPDOException('Unique fetch failed, map with more than one element found!');
                }
            });
        }
    }

    /**
     * Retrieves all rows as stdClass objects
     *
     * @return array Array of stdClass objects
     * @throws SPDOException If rows have been transformed
     */
    public function getObjects() {
        if ($this->transformed) {
            throw new SPDOException('Cannot safely cast transformed rows, use transform() to cast!');
        }
        //simply cast to objects
        return $this->immerse(function ($data) {
            foreach ($data as &$d) {
                $d = (object)$d;
            }
            return $data;
        });
    }

    public function getFunc($callback) {
        return $this->immerse(function ($data) use ($callback) {
            foreach ($data as &$d) {
                $d = $callback($d);
            }
            return $data;
        });
    }

    /**
     * Passes the parameter binding to the underlying statement
     *
     * @param string $parameter The number/name of the bind parameter
     * @param mixed $value The value that is bound
     * @param int $data_type The PDO data type
     * @return SPDOStatement $this
     */
    public function bindValue($parameter, $value, $data_type) {
        if (!is_numeric($parameter) && $parameter[0] !== ':') {
            $parameter = ':' . $parameter;
        }
        $this->statement->bindValue($parameter, $value, $data_type);
        return $this;
    }

}
