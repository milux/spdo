<?php

namespace milux\spdo;


class TestSPDOConfig extends SPDOConfig {

    private $connectionFactory;

    public function __construct(callable $connectionFactory) {
        $this->connectionFactory = $connectionFactory;
    }

    /**
     * @return string hostname of the database
     */
    public function getHost() {
        return 'localhost';
    }

    /**
     * @return string username for login
     */
    public function getUser() {
        return 'spdo_test';
    }

    /**
     * @return string password for login
     */
    public function getPassword() {
        return 'password';
    }

    /**
     * @return string selected database schema
     */
    public function getSchema() {
        return 'spdo_test';
    }

    public function newSPDOConnection() {
        return call_user_func($this->connectionFactory, $this);
    }

    /**
     * Pre-processes SQL strings, for example to replace prefix placeholders of table names
     *
     * @param $sql string unprocessed SQL
     *
     * @return string processed SQL
     */
    public function preProcess($sql) {
        return $sql;
    }
}