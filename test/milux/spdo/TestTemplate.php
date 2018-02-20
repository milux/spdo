<?php

namespace milux\spdo;


require __DIR__ . '/../../../vendor/autoload.php';

use PHPUnit\Framework\TestCase;

class TestTemplate extends TestCase {

    const TEST_TABLE = 'test_table';

    public static function setUpBeforeClass() {
        SPDO::setConfig(new TestSPDOConfig(function ($config) {
            return new ModernSPDOConnection($config);
        }));
        $testTable = self::TEST_TABLE;
        SPDO::exec(<<<SQL
CREATE TABLE IF NOT EXISTS $testTable (
    col1 INT NOT NULL,
    col2 INT NOT NULL,
    col3 VARCHAR(45) NULL DEFAULT NULL,
PRIMARY KEY (col1))
SQL
        );
    }

    /**
     * @throws SPDOException
     */
    public function tearDown() {
        SPDO::exec('TRUNCATE ' . self::TEST_TABLE);
    }

    /**
     * @throws SPDOException
     */
    public static function tearDownAfterClass() {
        SPDO::exec('DROP TABLE test_table');
    }

}