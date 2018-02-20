<?php

namespace milux\spdo;


require __DIR__ . '/../../../vendor/autoload.php';
require __DIR__ . '/../../autoload.php';

use milux\spdo\traits\MultiRowInsertSupport;
use milux\spdo\traits\OnDuplicateKeyUpdateSupport;

class MultiRowInsertConnection extends SPDOConnection {
    use MultiRowInsertSupport;
}

class OnDuplicateKeyConnection extends SPDOConnection {
    use OnDuplicateKeyUpdateSupport;
}

class TraitsTest extends TestTemplate {

    /**
     * Test multiple row insert trait
     * @throws SPDOException
     */
    public function testMultiRowInsert() {
        $config = new TestSPDOConfig(function ($config) {
            return new MultiRowInsertConnection($config);
        });
        /**
         * @var $instance MultiRowInsertConnection
         */
        $instance = $config->newSPDOConnection();
        self::assertInstanceOf('\milux\spdo\MultiRowInsertConnection', $instance);
        $instance->setMaxInsertRows(5);
        // Insert 20 rows in 4 chunks
        $col1 = [];
        $col2 = [];
        for ($i = 1; $i <= 20; $i++) {
            $col1[] = $i;
            $col2[] = $i * 10;
        }
        $instance->batchInsert(self::TEST_TABLE, [
            'col1' => $col1,
            'col2' => $col2
        ]);
        // The last round inserted 5 rows
        self::assertEquals(5, $instance->query('SELECT ROW_COUNT()')->cell());
        // Insert 4 rows
        $col1 = [];
        $col2 = [];
        for ($i = 21; $i <= 24; $i++) {
            $col1[] = $i;
            $col2[] = $i * 10;
        }
        $instance->batchInsert(self::TEST_TABLE, [
            'col1' => $col1,
            'col2' => $col2
        ]);
        // The last round inserted 4 rows
        self::assertEquals(4, $instance->query('SELECT ROW_COUNT()')->cell());
        // Insert 6 rows
        $col1 = [];
        $col2 = [];
        for ($i = 25; $i <= 30; $i++) {
            $col1[] = $i;
            $col2[] = $i * 10;
        }
        $instance->batchInsert(self::TEST_TABLE, [
            'col1' => $col1,
            'col2' => $col2
        ]);
        // The last round inserted 1 row
        self::assertEquals(1, $instance->query('SELECT ROW_COUNT()')->cell());
        // We expect 20 rows
        self::assertEquals(30, SPDO::count(self::TEST_TABLE));
    }

    /**
     * Test multiple row insert trait
     * @throws SPDOException
     */
    public function testOnDuplicateKey() {
        $config = new TestSPDOConfig(function ($config) {
            return new OnDuplicateKeyConnection($config);
        });
        /**
         * @var $instance OnDuplicateKeyConnection
         */
        $instance = $config->newSPDOConnection();
        self::assertInstanceOf('\milux\spdo\OnDuplicateKeyConnection', $instance);
        $instance->insert(self::TEST_TABLE, ['col1' => 1, 'col2' => 10]);
        $instance->insert(self::TEST_TABLE, ['col1' => 2, 'col2' => 20]);
        // Creates new row
        $instance->save(self::TEST_TABLE, ['col1' => 3], ['col2' => 300]);
        self::assertEquals(1, $instance->query('SELECT ROW_COUNT()')->cell());
        // Updates existing row to same value
        $instance->save(self::TEST_TABLE, ['col1' => 1], ['col2' => 10]);
        self::assertEquals(0, $instance->query('SELECT ROW_COUNT()')->cell());
        // Updates existing row to changed value
        $instance->save(self::TEST_TABLE, ['col1' => 1], ['col2' => 100]);
        self::assertEquals(2, $instance->query('SELECT ROW_COUNT()')->cell());
        // We expect 3 rows
        self::assertEquals(3, SPDO::count(self::TEST_TABLE));
    }

}
