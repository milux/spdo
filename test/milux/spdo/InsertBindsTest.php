<?php

namespace milux\spdo;


require __DIR__ . '/../../../vendor/autoload.php';
require __DIR__ . '/../../autoload.php';

class InsertBindsTest extends TestTemplate {

    /**
     * @throws SPDOException
     */
    public function testAssocInsertBind() {
        $connection = SPDO::getConfig()->newSPDOConnection();
        $testTable = self::TEST_TABLE;
        $insertStmt = $connection->prepare("
            INSERT INTO $testTable (col1, col2, col3)
            VALUES (:k, :l, :m)
        ");
        $insertStmt->bindTyped([
            'k' => 123,
            'l' => 456
        ]);
        $insertStmt->bindValue('m', null, \PDO::PARAM_NULL)->execute();
        self::assertEquals(1, $connection->query('SELECT ROW_COUNT()')->cell());
        self::assertNull($connection->query("SELECT col3 FROM $testTable")->cell());
    }

    /**
     * @throws SPDOException
     */
    public function testNumInsertBind() {
        $connection = SPDO::getConfig()->newSPDOConnection();
        $testTable = self::TEST_TABLE;
        $insertStmt = $connection->prepare("
            INSERT INTO $testTable (col1, col2, col3)
            VALUES (?, ?, ?)
        ");
        $insertStmt->bindTyped([
            123,
            456
        ]);
        $insertStmt->bindValue(3, null, \PDO::PARAM_NULL)->execute();
        self::assertEquals(1, $connection->query('SELECT ROW_COUNT()')->cell());
        self::assertNull($connection->query("SELECT col3 FROM $testTable")->cell());
    }

}