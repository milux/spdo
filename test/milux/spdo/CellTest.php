<?php
/**
 * Created by PhpStorm.
 * User: Michael
 * Date: 19.03.2018
 * Time: 21:42
 */

namespace milux\spdo;


require __DIR__ . '/../../../vendor/autoload.php';
require __DIR__ . '/../../autoload.php';

class CellTest extends TestTemplate {

    /**
     * @throws SPDOException
     */
    public function setUp() {
        SPDO::batchInsert(self::TEST_TABLE, [
            'col1' => [1, 4, 7],
            'col2' => [2, 5, 8],
            'col3' => [3, 6, 9]
        ]);
    }

    /**
     * @throws SPDOException
     */
    public function testCellCalls() {
        $testTable = self::TEST_TABLE;
        $stmt = SPDO::query("SELECT col1, col2, col3 FROM $testTable");
        self::assertEquals(1, $stmt->cell());
        self::assertEquals(2, $stmt->cell());
        self::assertEquals('3', $stmt->cell());
        self::assertEquals(4, $stmt->cell());
        self::assertEquals(5, $stmt->cell());
        self::assertEquals('6', $stmt->cell());
        self::assertEquals(7, $stmt->cell());
        self::assertEquals(8, $stmt->cell());
        self::assertEquals('9', $stmt->cell());
        self::assertEquals(false, $stmt->cell());
    }

    /**
     * @throws SPDOException
     */
    public function testCellReset() {
        $testTable = self::TEST_TABLE;
        $stmt = SPDO::query("SELECT col1, col2, col3 FROM $testTable");
        self::assertEquals(1, $stmt->cell());
        self::assertEquals(2, $stmt->cell());
        self::assertEquals('3', $stmt->cell());
        self::assertEquals(4, $stmt->cell());
        self::assertEquals(5, $stmt->cell());
        // Reset here, everything has to work as before now
        $this->testCellCalls();
    }

}
