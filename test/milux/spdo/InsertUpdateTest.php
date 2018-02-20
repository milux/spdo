<?php

namespace milux\spdo;


require __DIR__ . '/../../../vendor/autoload.php';
require __DIR__ . '/../../autoload.php';

class InsertUpdateTest extends TestTemplate {

    /**
     * @throws SPDOException
     */
    public function testInsertUpdate() {
        $testTable = self::TEST_TABLE;
        SPDO::insert($testTable, [
            'col1' => 123,
            'col2' => 234
        ]);
        SPDO::insert($testTable, [
            'col1' => 567,
            'col2' => 890
        ]);
        self::assertEquals(['123' => 234, '567' => 890],
            SPDO::query("SELECT col1, col2 FROM $testTable")->group(['col1'])->getUnique());
        SPDO::update($testTable, ['col2' => 891], 'col1 = ?', [567]);
        self::assertEquals(1, SPDO::query('SELECT ROW_COUNT()')->cell());
        self::assertEquals(['123' => 234, '567' => 891],
            SPDO::query("SELECT col1, col2 FROM $testTable")->group(['col1'])->getUnique());
    }

}