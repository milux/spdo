<?php

namespace milux\spdo;


require __DIR__ . '/../../../vendor/autoload.php';
require __DIR__ . '/../../autoload.php';

class GetTest extends TestTemplate {

    /**
     * @throws SPDOException
     */
    public function testGetVariants() {
        $testTable = self::TEST_TABLE;
        SPDO::insert($testTable, [
            'col1' => 123,
            'col2' => 234
        ]);
        self::assertEquals([['col1' => 123, 'col2' => 234]],
            SPDO::query("SELECT col1, col2 FROM $testTable")->get());
        self::assertEquals(['123' => [['col2' => 234]]],
            SPDO::query("SELECT col1, col2 FROM $testTable")->group(['col1'])->get(false));
        self::assertEquals(['123' => [234]],
            SPDO::query("SELECT col1, col2 FROM $testTable")->group(['col1'])->get());
        self::assertEquals(['123' => ['col2' => 234]],
            SPDO::query("SELECT col1, col2 FROM $testTable")->group(['col1'])->getUnique(false));
        self::assertEquals(['123' => 234],
            SPDO::query("SELECT col1, col2 FROM $testTable")->group(['col1'])->getUnique());
    }

}