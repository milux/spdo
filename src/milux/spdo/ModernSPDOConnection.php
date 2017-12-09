<?php
/**
 * An version of SPDOConnection with both MultiRowInsertSupport and OnDuplicateKeyUpdateSupport included.
 * For use with recent RDBMS with INSERT ON DUPLICATE KEY UPDATE and multiple values INSERT support.
 */

namespace milux\spdo;


use milux\spdo\traits\MultiRowInsertSupport;
use milux\spdo\traits\OnDuplicateKeyUpdateSupport;

class ModernSPDOConnection extends SPDOConnection {
    use MultiRowInsertSupport, OnDuplicateKeyUpdateSupport;
}