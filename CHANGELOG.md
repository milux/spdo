## 2.1.6
* (*) SPDOException is now inheriting from RuntimeException
* (+) Published Changelog

## 2.1.5
* (*) Removed usage of each() (deprecated)

## 2.1.4
* (*) Fixed license string
* (*) More parameter binding fixes

## 2.1.3
* (*) Fixed parameter binding issues

## 2.1.2
* (*) Fixed 2 function modifiers in SPDO to "static"

## 2.1.1
* (+) Added SPDOConnection::inTransaction() to check transaction state

## 2.1
* (+) Added SPDOConnection::batchInsertRows()
* (+) Added ModernSPDOConnection using MultiRowInsertSupport and OnDuplicateKeyUpdateSupport
* (*) ModernSPDOConnection is the new default, providing best performance for modern MySQL/MariaDB servers

## 2.0
* (*) Min. PHP version 5.4
* (*) Changed insert ID return semantics
* (+) Added SPDOConnection::count() method
* (+) Added MultiRowInsertSupport trait for efficient batch insertion
* (+) Added OnDuplicateKeyUpdateSupport trait for efficient "upsert" operations
* (-) Removed insert ID returning of batchInsert()