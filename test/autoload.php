<?php
spl_autoload_register(function ($c) {
    $filename = __DIR__ . '/' . str_replace('\\', '/', ltrim($c, '\\')) . '.php';
    if (file_exists($filename)) {
        include $filename;
    }
});