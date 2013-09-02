<?php

error_reporting(E_ALL | E_STRICT);

$autoloader = require dirname(__DIR__) . '/vendor/autoload.php';
$autoloader->add('Bread\Tests', __DIR__);
