<?php
/**
 * Bread PHP Framework (http://github.com/saiv/Bread)
 * Copyright 2010-2012, SAIV Development Team <development@saiv.it>
 *
 * Licensed under a Creative Commons Attribution 3.0 Unported License.
 * Redistributions of files must retain the above copyright notice.
 *
 * @copyright  Copyright 2010-2012, SAIV Development Team <development@saiv.it>
 * @link       http://github.com/saiv/Bread Bread PHP Framework
 * @package    Bread
 * @since      Bread PHP Framework
 * @license    http://creativecommons.org/licenses/by/3.0/
 */
namespace Bread\Storage;

use Bread\Configuration\Manager as Configuration;
use Bread\Storage\Exceptions;
use Exception;

class Manager
{

    protected static $drivers = array();
    protected static $mapping = array();

    public static function register($driver, $class, $options = array(), $domain = '__default__')
    {
        if (is_string($driver)) {
            if (!isset(static::$drivers["{$driver}@{$domain}"])) {
                static::$drivers["{$driver}@{$domain}"] = static::factory($driver, $options, $domain);
            }
            static::$mapping["{$class}@{$domain}"] = static::$drivers["{$driver}@{$domain}"];
        } else {
            static::$mapping["{$class}@{$domain}"] = $driver;
        }
        return static::$mapping["{$class}@{$domain}"];
    }

    public static function driver($class, $domain = '__default__')
    {
        $classes = class_parents($class);
        array_unshift($classes, $class);
        foreach ($classes as $c) {
            if (isset(static::$mapping["{$c}@{$domain}"])) {
                return static::$mapping["{$c}@{$domain}"];
            } elseif ($url = Configuration::get($c, 'storage.url', $domain)) {
                return static::register($url, $c, (array) Configuration::get($c, 'storage.options', $domain), $domain);
            }
        }
        throw new Exceptions\DriverNotRegistered($class);
    }

    public static function factory($url, $options = array(), $domain = '__default__')
    {
        $scheme = parse_url($url, PHP_URL_SCHEME);
        if (!$Driver = Configuration::get(__CLASS__, "drivers.$scheme")) {
            throw new Exception("Driver for {$scheme} not found.");
        }
        if (!is_subclass_of($Driver, 'Bread\Storage\Interfaces\Driver')) {
            throw new Exception("{$Driver} isn't a valid driver.");
        }
        return new $Driver($url, $options, $domain);
    }
}

Configuration::defaults('Bread\Storage\Manager', array(
    'drivers' => array(
        'mongodb' => 'Bread\Storage\Drivers\MongoDB',
        'mysql' => 'Bread\Storage\Drivers\Doctrine',
        'sqlsrv' => 'Bread\Storage\Drivers\Doctrine',
        'db2' => 'Bread\Storage\Drivers\Doctrine',
        'ldap' => 'Bread\Storage\Drivers\LDAP'
    )
));
