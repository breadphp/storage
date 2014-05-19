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

use Bread\Configuration\Manager as ConfigurationManager;
use Bread\Promises\Interfaces\Promise;
use JsonSerializable;
use Exception;
use ReflectionClass;

class Reference implements JsonSerializable
{

    public $_ref;

    public $_id;

    public function __construct($object, $domain = '__default__')
    {
        $class = get_class($object);
        $this->_ref = $class;
        $this->_id = Manager::driver($class, $domain)->getObjectId($object);
    }

    public function __toString()
    {
        return json_encode($this);
    }

    public function jsonSerialize() {
        return $this;
    }

    public static function __set_state($array)
    {
        $reflectionClass = new ReflectionClass(__CLASS__);
        $reference = $reflectionClass->newInstanceWithoutConstructor();
        foreach ($reflectionClass->getProperties() as $property) {
            if (isset($array[$property->name])) {
                $property->setValue($reference, $array[$property->name]);
            }
        }
        return $reference;
    }

    public static function is($reference, $class = null)
    {
        if ($reference instanceof Reference) {
            return $class ? $reference->_ref === $class : true;
        } elseif (is_string($reference)) {
            $reference = json_decode($reference, true);
        }
        if (is_array($reference)) {
            if (isset($reference['_ref']) && isset($reference['_id'])) {
                return $class ? $reference['_ref'] === $class : true;
            }
        }
        return false;
    }

    public static function fetch($reference, $domain = '__default__')
    {
        if (!static::is($reference)) {
            throw new Exception("Not a valid reference");
        } elseif (is_string($reference)) {
            $reference = json_decode($reference);
        } elseif (is_array($reference)) {
            $reference = (object) $reference;
        }
        return Manager::driver($reference->_ref, $domain)->getObject($reference->_ref, $reference->_id);
    }
}
