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

class Reference implements JsonSerializable
{

    public $_class;

    public $_keys;

    public function __construct($object)
    {
        $this->_class = get_class($object);
        $this->_keys = $this->keys($object);
    }

    public function __toString()
    {
        return json_encode($this);
    }
    
    public function jsonSerialize() {
        return $this;
    }

    protected function keys($object)
    {
        $class = get_class($object);
        if (!$configuredKeys = ConfigurationManager::get($class, 'keys')) {
            throw new Exception(sprintf('No keys configured on class %s', $class));
        }
        $keys = array();
        foreach ($configuredKeys as $keyProperty) {
            if (is_object($object->$keyProperty)) {
                $keys[$keyProperty] = new Reference($object->$keyProperty);
            } else {
                $keys[$keyProperty] = $object->$keyProperty;
            }
        }
        return $keys;
    }

    public static function is($reference)
    {
        if ($reference instanceof Reference) {
            return $reference;
        } elseif (is_object($reference)) {
            return false;
        } elseif (is_string($reference)) {
            $reference = json_decode($reference, true);
        }
        if (isset($reference['_class']) && isset($reference['_keys'])) {
            return $reference;
        }
        return false;
    }

    public static function fetch($reference)
    {
        if (!static::is($reference)) {
            throw new Exception("Not a valid reference");
        } elseif (is_string($reference)) {
            $reference = json_decode($reference, true);
        }
        if (is_array($reference)) {
            $reference = (object) $reference;
        }
        $class = $reference->_class;
        $search = $reference->_keys;
        return Manager::driver($class)->first($class, $search);
    }
}
