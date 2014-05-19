<?php

namespace Bread\Storage;

use Bread\Storage\Hydration\Instance;
use Bread\Promises\When;
use Bread\Caching\Cache;
use Bread\Configuration\Manager as Configuration;
use ReflectionClass;
use Traversable;
use Bread\Storage\Exceptions\DriverNotRegistered;

abstract class Driver
{
    protected $domain;
    protected $hydrationMap;
    protected $useCache = true;

    abstract protected function normalizeValue($name, $value, $class);

    public function getHydrationMap()
    {
        return $this->hydrationMap;
    }

    public function buildCollection(array $objects)
    {
        $collection = new Collection();
        foreach ($objects as $oid => $object) {
            $collection->append($object);
        }
        return $collection;
    }

    public function getObjectId($object)
    {
        return $this->hydrationMap->getInstance($object)->getObjectId();
    }

    protected function deleteCascade($object)
    {
        $class = get_class($object);
        foreach (Configuration::get($class, 'properties', $this->domain) as $property => $options) {
            if (isset($options['cascade']) && $options['cascade']) {
                if ($object->$property instanceof Traversable || is_array($object->$property)) {
                    $cascade = $object->$property;
                } else {
                    $cascade = array($object->$property);
                }
                foreach ($cascade as $c) {
                    Manager::driver($options['type'], $this->domain)->delete($c);
                }
            }
        }
    }

    protected function createObjectPlaceholder($class, $oid, $instance = null)
    {
        $reflector = new ReflectionClass($class);
        $object = $instance ? $instance->getObject() : $reflector->newInstanceWithoutConstructor();
        $this->hydrationMap->attach($object, $instance ? : new Instance($object, $oid, Instance::STATE_MANAGED));
        return When::resolve($object);
    }

    protected function hydrateObject($object, $properties, $class, $oid)
    {
        $reflector = new ReflectionClass($class);
        $promises = array();
        foreach ($properties as $name => $value) {
            if (Configuration::get($class, "properties.$name.multiple", $this->domain)) {
                $promises[$name] = When::all(array_map(function ($value) use ($name, $class) {
                    return $this->normalizeValue($name, $value, $class);
                }, $value))->then(function ($array) {
                    return new Collection($array);
                });
            } else {
                $promises[$name] = $this->normalizeValue($name, $value, $class);
            }
        }
        return When::all($promises, function ($properties) use ($class, $reflector, $oid, $object) {
            //$object = $reflector->newInstanceWithoutConstructor();
            foreach ($properties as $name => $value) {
                if (!$reflector->hasProperty($name)) {
                    continue;
                }
                $property = $reflector->getProperty($name);
                $property->setAccessible(true);
                $property->setValue($object, $value);
            }
            $this->hydrationMap->getInstance($object)->setObject($object);
            return $object;
        });
    }

    protected function fetchPropertiesFromCache($class, $oid)
    {
        $cacheKey = implode('::', array(
            __CLASS__,
            $class,
            $oid
        ));
        return Cache::instance()->fetch($cacheKey);
    }

    protected function storePropertiesToCache($cacheKey, $values)
    {
        return Cache::instance()->store($cacheKey, $values);
    }

    protected function fetchFromCache($class, array $search = array(), array $options = array())
    {
        $cacheKey = implode('::', array(
            __CLASS__,
            $class,
            md5(serialize($search + $options))
        ));
        if (!$this->useCache) {
            return When::reject($cacheKey);
        }
        return Cache::instance()->fetch($cacheKey);
    }

    protected function storeToCache($cacheKey, $objects) {
        if (!$this->useCache) {
            return $objects;
        }
        return Cache::instance()->store($cacheKey, $objects);
    }

    protected function invalidateCacheFor($class)
    {
        $cacheKey = implode('::', array(
            __CLASS__,
            $class
        ));
        return Cache::instance()->delete('/^' . preg_quote($cacheKey, '/') . '/');
    }
}
