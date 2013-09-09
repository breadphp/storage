<?php

namespace Bread\Storage;

use Bread\Storage\Hydration\Instance;
use Bread\Promises\When;
use Bread\Caching\Cache;
use ReflectionClass;

abstract class Driver
{
    protected $hydrationMap;
    
    abstract protected function normalizeValue($name, $value, $class);

    public function getHydrationMap()
    {
        return $this->hydrationMap;
    }
    
    public function buildCollection(array $objects)
    {
        $collection = new Collection();
        foreach ($objects as $oid => $object) {
            $this->hydrationMap->attach($object, new Instance($object, $oid));
            $collection->append($object);
        }
        return $collection;
    }
    
    protected function hydrateObject($properties, $class)
    {
        $reflector = new ReflectionClass($class);
        $promises = array();
        foreach ($properties as $name => $value) {
            $promises[$name] = $this->normalizeValue($name, $value, $class);
        }
        return When::all($promises, function($properties) use ($class, $reflector) {
            $object = $reflector->newInstanceWithoutConstructor();
            foreach ($properties as $name => $value) {
                $property = $reflector->getProperty($name);
                $property->setAccessible(true);
                $property->setValue($object, $value);
            }
            return $object;
        });
    }
    
    protected function fetchFromCache($class, array $search = array(), array $options = array())
    {
        $cacheKey = implode('::', array(
            __METHOD__,
            $class,
            md5(serialize($search + $options))
        ));
        return Cache::instance()->fetch($cacheKey);
    }
    
    protected function invalidateCacheFor($class)
    {
        $cacheKey = implode('::', array(
            __METHOD__,
            $class
        ));
        return Cache::instance()->delete('/^' . preg_quote($cacheKey, '/') . '/');
    }
}