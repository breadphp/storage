<?php

namespace Bread\Storage;

use Bread\Storage\Hydration\Instance;
use Bread\Promises\When;

abstract class Driver
{
    protected $hydrationMap;
    
    abstract protected function normalizeValue($name, $value, $class);

    public function getHydrationMap()
    {
        return $this->hydrationMap;
    }
    
    protected function hydrateObject($properties, $oid, Instance $instance)
    {
        $reflector = $instance->getReflector();
        $class = $instance->getClass();
        $object = $reflector->newInstanceWithoutConstructor();
        $instance->setObjectId($oid);
        $promises = array();
        foreach ($properties as $name => $value) {
            $promises[$name] = $this->normalizeValue($name, $value, $class);
        }
        return When::all($promises, function($properties) use ($object, $instance, $reflector) {
            foreach ($properties as $name => $value) {
                $property = $reflector->getProperty($name);
                $property->setAccessible(true);
                $property->setValue($object, $value);
            }
            $this->hydrationMap->attach($object, $instance);
            return $object;
        });
    }
}