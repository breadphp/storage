<?php

namespace Bread\Storage\Hydration;

use ReflectionClass;
use Bread\Storage\Collection;

class Instance
{
    const STATE_NEW = 0;
    const STATE_MANAGED = 1;
    const STATE_DELETED = 2;
    
    protected $className;
    protected $reflector;
    protected $object;
    protected $state;
    protected $oid;
    
    public function __construct($objectOrClassName, $oid, $state = self::STATE_MANAGED)
    {
        if (is_object($objectOrClassName)) {
            $this->className = get_class($objectOrClassName);
            $this->reflector = new ReflectionClass($this->className);
            $this->setObject($objectOrClassName);
        } elseif (is_string($objectOrClassName)) {
            $this->className = $objectOrClassName;
            $this->reflector = new ReflectionClass($this->className);
            $this->setObject($this->reflector->newInstanceWithoutConstructor(), false);
        }
        $this->oid = $oid;
        $this->state = $state;
    }
    
    public function getReflector()
    {
        return $this->reflector;
    }
    
    public function getClass()
    {
        return $this->className;
    }
    
    public function getObject()
    {
        return $this->object;
    }
    
    public function setObject($object, $clone = true)
    {
        $this->object = $clone ? clone $object : $object;
        $this->originalProperties = array_map(function ($value) {
            if ($value instanceof Collection) {
                return $value->getArrayCopy();
            }
        }, $this->getProperties($this->object));
    }
    
    public function getProperty($object, $name)
    {
        $property = $this->reflector->getProperty($name);
        $property->setAccessible(true);
        return $property->getValue($object);
    }
    
    public function setProperty($object, $name, $value)
    {
        $property = $this->reflector->getProperty($name);
        $property->setAccessible(true);
        $property->setValue($object, $value);
    }
    
    public function getObjectId()
    {
        return $this->oid;
    }
    
    public function setObjectId($oid)
    {
        $this->oid = $oid;
    }
    
    public function getPropertyNames()
    {
        $propertyNames = array();
        foreach ($this->reflector->getProperties() as $property) {
            $propertyNames[] = $property->name;
        }
        return $propertyNames;
    }
    
    public function getProperties($object)
    {
        $properties = array();
        foreach ($this->reflector->getProperties() as $property) {
            $property->setAccessible(true);
            $properties[$property->name] = $property->getValue($object);
        }
        return $properties;
    }
    
    public function getModifiedProperties($object)
    {
        if ($this->state === self::STATE_NEW) {
            return $this->getProperties($object);
        }
        $modifiedProperties = array();
        foreach ($this->reflector->getProperties() as $property) {
            $property->setAccessible(true);
            $previousValue = $property->getValue($this->object);
            $currentValue = $property->getValue($object);
            if ($currentValue instanceof Collection) {
                $currentValue = $currentValue->getArrayCopy();
            }
            if ($previousValue !== $currentValue) {
                $modifiedProperties[$property->name] = $currentValue;
            }
        }
        return $modifiedProperties;
    }
    
    public function setProperties(array $properties) {
        foreach ($properties as $name => $value) {
            $property = $this->reflector->getProperty($name);
            $property->setAccessible(true);
            $property->setValue($this->object, $value);
        }
    }
    
    public function getState()
    {
        return $this->state;
    }
    
    public function setState($state)
    {
        $this->state = $state;
    }
}