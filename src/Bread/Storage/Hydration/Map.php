<?php
namespace Bread\Storage\Hydration;

use SplObjectStorage;
use InvalidArgumentException;

class Map extends SplObjectStorage
{
    protected $oidMap = array();

    public function getInstance($object)
    {
        if (!$this->offsetExists($object)) {
            return new Instance($object, null, Instance::STATE_NEW);
        }
        return $this->offsetGet($object);
    }

    public function objectExists($class, $oid)
    {
        return isset($this->oidMap[$class . ":" . $oid]) ? $this->oidMap[$class . ":" . $oid] : false;
    }

    public function attach($object, $instance = null)
    {
        $this->oidMap[get_class($object) . ":" . $instance->getObjectId()] = $object;
        parent::attach($object, $instance);
    }

    public function detach($object)
    {
        $instance = $this->getInstance($object);
        unset($this->oidMap[get_class($object) . ":" . $instance->getObjectId()]);
        parent::detach($object);
    }
}