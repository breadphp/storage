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
            $this->attach($object, new Instance($object, Instance::STATE_NEW));
        }
        return $this->offsetGet($object);
    }
    
    public function objectExists($oid)
    {
        return isset($this->oidMap[$oid]) ? $this->oidMap[$oid] : false;
    }
    
    public function attach($object, $instance)
    {
        $this->oidMap[$instance->getObjectId()] = $object;
        parent::attach($object, $instance);
    }
}