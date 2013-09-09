<?php
namespace Bread\Storage;

use ArrayObject;
use JsonSerializable;

class Collection extends ArrayObject implements JsonSerializable
{

    public function jsonSerialize()
    {
        return $this->getArrayCopy();
    }
}