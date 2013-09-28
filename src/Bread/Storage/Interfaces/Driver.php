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
namespace Bread\Storage\Interfaces;

interface Driver
{

    public function store($object, $oid = null);

    public function delete($object);

    public function count($class, array $search = array(), array $options = array());

    public function first($class, array $search = array(), array $options = array());

    public function fetch($class, array $search = array(), array $options = array());

    public function purge($class, array $search = array(), array $options = array());
}
