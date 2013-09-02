<?php

namespace Bread\Tests\Storage\Drivers;

use Bread\Storage\Drivers\LDAP;
use PHPUnit_Framework_TestCase;

class LDAPTest extends PHPUnit_Framework_TestCase
{
    public $uri;
    public $driver;
    public $class;
    public $singleEqualityCondition;
    
    public function setUp()
    {
        $this->uri = 'ldap://localhost/ou=people,dc=pc-lovatog?objectClass=inetOrgPerson';
        $this->driver = new LDAP($this->uri);
        $this->class = 'Bread\Tests\Person';
        $this->singleEqualityCondition = array(
          'uid' => 'glovato'
        );
    }
    
    public function tearDown()
    {
        unset($this->driver);
    }
    
    public function testFetchSingleEqualityCondition()
    {
        $fetch = $this->driver->fetch($this->class, $this->singleEqualityCondition);
        $result = $this->getMock('Bread\Tests\CallableStub');
        $constraint = new \PHPUnit_Framework_Constraint_Count(1);
        $result->expects($this->once())->method('__invoke')->with($constraint);
        $fetch->then($result);
    }
}