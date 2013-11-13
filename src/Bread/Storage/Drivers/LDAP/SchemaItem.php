<?php
namespace Bread\Storage\Drivers\LDAP;

/**
 * Generic parent class for all schema items.
 * A schema item is
 * an ObjectClass, an AttributeBype, a MatchingRule, or a Syntax.
 * All schema items have at least two things in common: An OID
 * and a description. This class provides an implementation for
 * these two data.
 */
abstract class SchemaItem
{

    /**
     * The OID of this schema item.
     */
    protected $oid = null;

    /**
     * The description of this schema item.
     */
    protected $description = null;

    /**
     * Boolean value indicating whether this objectClass is obsolete
     */
    protected $is_obsolete = false;

    public function setOID($oid)
    {
        $this->oid = $oid;
    }

    public function __get($property)
    {
        return $this->$property;
    }
}