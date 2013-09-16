<?php
namespace Bread\Storage\Drivers\LDAP;
/**
 * Generic parent class for all schema items. A schema item is
 * an ObjectClass, an AttributeBype, a MatchingRule, or a Syntax.
 * All schema items have at least two things in common: An OID
 * and a description. This class provides an implementation for
 * these two data.
 *
 * @package phpLDAPadmin
 * @subpackage Schema
 */
abstract class SchemaItem {
    # The schema item's name.
    protected $name = null;
    # The OID of this schema item.
    private $oid = null;
    # The description of this schema item.
    protected $description = null;
    # Boolean value indicating whether this objectClass is obsolete
    private $is_obsolete = false;

    public function setOID($oid) {
        if (DEBUG_ENABLED && (($fargs=func_get_args())||$fargs='NOARGS'))
            debug_log('Entered (%%)',9,1,__FILE__,__LINE__,__METHOD__,$fargs);

        $this->oid = $oid;
    }

    public function setDescription($desc) {
        if (DEBUG_ENABLED && (($fargs=func_get_args())||$fargs='NOARGS'))
            debug_log('Entered (%%)',9,1,__FILE__,__LINE__,__METHOD__,$fargs);

        $this->description = $desc;
    }

    public function getOID() {
        if (DEBUG_ENABLED && (($fargs=func_get_args())||$fargs='NOARGS'))
            debug_log('Entered (%%)',9,1,__FILE__,__LINE__,__METHOD__,$fargs,$this->oid);

        return $this->oid;
    }

    public function getDescription() {
        if (DEBUG_ENABLED && (($fargs=func_get_args())||$fargs='NOARGS'))
            debug_log('Entered (%%)',9,1,__FILE__,__LINE__,__METHOD__,$fargs,$this->description);

        return $this->description;
    }

    /**
     * Gets whether this objectClass is flagged as obsolete by the LDAP server.
     */
    public function getIsObsolete() {
        if (DEBUG_ENABLED && (($fargs=func_get_args())||$fargs='NOARGS'))
            debug_log('Entered (%%)',9,1,__FILE__,__LINE__,__METHOD__,$fargs,$this->is_obsolete);

        return $this->is_obsolete;
    }

    /**
     * Return the objects name.
     *
     * param boolean $lower Return the name in lower case (default)
     * @return string The name
     */
    public function getName($lower=true) {
        if (DEBUG_ENABLED && (($fargs=func_get_args())||$fargs='NOARGS'))
            debug_log('Entered (%%)',9,1,__FILE__,__LINE__,__METHOD__,$fargs,$this->name);

        return $lower ? strtolower($this->name) : $this->name;
    }
}