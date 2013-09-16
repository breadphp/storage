<?php
namespace Bread\Storage\Drivers\LDAP\ObjectClass;

/**
 * A simple class for representing AttributeTypes used only by the ObjectClass class.
 * Users should never instantiate this class. It represents an attribute internal to
 * an ObjectClass. If PHP supported inner-classes and variable permissions, this would
 * be interior to class ObjectClass and flagged private. The reason this class is used
 * and not the "real" class AttributeType is because this class supports the notion of
 * a "source" objectClass, meaning that it keeps track of which objectClass originally
 * specified it. This class is therefore used by the class ObjectClass to determine
 * inheritance.
 *
 * @package phpLDAPadmin
 * @subpackage Schema
 */
class ObjectClassAttribute {
    # This Attribute's name (needs to be public, as we sort on it with masort).
    public $name;
    # This Attribute's root (needs to be public, as we sort on it with masort).
    public $source;

    /**
     * Creates a new ObjectClass_ObjectClassAttribute with specified name and source objectClass.
     *
     * @param string $name the name of the new attribute.
     * @param string $source the name of the ObjectClass which specifies this attribute.
     */
    public function __construct($name,$source) {
        if (DEBUG_ENABLED && (($fargs=func_get_args())||$fargs='NOARGS'))
            debug_log('Entered (%%)',9,0,__FILE__,__LINE__,__METHOD__,$fargs);

        $this->name = $name;
        $this->source = $source;
    }

    # Gets this attribute's name
    public function getName($lower=true) {
    if (DEBUG_ENABLED && (($fargs=func_get_args())||$fargs='NOARGS'))
        debug_log('Entered (%%)',9,1,__FILE__,__LINE__,__METHOD__,$fargs,$this->name);

        return $lower ? strtolower($this->name) : $this->name;
}

    # Gets the name of the ObjectClass which originally specified this attribute.
    public function getSource() {
    if (DEBUG_ENABLED && (($fargs=func_get_args())||$fargs='NOARGS'))
			debug_log('Entered (%%)',9,1,__FILE__,__LINE__,__METHOD__,$fargs,$this->source);

		return $this->source;
	}
}