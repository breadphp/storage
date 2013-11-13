<?php
namespace Bread\Storage\Drivers\LDAP;

/**
 * Represents an LDAP AttributeType
 */
class AttributeType extends SchemaItem
{

    /**
     * The schema item's name.
     */
    protected $name = null;

    /**
     * The attribute from which this attribute inherits (if any)
     */
    protected $sup_attribute = null;

    /**
     * The equality rule used
     */
    protected $equality = null;

    /**
     * The ordering of the attributeType
     */
    protected $ordering = null;

    /**
     * Boolean: supports substring matching?
     */
    protected $sub_str = null;

    /**
     * The full syntax string, ie 1.2.3.4{16}
     */
    protected $syntax = null;

    /**
     * boolean: is single valued only?
     */
    protected $is_single_value = false;

    /**
     * boolean: is collective?
     */
    protected $is_collective = false;

    /**
     * boolean: can use modify?
     */
    protected $is_no_user_modification = false;

    /**
     * The usage string set by the LDAP schema
     */
    protected $usage = null;

    /**
     * An array of alias attribute names, strings
     */
    protected $aliases = array();

    /**
     * The max number of characters this attribute can be
     */
    protected $max_length = null;

    /**
     * A string description of the syntax type (taken from the LDAPSyntaxes)
     */
    protected $type = null;

    /**
     * Creates a new AttributeType object from a raw LDAP AttributeType string.
     */
    public function __construct($attr)
    {
        $attr = (trim(trim(trim($attr, "("), ")")));
        $regex = "/(?<oid>[\S]*){1}"
            . "( NAME \( (?<aliases>[^()]*) \))?( NAME (?<name>[\S]*))?"
            . "( DESC '(?<description>[^']*)')?"
            . "( (?<is_obsolete>OBSOLETE))?"
            . "( SUP (?<sup_attribute>[\S]*))?"
            . "( EQUALITY (?<equality>[\S]*))?"
            . "( ORDERING (?<ordering>[\S]*))?"
            . "( SUBSTR (?<sub_str>[\S]*))?"
            . "( SYNTAX (?<syntax>[\S]*))?"
            . "( (?<is_single_value>SINGLE-VALUE))?"
            . "( (?<is_collective>COLLECTIVE))?"
            . "( X-ORDERED (?<xorder>))?"
            . "( (?<is_no_user_modification>NO-USER-MODIFICATION))?"
            . "( USAGE (?<usage>[\S]*))?/";
        preg_match($regex, $attr, $matches);
        foreach (array_filter($matches) as $attribute => $value) {
            switch ($attribute) {
                case 'oid':
                    $this->setOID($value);
                    break;
                case 'name':
                case 'description':
                case 'sup_attribute':
                case 'equality':
                case 'ordering':
                case 'sub_str':
                case 'usage':
                    $this->$attribute = trim($value, "'");
                    break;
                case 'syntax':
                    $explode = explode("{", trim($value, "}"));
                    $this->$attribute = array_shift($explode);
                    if ($explode) {
                        $this->max_length = array_shift($explode);
                    }
                    break;
                case 'aliases':
                    $split = explode("' '", trim(trim($value), "'"));
                    $this->name = (trim(array_shift($split), "'"));
                    $this->aliases = $split;
                    break;
                case 'is_obsolete':
                case 'is_single_value':
                case 'is_collective':
                case 'is_no_user_modification':
                    $this->$attribute = true;
                    break;
            }
        }
    }

    /**
     * Sets this attribute's type.
     */
    public function setType($type) {
        $this->type = $type;
    }

    public function setSupAttribute($attribute) {
        $this->sup_attribute = $attribute;
    }

    public function changeAliases($name)
    {
        $this->aliases[] = $this->name;
        unset($this->aliases[array_search($name, $this->aliases)]);
        $this->name = $name;
    }
}