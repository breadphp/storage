<?php
namespace Bread\Storage\Drivers\LDAP;

/**
 * Represents an LDAP objectClass
 */
class ObjectClass extends SchemaItem
{

    /**
     * The schema item's name.
     */
    protected $name = null;

    /**
     * An array of alias attribute names, strings
     */
    protected $aliases = array();

    /**
     * Array of objectClass names from which this objectClass inherits
     */
    protected $sup_classes = array();

    /**
     * One of STRUCTURAL, ABSTRACT, or AUXILIARY
     */
    protected $type = null;

    /**
     * Arrays of attribute names that this objectClass requires
     */
    protected $must_attrs = array();

    /**
     * Arrays of attribute names that this objectClass allows, but does not require
     */
    protected $may_attrs = array();

    /**
     * Creates a new ObjectClass object given a raw LDAP objectClass string.
     */
    public function __construct($class)
    {
        $regex = "/(?<oid>[\S]*){1}"
            . "( NAME \( (?<aliases>[^()]*) \))?( NAME (?<name>[\S]*))?"
            . "( DESC '(?<description>[^']*)')?"
            . "( (?<obsolete>OBSOLETE))?"
            . "( SUP \( (?<msup_classes>[^()]*) \))?( SUP (?<sup_classes>[\S]*))?"
            . "( (?<abstract>ABSTRACT))?"
            . "( (?<structural>STRUCTURAL))?"
            . "( (?<auxiliary>AUXILIARY))?"
            . "( MUST \( (?<mmust>[^()]*) \))?( MUST (?<must>[\S]*))?"
            . "( MAY \( (?<mmay>[^()]*) \))?( MAY (?<may>[\S]*))?/";
        preg_match($regex, trim(trim(trim($class, "("), ")")), $matches);
        foreach (array_filter($matches) as $attribute => $value) {
            switch ($attribute) {
                case 'oid':
                    $this->setOID($value);
                    break;
                case 'aliases':
                    $split = explode("' '", trim(trim($value),"'"));
                    $this->name = (trim(array_shift($split),"'"));
                    $this->aliases = $split;
                    break;
                case 'name':
                case 'description':
                    $this->$attribute = trim($value,"'");;
                    break;
                case 'obsolete' :
                    $this->is_obsolete = true;
                    break;
                case 'sup_classes':
                    $this->sup_classes[] = $value;
                    break;
                case 'msup_classes':
                    $this->sup_classes = explode(' $ ', $value);
                    break;
                case 'abstract':
                case 'structural':
                case 'auxiliary':
                    $this->type = $attribute;
                    break;
                case 'mmust':
                    foreach(explode(' $ ', $value) as $attr){
                        $this->must_attrs[] = $attr;
                    }
                    break;
                case 'must':
                    $this->must_attrs[] = $value;
                    break;
                case 'mmay':
                    foreach(explode(' $ ', $value) as $attr){
                        $this->may_attrs[] = $attr;
                    }
                    break;
                case 'may':
                    $this->may_attrs[] = $value;
                    break;
            }
        }
    }

    public function isStructural()
    {
        if ($this->type == 'structural')
            return true;
        else
            return false;
    }

    public function resetSupClasses()
    {
        $this->sup_classes = array();
    }

    public function addSupClass($class)
    {
        $this->sup_classes[] = $class;
    }

    public function resetMustAttributes()
    {
        $this->must_attrs = array();
    }

    public function addMustAttribute($attribute)
    {
        $this->must_attrs[] = $attribute;
    }

    public function resetMayAttributes()
    {
        $this->may_attrs = array();
    }

    public function addMayAttribute($attribute)
    {
        $this->may_attrs[] = $attribute;
    }

}