<?php
namespace Bread\Storage\Drivers\LDAP;

/**
 * Represents an LDAP AttributeType
 *
 * @package phpLDAPadmin
 * @subpackage Schema
 */
class AttributeType extends SchemaItem {
    
    // TODO const for all types, syntaxes, equality matches, etc.
    const TYPE_DN = 'Distinguished Name';
    const TYPE_GENERALIZED_TIME = 'Generalized Time';
    const FORMAT_GENERALIZED_TIME = 'YmdHisZ';
    
    # The attribute from which this attribute inherits (if any)
    private $sup_attribute = null;
    # The equality rule used
    private $equality = null;
    # The ordering of the attributeType
    private $ordering = null;
    # Boolean: supports substring matching?
    private $sub_str = null;
    # The full syntax string, ie 1.2.3.4{16}
    private $syntax = null;
    private $syntax_oid = null;
    # boolean: is single valued only?
    private $is_single_value = false;
    # boolean: is collective?
    private $is_collective = false;
    # boolean: can use modify?
    private $is_no_user_modification = false;
    # The usage string set by the LDAP schema
    private $usage = null;
    # An array of alias attribute names, strings
    private $aliases = array();
    # The max number of characters this attribute can be
    private $max_length = null;
    # A string description of the syntax type (taken from the LDAPSyntaxes)
    private $type = null;
    # An array of objectClasses which use this attributeType (must be set by caller)
    private $used_in_object_classes = array();
    # A list of object class names that require this attribute type.
    private $required_by_object_classes = array();
    # This attribute has been forced a MAY attribute by the configuration.
    private $forced_as_may = false;

    /**
     * Creates a new AttributeType object from a raw LDAP AttributeType string.
     */
    public function __construct($attr) {
        if (DEBUG_ENABLED && (($fargs=func_get_args())||$fargs='NOARGS'))
            debug_log('Entered (%%)',9,0,__FILE__,__LINE__,__METHOD__,$fargs);

        $strings = preg_split('/[\s,]+/',$attr,-1,PREG_SPLIT_DELIM_CAPTURE);

        for($i=0; $i<count($strings); $i++) {

            switch($strings[$i]) {
              case '(':
                  break;

              case 'NAME':
                  # Some schema's return a (' instead of a ( '
                  if ($strings[$i+1] != '(' && ! preg_match('/^\(/',$strings[$i+1])) {
                      do {
                          $i++;
                          if (strlen($this->name)==0)
                              $this->name = $strings[$i];
                          else
                              $this->name .= ' '.$strings[$i];

                      } while (! preg_match("/\'$/s",$strings[$i]));

                      # This attribute has no aliases
                      $this->aliases = array();

                  } else {
                      $i++;
                      do {
                          # In case we came here becaues of a ('
                          if (preg_match('/^\(/',$strings[$i]))
                              $strings[$i] = preg_replace('/^\(/','',$strings[$i]);
                          else
                              $i++;

                          if (strlen($this->name) == 0)
                              $this->name = $strings[$i];
                          else
                              $this->name .= ' '.$strings[$i];

                      } while (! preg_match("/\'$/s",$strings[$i]));

                      # Add alias names for this attribute
                      while ($strings[++$i] != ')') {
                          $alias = $strings[$i];
                          $alias = preg_replace("/^\'/",'',$alias);
                          $alias = preg_replace("/\'$/",'',$alias);
                          $this->addAlias($alias);
                      }
                  }

                  if (DEBUG_ENABLED)
                      debug_log('Case NAME returned (%s) (%s)',8,0,__FILE__,__LINE__,__METHOD__,$this->name,$this->aliases);
                  break;

              case 'DESC':
                  do {
                      $i++;
                      if (strlen($this->description)==0)
                          $this->description=$this->description.$strings[$i];
                      else
                          $this->description=$this->description.' '.$strings[$i];
                  } while (! preg_match("/\'$/s",$strings[$i]));

                  if (DEBUG_ENABLED)
                      debug_log('Case DESC returned (%s)',8,0,__FILE__,__LINE__,__METHOD__,$this->description);
                  break;

              case 'OBSOLETE':
                  $this->is_obsolete = TRUE;

                  if (DEBUG_ENABLED)
                      debug_log('Case OBSOLETE returned (%s)',8,0,__FILE__,__LINE__,__METHOD__,$this->is_obsolete);
                  break;

              case 'SUP':
                  $i++;
                  $this->sup_attribute = $strings[$i];

                  if (DEBUG_ENABLED)
                      debug_log('Case SUP returned (%s)',8,0,__FILE__,__LINE__,__METHOD__,$this->sup_attribute);
                  break;

              case 'EQUALITY':
                  $i++;
                  $this->equality = $strings[$i];

                  if (DEBUG_ENABLED)
                      debug_log('Case EQUALITY returned (%s)',8,0,__FILE__,__LINE__,__METHOD__,$this->equality);
                  break;

              case 'ORDERING':
                  $i++;
                  $this->ordering = $strings[$i];

                  if (DEBUG_ENABLED)
                      debug_log('Case ORDERING returned (%s)',8,0,__FILE__,__LINE__,__METHOD__,$this->ordering);
                  break;

              case 'SUBSTR':
                  $i++;
                  $this->sub_str = $strings[$i];

                  if (DEBUG_ENABLED)
                      debug_log('Case SUBSTR returned (%s)',8,0,__FILE__,__LINE__,__METHOD__,$this->sub_str);
                  break;

              case 'SYNTAX':
                  $i++;
                  $this->syntax = $strings[$i];
                  $this->syntax_oid = preg_replace('/{\d+}$/','',$this->syntax);

                  # Does this SYNTAX string specify a max length (ie, 1.2.3.4{16})
                  if (preg_match('/{(\d+)}$/',$this->syntax,$this->max_length))
                      $this->max_length = $this->max_length[1];
                  else
                      $this->max_length = null;

                  if ($i < count($strings) - 1 && $strings[$i+1] == '{') {
                      do {
                          $i++;
                          $this->name .= ' '.$strings[$i];
                      } while ($strings[$i] != '}');
                  }

                  if (DEBUG_ENABLED)
                      debug_log('Case SYNTAX returned (%s) (%s) (%s)',8,0,__FILE__,__LINE__,__METHOD__,
                          $this->syntax,$this->syntax_oid,$this->max_length);
                  break;

              case 'SINGLE-VALUE':
                  $this->is_single_value = TRUE;
                  if (DEBUG_ENABLED)
                      debug_log('Case SINGLE-VALUE returned (%s)',8,0,__FILE__,__LINE__,__METHOD__,$this->is_single_value);
                  break;

              case 'COLLECTIVE':
                  $this->is_collective = TRUE;

                  if (DEBUG_ENABLED)
                      debug_log('Case COLLECTIVE returned (%s)',8,0,__FILE__,__LINE__,__METHOD__,$this->is_collective);
                  break;

              case 'NO-USER-MODIFICATION':
                  $this->is_no_user_modification = TRUE;

                  if (DEBUG_ENABLED)
                      debug_log('Case NO-USER-MODIFICATION returned (%s)',8,0,__FILE__,__LINE__,__METHOD__,$this->is_no_user_modification);
                  break;

              case 'USAGE':
                  $i++;
                  $this->usage = $strings[$i];

                  if (DEBUG_ENABLED)
                      debug_log('Case USAGE returned (%s)',8,0,__FILE__,__LINE__,__METHOD__,$this->usage);
                  break;

              default:
                  if (preg_match('/[\d\.]+/i',$strings[$i]) && $i == 1) {
                      $this->setOID($strings[$i]);

                      if (DEBUG_ENABLED)
                          debug_log('Case default returned (%s)',8,0,__FILE__,__LINE__,__METHOD__,$this->getOID());
                  }
            }
        }

        $this->name = preg_replace("/^\'/",'',$this->name);
        $this->name = preg_replace("/\'$/",'',$this->name);
        $this->description = preg_replace("/^\'/",'',$this->description);
        $this->description = preg_replace("/\'$/",'',$this->description);
        $this->syntax = preg_replace("/^\'/",'',$this->syntax);
        $this->syntax = preg_replace("/\'$/",'',$this->syntax);
        $this->syntax_oid = preg_replace("/^\'/",'',$this->syntax_oid);
        $this->syntax_oid = preg_replace("/\'$/",'',$this->syntax_oid);
        $this->sup_attribute = preg_replace("/^\'/",'',$this->sup_attribute);
        $this->sup_attribute = preg_replace("/\'$/",'',$this->sup_attribute);

        if (DEBUG_ENABLED)
            debug_log('Returning ()',9,0,__FILE__,__LINE__,__METHOD__);
    }

    /**
     * Gets this attribute's usage string as defined by the LDAP server
     *
     * @return string
     */
    public function getUsage() {
        if (DEBUG_ENABLED && (($fargs=func_get_args())||$fargs='NOARGS'))
            debug_log('Entered (%%)',9,1,__FILE__,__LINE__,__METHOD__,$fargs,$this->usage);

        return $this->usage;
    }

    /**
     * Gets this attribute's parent attribute (if any). If this attribute does not
     * inherit from another attribute, null is returned.
     *
     * @return string
     */
    public function getSupAttribute() {
        if (DEBUG_ENABLED && (($fargs=func_get_args())||$fargs='NOARGS'))
            debug_log('Entered (%%)',9,1,__FILE__,__LINE__,__METHOD__,$fargs,$this->sup_attribute);

        return $this->sup_attribute;
    }

    /**
     * Gets this attribute's equality string
     *
     * @return string
     */
    public function getEquality() {
        if (DEBUG_ENABLED && (($fargs=func_get_args())||$fargs='NOARGS'))
            debug_log('Entered (%%)',9,1,__FILE__,__LINE__,__METHOD__,$fargs,$this->equality);

        return $this->equality;
    }

    /**
     * Gets this attribute's ordering specification.
     *
     * @return string
     */
    public function getOrdering() {
        if (DEBUG_ENABLED && (($fargs=func_get_args())||$fargs='NOARGS'))
            debug_log('Entered (%%)',9,1,__FILE__,__LINE__,__METHOD__,$fargs,$this->ordering);

        return $this->ordering;
    }

    /**
     * Gets this attribute's substring matching specification
     *
     * @return string
     */
    public function getSubstr() {
        if (DEBUG_ENABLED && (($fargs=func_get_args())||$fargs='NOARGS'))
            debug_log('Entered (%%)',9,1,__FILE__,__LINE__,__METHOD__,$fargs,$this->sub_str);

        return $this->sub_str;
    }

    /**
     * Gets the names of attributes that are an alias for this attribute (if any).
     *
     * @return array An array of names of attributes which alias this attribute or
     *          an empty array if no attribute aliases this object.
     */
    public function getAliases() {
        if (DEBUG_ENABLED && (($fargs=func_get_args())||$fargs='NOARGS'))
            debug_log('Entered (%%)',9,1,__FILE__,__LINE__,__METHOD__,$fargs,$this->aliases);

        return $this->aliases;
    }

    /**
     * Returns whether the specified attribute is an alias for this one (based on this attribute's alias list).
     *
     * @param string $attr_name The name of the attribute to check.
     * @return boolean True if the specified attribute is an alias for this one, or false otherwise.
     */
    public function isAliasFor($attr_name) {
        if (DEBUG_ENABLED && (($fargs=func_get_args())||$fargs='NOARGS'))
            debug_log('Entered (%%)',9,0,__FILE__,__LINE__,__METHOD__,$fargs);

        foreach ($this->aliases as $alias_attr_name)
        if (strcasecmp($alias_attr_name,$attr_name) == 0)
            return true;

        return false;
    }

    /**
     * Gets this attribute's raw syntax string (ie: "1.2.3.4{16}").
     *
     * @return string The raw syntax string
     */
    public function getSyntaxString() {
        if (DEBUG_ENABLED && (($fargs=func_get_args())||$fargs='NOARGS'))
            debug_log('Entered (%%)',9,1,__FILE__,__LINE__,__METHOD__,$fargs,$this->syntax);

        return $this->syntax;
    }

    /**
     * Gets this attribute's syntax OID. Differs from getSyntaxString() in that this
     * function only returns the actual OID with any length specification removed.
     * Ie, if the syntax string is "1.2.3.4{16}", this function only retruns
     * "1.2.3.4".
     *
     * @return string The syntax OID string.
     */
    public function getSyntaxOID() {
        if (DEBUG_ENABLED && (($fargs=func_get_args())||$fargs='NOARGS'))
            debug_log('Entered (%%)',9,1,__FILE__,__LINE__,__METHOD__,$fargs,$this->syntax_oid);

        return $this->syntax_oid;
    }

    /**
     * Gets this attribute's the maximum length. If no maximum is defined by the LDAP server, null is returned.
     *
     * @return int The maximum length (in characters) of this attribute or null if no maximum is specified.
     */
    public function getMaxLength() {
        if (DEBUG_ENABLED && (($fargs=func_get_args())||$fargs='NOARGS'))
            debug_log('Entered (%%)',9,1,__FILE__,__LINE__,__METHOD__,$fargs,$this->max_length);

        return $this->max_length;
    }

    /**
     * Gets whether this attribute is single-valued. If this attribute only supports single values, true
     * is returned. If this attribute supports multiple values, false is returned.
     *
     * @return boolean Returns true if this attribute is single-valued or false otherwise.
     */
    public function getIsSingleValue() {
        if (DEBUG_ENABLED && (($fargs=func_get_args())||$fargs='NOARGS'))
            debug_log('Entered (%%)',9,1,__FILE__,__LINE__,__METHOD__,$fargs,$this->is_single_value);

        return $this->is_single_value;
    }

    /**
     * Sets whether this attribute is single-valued.
     *
     * @param boolean $is
     */
    public function setIsSingleValue($is) {
        if (DEBUG_ENABLED && (($fargs=func_get_args())||$fargs='NOARGS'))
            debug_log('Entered (%%)',9,1,__FILE__,__LINE__,__METHOD__,$fargs);

        $this->is_single_value = $is;
    }

    /**
     * Gets whether this attribute is collective.
     *
     * @return boolean Returns true if this attribute is collective and false otherwise.
     */
    public function getIsCollective() {
        if (DEBUG_ENABLED && (($fargs=func_get_args())||$fargs='NOARGS'))
            debug_log('Entered (%%)',9,1,__FILE__,__LINE__,__METHOD__,$fargs,$this->is_collective);

        return $this->is_collective;
    }

    /**
     * Gets whether this attribute is not modifiable by users.
     *
     * @return boolean Returns true if this attribute is not modifiable by users.
     */
    public function getIsNoUserModification() {
        if (DEBUG_ENABLED && (($fargs=func_get_args())||$fargs='NOARGS'))
            debug_log('Entered (%%)',9,1,__FILE__,__LINE__,__METHOD__,$fargs,$this->is_no_user_modification);

        return $this->is_no_user_modification;
    }

    /**
     * Gets this attribute's type
     *
     * @return string The attribute's type.
     */
    public function getType() {
        if (DEBUG_ENABLED && (($fargs=func_get_args())||$fargs='NOARGS'))
            debug_log('Entered (%%)',9,1,__FILE__,__LINE__,__METHOD__,$fargs,$this->type);

        return $this->type;
    }

    /**
     * Removes an attribute name from this attribute's alias array.
     *
     * @param string $remove_alias_name The name of the attribute to remove.
     * @return boolean true on success or false on failure (ie, if the specified
     *           attribute name is not found in this attribute's list of aliases)
     */
    public function removeAlias($remove_alias_name) {
        if (DEBUG_ENABLED && (($fargs=func_get_args())||$fargs='NOARGS'))
            debug_log('Entered (%%)',9,0,__FILE__,__LINE__,__METHOD__,$fargs);

        foreach ($this->aliases as $i => $alias_name) {

            if (strcasecmp($alias_name,$remove_alias_name) == 0) {
                unset($this->aliases[$i]);

                $this->aliases = array_values($this->aliases);
                return true;
            }
        }
        return false;
    }

    /**
     * Adds an attribute name to the alias array.
     *
     * @param string $alias The name of a new attribute to add to this attribute's list of aliases.
     */
    public function addAlias($alias) {
        if (DEBUG_ENABLED && (($fargs=func_get_args())||$fargs='NOARGS'))
            debug_log('Entered (%%)',9,0,__FILE__,__LINE__,__METHOD__,$fargs);

        array_push($this->aliases,$alias);
    }

    /**
     * Sets this attriute's name.
     *
     * @param string $name The new name to give this attribute.
     */
    public function setName($name) {
        if (DEBUG_ENABLED && (($fargs=func_get_args())||$fargs='NOARGS'))
            debug_log('Entered (%%)',9,1,__FILE__,__LINE__,__METHOD__,$fargs);

        $this->name = $name;
    }

    /**
     * Sets this attriute's SUP attribute (ie, the attribute from which this attribute inherits).
     *
     * @param string $attr The name of the new parent (SUP) attribute
     */
    public function setSupAttribute($attr) {
        if (DEBUG_ENABLED && (($fargs=func_get_args())||$fargs='NOARGS'))
            debug_log('Entered (%%)',9,1,__FILE__,__LINE__,__METHOD__,$fargs);

        $this->sup_attribute = $attr;
    }

    /**
     * Sets this attribute's list of aliases.
     *
     * @param array $aliases The array of alias names (strings)
     */
    public function setAliases($aliases) {
        if (DEBUG_ENABLED && (($fargs=func_get_args())||$fargs='NOARGS'))
            debug_log('Entered (%%)',9,1,__FILE__,__LINE__,__METHOD__,$fargs);

        $this->aliases = $aliases;
    }

    /**
     * Sets this attribute's type.
     *
     * @param string $type The new type.
     */
    public function setType($type) {
        if (DEBUG_ENABLED && (($fargs=func_get_args())||$fargs='NOARGS'))
            debug_log('Entered (%%)',9,1,__FILE__,__LINE__,__METHOD__,$fargs);

        $this->type = $type;
    }

    /**
     * Adds an objectClass name to this attribute's list of "used in" objectClasses,
     * that is the list of objectClasses which provide this attribute.
     *
     * @param string $name The name of the objectClass to add.
     */
    public function addUsedInObjectClass($name) {
        if (DEBUG_ENABLED && (($fargs=func_get_args())||$fargs='NOARGS'))
            debug_log('Entered (%%)',9,0,__FILE__,__LINE__,__METHOD__,$fargs);

        foreach ($this->used_in_object_classes as $used_in_object_class) {
            if (DEBUG_ENABLED)
                debug_log('Checking (%s) with (%s)',8,0,__FILE__,__LINE__,__METHOD__,$used_in_object_class,$name);

            if (strcasecmp($used_in_object_class,$name) == 0)
                return false;
        }

        array_push($this->used_in_object_classes,$name);
    }

    /**
     * Gets the list of "used in" objectClasses, that is the list of objectClasses
     * which provide this attribute.
     *
     * @return array An array of names of objectclasses (strings) which provide this attribute
     */
    public function getUsedInObjectClasses() {
        if (DEBUG_ENABLED && (($fargs=func_get_args())||$fargs='NOARGS'))
            debug_log('Entered (%%)',9,1,__FILE__,__LINE__,__METHOD__,$fargs,$this->used_in_object_classes);

        return $this->used_in_object_classes;
    }

    /**
     * Adds an objectClass name to this attribute's list of "required by" objectClasses,
     * that is the list of objectClasses which must have this attribute.
     *
     * @param string $name The name of the objectClass to add.
     */
    public function addRequiredByObjectClass($name) {
        if (DEBUG_ENABLED && (($fargs=func_get_args())||$fargs='NOARGS'))
            debug_log('Entered (%%)',9,0,__FILE__,__LINE__,__METHOD__,$fargs);

        foreach ($this->required_by_object_classes as $required_by_object_class)
        if (strcasecmp($required_by_object_class,$name) == 0)
            return false;

        array_push($this->required_by_object_classes,$name);
    }

    /**
     * Gets the list of "required by" objectClasses, that is the list of objectClasses
     * which provide must have attribute.
     *
     * @return array An array of names of objectclasses (strings) which provide this attribute
     */
    public function getRequiredByObjectClasses() {
        if (DEBUG_ENABLED && (($fargs=func_get_args())||$fargs='NOARGS'))
            debug_log('Entered (%%)',9,1,__FILE__,__LINE__,__METHOD__,$fargs,$this->required_by_object_classes);

        return $this->required_by_object_classes;
    }

    /**
     * This function will mark this attribute as a forced MAY attribute
     */
    public function setForceMay() {
        if (DEBUG_ENABLED && (($fargs=func_get_args())||$fargs='NOARGS'))
            debug_log('Entered (%%)',9,1,__FILE__,__LINE__,__METHOD__,$fargs);

        $this->forced_as_may = true;
    }

    public function isForceMay() {
        if (DEBUG_ENABLED && (($fargs=func_get_args())||$fargs='NOARGS'))
            debug_log('Entered (%%)',9,1,__FILE__,__LINE__,__METHOD__,$fargs,$this->forced_as_may);

        return $this->forced_as_may;
    }
}