<?php
namespace Bread\Storage\Drivers\LDAP;

/**
 * Represents an LDAP objectClass
 *
 * @package phpLDAPadmin
 * @subpackage Schema
 */
class ObjectClass extends SchemaItem {
    # The server ID that this objectclass belongs to.
    private $server_id = null;
    # Array of objectClass names from which this objectClass inherits
    private $sup_classes = array();
    # One of STRUCTURAL, ABSTRACT, or AUXILIARY
    private $type;
    # Arrays of attribute names that this objectClass requires
    private $must_attrs = array();
    # Arrays of attribute names that this objectClass allows, but does not require
    private $may_attrs = array();
    # Arrays of attribute names that this objectClass has been forced to MAY attrs, due to configuration
    private $force_may = array();
    # Array of objectClasses which inherit from this one (must be set at runtime explicitly by the caller)
    private $children_objectclasses = array();
    # The objectclass hierarchy
    private $hierarchy = array();

    /**
     * Creates a new ObjectClass object given a raw LDAP objectClass string.
    */
    public function __construct($class,$server) {
        if (DEBUG_ENABLED && (($fargs=func_get_args())||$fargs='NOARGS'))
            debug_log('Entered (%%)',9,0,__FILE__,__LINE__,__METHOD__,$fargs);

        $this->server_id = $server->getIndex();
        $this->type = $server->getValue('server','schema_oclass_default');

        $strings = preg_split('/[\s,]+/',$class,-1,PREG_SPLIT_DELIM_CAPTURE);
        $str_count = count($strings);

        for ($i=0; $i < $str_count; $i++) {

            switch ($strings[$i]) {
              case '(':
                  break;

              case 'NAME':
                  if ($strings[$i+1]!='(') {
                      do {
                          $i++;
                          if (strlen($this->name) == 0)
                              $this->name = $strings[$i];
                          else
                              $this->name .= ' '.$strings[$i];

                      } while (! preg_match('/\'$/s',$strings[$i]));

                  } else {
                      $i++;
                      do {
                          $i++;
                          if (strlen($this->name) == 0)
                              $this->name = $strings[$i];
                          else
                              $this->name .= ' '.$strings[$i];

                      } while (! preg_match('/\'$/s',$strings[$i]));

                      do {
                          $i++;
                      } while (! preg_match('/\)+\)?/',$strings[$i]));
                  }

                  $this->name = preg_replace('/^\'/','',$this->name);
                  $this->name = preg_replace('/\'$/','',$this->name);

                  if (DEBUG_ENABLED)
                      debug_log('Case NAME returned (%s)',8,0,__FILE__,__LINE__,__METHOD__,$this->name);
                  break;

              case 'DESC':
                  do {
                      $i++;
                      if (strlen($this->description) == 0)
                          $this->description=$this->description.$strings[$i];
                      else
                          $this->description=$this->description.' '.$strings[$i];

                  } while (! preg_match('/\'$/s',$strings[$i]));

                  if (DEBUG_ENABLED)
                      debug_log('Case DESC returned (%s)',8,0,__FILE__,__LINE__,__METHOD__,$this->description);
                  break;

              case 'OBSOLETE':
                  $this->is_obsolete = TRUE;

                  if (DEBUG_ENABLED)
                      debug_log('Case OBSOLETE returned (%s)',8,0,__FILE__,__LINE__,__METHOD__,$this->is_obsolete);
                  break;

              case 'SUP':
                  if ($strings[$i+1] != '(') {
                      $i++;
                      array_push($this->sup_classes,preg_replace("/'/",'',$strings[$i]));

                  } else {
                      $i++;
                      do {
                          $i++;
                          if ($strings[$i] != '$')
                              array_push($this->sup_classes,preg_replace("/'/",'',$strings[$i]));

                      } while (! preg_match('/\)+\)?/',$strings[$i+1]));
                  }

                  if (DEBUG_ENABLED)
                      debug_log('Case SUP returned (%s)',8,0,__FILE__,__LINE__,__METHOD__,$this->sup_classes);
                  break;

              case 'ABSTRACT':
                  $this->type = 'abstract';

                  if (DEBUG_ENABLED)
                      debug_log('Case ABSTRACT returned (%s)',8,0,__FILE__,__LINE__,__METHOD__,$this->type);
                  break;

              case 'STRUCTURAL':
                  $this->type = 'structural';

                  if (DEBUG_ENABLED)
                      debug_log('Case STRUCTURAL returned (%s)',8,0,__FILE__,__LINE__,__METHOD__,$this->type);
                  break;

              case 'AUXILIARY':
                  $this->type = 'auxiliary';

                  if (DEBUG_ENABLED)
                      debug_log('Case AUXILIARY returned (%s)',8,0,__FILE__,__LINE__,__METHOD__,$this->type);
                  break;

              case 'MUST':
                  $attrs = array();

                  $i = $this->parseList(++$i,$strings,$attrs);

                  if (DEBUG_ENABLED)
                      debug_log('parseList returned %d (%s)',8,0,__FILE__,__LINE__,__METHOD__,$i,$attrs);

                  foreach ($attrs as $string) {
                      $attr = new ObjectClass\ObjectClassAttribute($string,$this->name);

                      if ($server->isForceMay($attr->getName())) {
                          array_push($this->force_may,$attr);
                          array_push($this->may_attrs,$attr);

                      } else
                          array_push($this->must_attrs,$attr);
                  }

                  if (DEBUG_ENABLED)
                      debug_log('Case MUST returned (%s) (%s)',8,0,__FILE__,__LINE__,__METHOD__,$this->must_attrs,$this->force_may);
                  break;

              case 'MAY':
                  $attrs = array();

                  $i = $this->parseList(++$i,$strings,$attrs);

                  if (DEBUG_ENABLED)
                      debug_log('parseList returned %d (%s)',8,0,__FILE__,__LINE__,__METHOD__,$i,$attrs);

                  foreach ($attrs as $string) {
                      $attr = new ObjectClass\ObjectClassAttribute($string,$this->name);
                      array_push($this->may_attrs,$attr);
                  }

                  if (DEBUG_ENABLED)
                      debug_log('Case MAY returned (%s)',8,0,__FILE__,__LINE__,__METHOD__,$this->may_attrs);
                  break;

              default:
                  if (preg_match('/[\d\.]+/i',$strings[$i]) && $i == 1) {
                      $this->setOID($strings[$i]);

                      if (DEBUG_ENABLED)
                          debug_log('Case default returned (%s)',8,0,__FILE__,__LINE__,__METHOD__,$this->getOID());
                  }
                  break;
            }
        }

        $this->description = preg_replace("/^\'/",'',$this->description);
        $this->description = preg_replace("/\'$/",'',$this->description);

        if (DEBUG_ENABLED)
            debug_log('Returning () - NAME (%s), DESCRIPTION (%s), MUST (%s), MAY (%s), FORCE MAY (%s)',9,0,__FILE__,__LINE__,__METHOD__,
                $this->name,$this->description,$this->must_attrs,$this->may_attrs,$this->force_may);
    }

    /**
     * Parse an LDAP schema list
     */
    private function parseList($i,$strings,&$attrs) {
        if (DEBUG_ENABLED && (($fargs=func_get_args())||$fargs='NOARGS'))
            debug_log('Entered (%%)',9,0,__FILE__,__LINE__,__METHOD__,$fargs);

        /*
         * A list starts with a ( followed by a list of attributes separated by $ terminated by )
        * The first token can therefore be a ( or a (NAME or a (NAME)
            * The last token can therefore be a ) or NAME)
        * The last token may be terminate by more than one bracket
        */

        $string = $strings[$i];
        if (! preg_match('/^\(/',$string)) {
            # A bareword only - can be terminated by a ) if the last item
            if (preg_match('/\)+$/',$string))
                $string = preg_replace('/\)+$/','',$string);

            array_push($attrs,$string);

        } elseif (preg_match('/^\(.*\)$/',$string)) {
            $string = preg_replace('/^\(/','',$string);
            $string = preg_replace('/\)+$/','',$string);
            array_push($attrs,$string);

        } else {
            # Handle the opening cases first
            if ($string == '(') {
                $i++;

            } elseif (preg_match('/^\(./',$string)) {
                $string = preg_replace('/^\(/','',$string);
                array_push($attrs,$string);
                $i++;
            }

            # Token is either a name, a $ or a ')'
            # NAME can be terminated by one or more ')'
			while (! preg_match('/\)+$/',$strings[$i])) {
			$string = $strings[$i];
			    if ($string == '$') {
			    $i++;
			    continue;
                }

                if (preg_match('/\)$/',$string))
                $string = preg_replace('/\)+$/','',$string);
                else
                    $i++;

				array_push($attrs,$string);
    }
    }

    sort($attrs);

    if (DEBUG_ENABLED)
        debug_log('Returning (%d,[%s],[%s])',9,0,__FILE__,__LINE__,__METHOD__,$i,$strings,$attrs);

        return $i;
        }

        /**
        * This will return all our parent ObjectClass Objects
            */
            public function getParents() {
        if (DEBUG_ENABLED && (($fargs=func_get_args())||$fargs='NOARGS'))
            debug_log('Entered (%%)',9,0,__FILE__,__LINE__,__METHOD__,$fargs);

            if ((count($this->sup_classes) == 1) && ($this->sup_classes[0] == 'top'))
                return array();

                $server = $_SESSION[APPCONFIG]->getServer($this->server_id);
                $return = array();

                foreach ($this->sup_classes as $object_class) {
                array_push($return,$object_class);

                $oc = $server->getSchemaObjectClass($object_class);

                if ($oc)
                    $return = array_merge($return,$oc->getParents());
                }

                return $return;
                }

                /**
                * Gets an array of AttributeType objects that entries of this ObjectClass must define.
                * This differs from getMustAttrNames in that it returns an array of AttributeType objects
                *
                * @param array $parents An array of ObjectClass objects to use when traversing
                *             the inheritance tree. This presents some what of a bootstrapping problem
                *             as we must fetch all objectClasses to determine through inheritance which
                *             attributes this objectClass requires.
                * @return array The array of required AttributeType objects.
                *
                * @see getMustAttrNames
                * @see getMayAttrs
                * @see getMayAttrNames
                */
                public function getMustAttrs($parents=false) {
                if (DEBUG_ENABLED && (($fargs=func_get_args())||$fargs='NOARGS'))
                    debug_log('Entered (%%)',9,0,__FILE__,__LINE__,__METHOD__,$fargs);

                if (! $parents)
                    return $this->must_attrs;

                $server = $_SESSION[APPCONFIG]->getServer($this->server_id);
                $attrs = $this->must_attrs;

                foreach ($this->getParents() as $sup_class) {
                $sc = $server->getSchemaObjectClass($sup_class);
                $attrs = array_merge($attrs,$sc->getMustAttrs());
                }

                masort($attrs,'name,source');

                # Remove any duplicates
                foreach ($attrs as $index => $attr)
                if (isset($allattr[$attr->getName()]))
                    unset($attrs[$index]);
                else
                    $allattr[$attr->getName()] = 1;

                    return $attrs;
	}

                        /**
	 * Gets an array of AttributeType objects that entries of this ObjectClass may define.
	 * This differs from getMayAttrNames in that it returns an array of AttributeType objects
	 *
	 * @param array $parents An array of ObjectClass objects to use when traversing
	 *             the inheritance tree. This presents some what of a bootstrapping problem
	 *             as we must fetch all objectClasses to determine through inheritance which
	 *             attributes this objectClass provides.
	 * @return array The array of allowed AttributeType objects.
	 *
	 * @see getMustAttrNames
	 * @see getMustAttrs
	 * @see getMayAttrNames
	 * @see AttributeType
	 */
	public function getMayAttrs($parents=false) {
	if (DEBUG_ENABLED && (($fargs=func_get_args())||$fargs='NOARGS'))
	 debug_log('Entered (%%)',9,0,__FILE__,__LINE__,__METHOD__,$fargs);

		if (! $parents)
		    return $this->may_attrs;

		$server = $_SESSION[APPCONFIG]->getServer($this->server_id);
		$attrs = $this->may_attrs;

		foreach ($this->getParents() as $sup_class) {
		$sc = $server->getSchemaObjectClass($sup_class);
		$attrs = array_merge($attrs,$sc->getMayAttrs());
		}

		masort($attrs,'name,source');

		# Remove any duplicates
		foreach ($attrs as $index => $attr)
		if (isset($allattr[$attr->name]))
		    unset($attrs[$index]);
		else
		    $allattr[$attr->name] = 1;

		return $attrs;
		}

		public function getForceMayAttrs() {
                if (DEBUG_ENABLED && (($fargs=func_get_args())||$fargs='NOARGS'))
                    debug_log('Entered (%%)',9,0,__FILE__,__LINE__,__METHOD__,$fargs);

                    return $this->force_may;
                }

                /**
                * Gets an array of attribute names (strings) that entries of this ObjectClass must define.
                * This differs from getMustAttrs in that it returns an array of strings rather than
                * array of AttributeType objects
                *
                * @param array $parents An array of ObjectClass objects to use when traversing
                *             the inheritance tree. This presents some what of a bootstrapping problem
                *             as we must fetch all objectClasses to determine through inheritance which
                *             attributes this objectClass provides.
                * @return array The array of allowed attribute names (strings).
                *
                * @see getMustAttrs
                * @see getMayAttrs
                * @see getMayAttrNames
                */
                public function getMustAttrNames($parents=false) {
                if (DEBUG_ENABLED && (($fargs=func_get_args())||$fargs='NOARGS'))
                debug_log('Entered (%%)',9,0,__FILE__,__LINE__,__METHOD__,$fargs);

                $attr_names = array();

                foreach ($this->getMustAttrs($parents) as $attr)
                    array_push($attr_names,$attr->getName());

                return $attr_names;
                }

                /**
                * Gets an array of attribute names (strings) that entries of this ObjectClass must define.
                * This differs from getMayAttrs in that it returns an array of strings rather than
	 * array of AttributeType objects
	 *
	 * @param array $parents An array of ObjectClass objects to use when traversing
	 *             the inheritance tree. This presents some what of a bootstrapping problem
	 *             as we must fetch all objectClasses to determine through inheritance which
	 *             attributes this objectClass provides.
	 * @return array The array of allowed attribute names (strings).
	 *
	 * @see getMustAttrs
	 * @see getMayAttrs
	 * @see getMustAttrNames
	 */
	 public function getMayAttrNames($parents=false) {
	     if (DEBUG_ENABLED && (($fargs=func_get_args())||$fargs='NOARGS'))
	     debug_log('Entered (%%)',9,0,__FILE__,__LINE__,__METHOD__,$fargs);

                $attr_names = array();

                foreach ($this->getMayAttrs($parents) as $attr)
                    array_push($attr_names,$attr->getName());

                    return $attr_names;
                }

                /**
                * Adds an objectClass to the list of objectClasses that inherit
                * from this objectClass.
                *
                * @param String $name The name of the objectClass to add
                * @return boolean Returns true on success or false on failure (objectclass already existed for example)
	 */
	public function addChildObjectClass($name) {
	if (DEBUG_ENABLED && (($fargs=func_get_args())||$fargs='NOARGS'))
	debug_log('Entered (%%)',9,0,__FILE__,__LINE__,__METHOD__,$fargs);

	$name = trim($name);

	foreach ($this->children_objectclasses as $existing_objectclass)
	         if (strcasecmp($name,$existing_objectclass) == 0)
	             return false;

	             array_push($this->children_objectclasses,$name);
	     }

	     /**
	     * Returns the array of objectClass names which inherit from this objectClass.
	     *
	     * @return Array Names of objectClasses which inherit from this objectClass.
	     */
	     public function getChildObjectClasses() {
	     if (DEBUG_ENABLED && (($fargs=func_get_args())||$fargs='NOARGS'))
	         debug_log('Entered (%%)',9,0,__FILE__,__LINE__,__METHOD__,$fargs);

	     return $this->children_objectclasses;
	     }

	     /**
	     * Gets the objectClass names from which this objectClass inherits.
	     *
	     * @return array An array of objectClass names (strings)
	     */
	     public function getSupClasses() {
	     if (DEBUG_ENABLED && (($fargs=func_get_args())||$fargs='NOARGS'))
	         debug_log('Entered (%%)',9,0,__FILE__,__LINE__,__METHOD__,$fargs);

	         return $this->sup_classes;
	     }

	     /**
	     * Return if this objectClass is related to $oclass
	         *
	         * @param array ObjectClasses that this attribute may be related to
	         */
	         public function isRelated($oclass) {
	     if (DEBUG_ENABLED && (($fargs=func_get_args())||$fargs='NOARGS'))
	         debug_log('Entered (%%)',9,0,__FILE__,__LINE__,__METHOD__,$fargs);

		# If I am in the array, we'll just return false
	     if (in_array_ignore_case($this->name,$oclass))
	         return false;

	         $server = $_SESSION[APPCONFIG]->getServer($this->server_id);

	     foreach ($oclass as $object_class) {
	     $oc = $server->getSchemaObjectClass($object_class);

	     if ($oc->isStructural() && in_array_ignore_case($this->getName(),$oc->getParents()))
	         return true;
	     }

	     return false;
	     }

	     /**
	     * Gets the type of this objectClass: STRUCTURAL, ABSTRACT, or AUXILIARY.
	     */
	     public function getType() {
	     if (DEBUG_ENABLED && (($fargs=func_get_args())||$fargs='NOARGS'))
	         debug_log('Entered (%%)',9,1,__FILE__,__LINE__,__METHOD__,$fargs,$this->type);

	         return $this->type;
	     }

	     /**
	 * Adds the specified array of attributes to this objectClass' list of
	 * MUST attributes. The resulting array of must attributes will contain
	 * unique members.
	 *
	 * @param array $attr An array of attribute names (strings) to add.
	 */
	private function addMustAttrs($attr) {
		if (DEBUG_ENABLED && (($fargs=func_get_args())||$fargs='NOARGS'))
			debug_log('Entered (%%)',9,0,__FILE__,__LINE__,__METHOD__,$fargs);

		if (! is_array($attr) || ! count($attr))
			return;

		$this->must_attrs = array_values(array_unique(array_merge($this->must_attrs,$attr)));
	}

	/**
	 * Behaves identically to addMustAttrs, but it operates on the MAY
	 * attributes of this objectClass.
	 *
	 * @param array $attr An array of attribute names (strings) to add.
	 */
	private function addMayAttrs($attr) {
		if (DEBUG_ENABLED && (($fargs=func_get_args())||$fargs='NOARGS'))
			debug_log('Entered (%%)',9,0,__FILE__,__LINE__,__METHOD__,$fargs);

		if (! is_array($attr) || ! count($attr))
			return;

		$this->may_attrs = array_values(array_unique(array_merge($this->may_attrs,$attr)));
	}

	/**
	 * Determine if an array is listed in the force_may attrs
	 */
	public function isForceMay($attr) {
		if (DEBUG_ENABLED && (($fargs=func_get_args())||$fargs='NOARGS'))
			debug_log('Entered (%%)',9,0,__FILE__,__LINE__,__METHOD__,$fargs);

		foreach ($this->force_may as $forcemay)
			if ($forcemay->getName() == $attr)
				return true;

		return false;
	}

	public function isStructural() {
		if (DEBUG_ENABLED && (($fargs=func_get_args())||$fargs='NOARGS'))
			debug_log('Entered (%%)',9,0,__FILE__,__LINE__,__METHOD__,$fargs);

		if ($this->type == 'structural')
			return true;
		else
			return false;
	}
}