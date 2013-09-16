<?php
namespace Bread\Storage\Drivers\LDAP;

/**
 * Represents an LDAP MatchingRule
 *
 * @package phpLDAPadmin
 * @subpackage Schema
 */
class MatchingRule extends SchemaItem {
    # This rule's syntax OID
    private $syntax = null;
    # An array of attribute names who use this MatchingRule
    private $used_by_attrs = array();

    /**
     * Creates a new MatchingRule object from a raw LDAP MatchingRule string.
    */
    function __construct($strings) {
        if (DEBUG_ENABLED && (($fargs=func_get_args())||$fargs='NOARGS'))
            debug_log('Entered (%%)',9,0,__FILE__,__LINE__,__METHOD__,$fargs);

        $strings = preg_split('/[\s,]+/',$strings,-1,PREG_SPLIT_DELIM_CAPTURE);

        for ($i=0; $i<count($strings); $i++) {
            switch($strings[$i]) {
              case '(':
                  break;

              case 'NAME':
                  if ($strings[$i+1] != '(') {
                      do {
                          $i++;
                          if (strlen($this->name) == 0)
                              $this->name = $strings[$i];
                          else
                              $this->name .= ' '.$strings[$i];
                      } while (! preg_match("/\'$/s",$strings[$i]));

                  } else {
                      $i++;
                      do {
                          $i++;
                          if (strlen($this->name) == 0)
                              $this->name = $strings[$i];
                          else
                              $this->name .= ' '.$strings[$i];
                      } while (! preg_match("/\'$/s",$strings[$i]));

                      do {
                          $i++;
                      } while (! preg_match('/\)+\)?/',$strings[$i]));
                  }

                  $this->name = preg_replace("/^\'/",'',$this->name);
                  $this->name = preg_replace("/\'$/",'',$this->name);
                  break;

              case 'DESC':
                  do {
                      $i++;
                      if (strlen($this->description)==0)
                          $this->description=$this->description.$strings[$i];
                      else
                          $this->description=$this->description.' '.$strings[$i];
                  } while (! preg_match("/\'$/s",$strings[$i]));
                  break;

              case 'OBSOLETE':
                  $this->is_obsolete = TRUE;
                  break;

              case 'SYNTAX':
                  $this->syntax = $strings[++$i];
                  break;

              default:
                  if (preg_match('/[\d\.]+/i',$strings[$i]) && $i == 1)
                      $this->setOID($strings[$i]);
            }
        }
        $this->description = preg_replace("/^\'/",'',$this->description);
        $this->description = preg_replace("/\'$/",'',$this->description);
    }

    /**
     * Sets the list of used_by_attrs to the array specified by $attrs;
     *
     * @param array $attrs The array of attribute names (strings) which use this MatchingRule
     */
    public function setUsedByAttrs($attrs) {
        if (DEBUG_ENABLED && (($fargs=func_get_args())||$fargs='NOARGS'))
            debug_log('Entered (%%)',9,1,__FILE__,__LINE__,__METHOD__,$fargs);

        $this->used_by_attrs = $attrs;
    }

    /**
     * Adds an attribute name to the list of attributes who use this MatchingRule
     *
     * @return true if the attribute was added and false otherwise (already in the list)
     */
    public function addUsedByAttr($attr) {
        if (DEBUG_ENABLED && (($fargs=func_get_args())||$fargs='NOARGS'))
            debug_log('Entered (%%)',9,0,__FILE__,__LINE__,__METHOD__,$fargs);

        foreach ($this->used_by_attrs as $attr_name)
        if (strcasecmp($attr_name,$attr) == 0)
            return false;

        array_push($this->used_by_attrs,$attr);

        return true;
    }

    /**
     * Gets an array of attribute names (strings) which use this MatchingRule
     *
     * @return array The array of attribute names (strings).
     */
    public function getUsedByAttrs() {
        if (DEBUG_ENABLED && (($fargs=func_get_args())||$fargs='NOARGS'))
            debug_log('Entered (%%)',9,1,__FILE__,__LINE__,__METHOD__,$fargs,$this->used_by_attrs);

        return $this->used_by_attrs;
    }
}