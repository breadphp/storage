<?php
namespace Bread\Storage\Drivers\LDAP;

/**
 * Represents an LDAP schema matchingRuleUse entry
 *
 * @package phpLDAPadmin
 * @subpackage Schema
 */
class MatchingRuleUse extends SchemaItem {
    # An array of attribute names who use this MatchingRule
    private $used_by_attrs = array();

    function __construct($strings) {
        if (DEBUG_ENABLED && (($fargs=func_get_args())||$fargs='NOARGS'))
            debug_log('Entered (%%)',9,0,__FILE__,__LINE__,__METHOD__,$fargs);

        $strings = preg_split('/[\s,]+/',$strings,-1,PREG_SPLIT_DELIM_CAPTURE);

        for($i=0; $i<count($strings); $i++) {
            switch($strings[$i]) {
              case '(':
                  break;

              case 'NAME':
                  if ($strings[$i+1] != '(') {
                      do {
                          $i++;
                          if (! isset($this->name) || strlen($this->name) == 0)
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

              case 'APPLIES':
                  if ($strings[$i+1] != '(') {
                      # Has a single attribute name
                      $i++;
                      $this->used_by_attrs = array($strings[$i]);

                  } else {
                      # Has multiple attribute names
                      $i++;
                      while ($strings[$i] != ')') {
                          $i++;
                          $new_attr = $strings[$i];
                          $new_attr = preg_replace("/^\'/",'',$new_attr);
                          $new_attr = preg_replace("/\'$/",'',$new_attr);
                          array_push($this->used_by_attrs,$new_attr);
                          $i++;
                      }
                  }
                  break;

              default:
                  if (preg_match('/[\d\.]+/i',$strings[$i]) && $i == 1)
                      $this->setOID($strings[$i]);
            }
        }

        sort($this->used_by_attrs);
    }

    /**
     * Gets an array of attribute names (strings) which use this MatchingRuleUse object.
     *
     * @return array The array of attribute names (strings).
     */
    public function getUsedByAttrs() {
        if (DEBUG_ENABLED && (($fargs=func_get_args())||$fargs='NOARGS'))
            debug_log('Entered (%%)',9,1,__FILE__,__LINE__,__METHOD__,$fargs,$this->used_by_attrs);

        return $this->used_by_attrs;
    }
}