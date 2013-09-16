<?php
namespace Bread\Storage\Drivers\LDAP;

/**
 * Represents an LDAP Syntax
 *
 * @package phpLDAPadmin
 * @subpackage Schema
 */
class Syntax extends SchemaItem {
    /**
     * Creates a new Syntax object from a raw LDAP syntax string.
     */
    public function __construct($class) {
        if (DEBUG_ENABLED && (($fargs=func_get_args())||$fargs='NOARGS'))
            debug_log('Entered (%%)',9,0,__FILE__,__LINE__,__METHOD__,$fargs);

        $strings = preg_split('/[\s,]+/',$class,-1,PREG_SPLIT_DELIM_CAPTURE);

        for($i=0; $i<count($strings); $i++) {
            switch($strings[$i]) {
              case '(':
                  break;

              case 'DESC':
                  do {
                      $i++;
                      if (strlen($this->description) == 0)
                          $this->description=$this->description.$strings[$i];
                      else
                          $this->description=$this->description.' '.$strings[$i];
                  } while (! preg_match("/\'$/s",$strings[$i]));
                  break;

              default:
                  if (preg_match('/[\d\.]+/i',$strings[$i]) && $i == 1)
                      $this->setOID($strings[$i]);
            }
        }

        $this->description = preg_replace("/^\'/",'',$this->description);
        $this->description = preg_replace("/\'$/",'',$this->description);
    }
}