<?php
namespace Bread\Storage\Drivers\LDAP;

/**
 * Represents an LDAP Syntax
 */
class Syntax extends SchemaItem {
    protected $x_binary_transfer_required;
    protected $x_not_human_readable;
    /**
     * Creates a new Syntax object from a raw LDAP syntax string.
     */
    public function __construct($class) {
        $regex = "/(?<oid>[\S]*){1}"
            . "( DESC '(?<description>[^']*)')?"
            . "( X-BINARY-TRANSFER-REQUIRED '(?<required>[^']*)')?"
            . "( X-NOT-HUMAN-READABLE '(?<human>[^']*)')?/";
        preg_match($regex, trim(trim(trim($class,"("),")")), $matches);
        foreach (array_filter($matches) as $attribute => $value) {
            switch ($attribute) {
                case 'oid':
                    $this->setOID($value);
                    break;
                case 'description':
                case 'x_binary_transfer_required':
                case 'x_not_human_readable':
                case 'description':
                    $this->$attribute = $value;
                    break;
            }
        }
    }
}