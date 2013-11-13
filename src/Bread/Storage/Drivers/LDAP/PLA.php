<?php

namespace Bread\Storage\Drivers\LDAP;
use Bread\Promises\When;
use Doctrine\Common\Cache;

class PLA
{
    const TYPE_DN = 'Distinguished Name';
    const TYPE_GENERALIZED_TIME = 'Generalized Time';
    const FORMAT_GENERALIZED_TIME = 'YmdHisZ';

    protected $link;

    protected $cache;

    public function __construct($link)
    {
        $this->link = $link;
        $this->cache = new Cache\ArrayCache();
    }


    public function getSchemaAttribute($attribute_name)
    {
        if($attribute = $this->cache->fetch($attribute_name)){
            return $attribute;
        } else {
            $schemas = $this->getRawSchema(array(
                    'schema_dn' => 'cn=Subschema',
                    'schema_filter' => '(objectClass=*)',
                    'schema_to_fetch' => 'attributeTypes'
                ));
            $syntaxes = $this->SchemaSyntaxes();
            foreach ($schemas as $line) {
                if (strpos($line, "'$attribute_name'") !== false) {
                    $attribute = new AttributeType($line);
                    if (isset($syntaxes[$attribute->syntax])) {
                        $attribute->setType($syntaxes[$attribute->syntax]->description);
                    }
                    if ($aliases = $attribute->aliases) {
                        if (in_array($attribute_name, $aliases)) {
                            $attribute->changeAliases($attribute_name);
                        }
                    }
                    if ($sup_attr_name = $attribute->sup_attribute) {
                        $attribute->setSupAttribute($this->getSchemaAttribute($sup_attr_name));
                    }
                    $this->cache->save($attribute_name, $attribute);
                    return $attribute;
                }
            }
        }
    }

    public function getSchemaObjectClass($class_name)
    {
        if($objClass = $this->cache->fetch($class_name)){
            return $objClass;
        } else {
            $schemas = $this->getRawSchema(array(
                'schema_dn' => 'cn=Subschema',
                'schema_filter' => '(objectClass=*)',
                'schema_to_fetch' => 'objectclasses'
            ));
            $children = array();
            foreach ($schemas as $line) {
                if (strpos($line, "'$class_name'") !== false) {
                    $object_class = new ObjectClass($line);
                    //parents
                    if ($sup_classes = $object_class->sup_classes) {
                        $object_class->resetSupClasses();
                        foreach ($sup_classes as $sup_class) {
                            $object_class->addSupClass($this->getSchemaObjectClass($sup_class));
                        }
                    }
                    //must attributes
                    $must_attrs = $object_class->must_attrs;
                    $object_class->resetMustAttributes();
                    foreach ($must_attrs as $attribute){
                        $object_class->addMustAttribute($this->getSchemaAttribute($attribute));
                    }
                    //may attributes
                    $may_attrs = $object_class->may_attrs;
                    $object_class->resetMayAttributes();
                    foreach ($may_attrs as $attribute){
                        $object_class->addMayAttribute($this->getSchemaAttribute($attribute));
                    }
                    $this->cache->save($class_name, $object_class);
                    return $object_class;
                }
            }
        }
    }

    protected function getRawSchema(array $search)
    {
        if($schema = $this->cache->fetch($search['schema_to_fetch'])){
            return $schema;
        } else {
            $schema_search = @ldap_read($this->link, $search['schema_dn'], $search['schema_filter'], array(
                strtolower($search['schema_to_fetch'])
            ), false, 0, 10, LDAP_DEREF_NEVER);
            $schema_entries = @ldap_get_entries($this->link, $schema_search);
            $schema = $schema_entries[0][strtolower($search['schema_to_fetch'])];
            unset($schema['count']);
            $this->cache->save($search['schema_to_fetch'], $schema);
            return $schema;
        }
    }

    protected function SchemaSyntaxes()
    {
        $schemas = $this->getRawSchema(array(
            'schema_dn' => 'cn=Subschema',
            'schema_filter' => '(objectClass=*)',
            'schema_to_fetch' => 'ldapSyntaxes'
        ));
        $syntaxes = array();
        foreach ($schemas as $line) {
            $syntax = new Syntax($line);
            $syntaxes[strtolower(trim($syntax->oid))] = $syntax;
        }
        return $syntaxes;
    }
}