<?php

namespace Bread\Storage\Drivers;

use Bread\Storage\Driver;
use Bread\Storage\Interfaces\Driver as DriverInterface;
use Bread\Storage\Hydration\Instance;
use Bread\Storage\Hydration\Map;
use Bread\Storage\Reference;
use Bread\Storage\Manager;
use Bread\Promises\When;
use Bread\Promises\Interfaces\Promise;
use Bread\Configuration\Manager as Configuration;
use Exception;
use Bread\Storage\Exceptions\UnsupportedOption;
use Bread\Storage\Exceptions\UnsupportedLogic;
use Bread\Storage\Exceptions\UnsupportedCondition;
use Bread\Storage\Collection;
use ReflectionClass;
use DateTime;
use DateInterval;
use Bread\Storage\Drivers\LDAP\AttributeType;

class LDAP extends Driver implements DriverInterface
{
    const DEFAULT_PORT = 389;
    const DATETIME_FORMAT = 'U';
    const FILTER_ALL = 'objectClass=*';
    const ATTRSONLY = 0;
    const SIZELIMIT = 0;
    const TIMELIMIT = 0;
    const OBJECTID_FIELD_NAME = 'dn';
    const LDAP_SUCCESS = 0x00;
    const LDAP_OPERATIONS_ERROR = 0x01;
    const LDAP_PROTOCOL_ERROR = 0x02;
    const LDAP_TIMELIMIT_EXCEEDED = 0x03;
    const LDAP_SIZELIMIT_EXCEEDED = 0x04;
    const LDAP_COMPARE_FALSE = 0x05;
    const LDAP_COMPARE_TRUE = 0x06;
    const LDAP_AUTH_METHOD_NOT_SUPPORTED = 0x07;
    const LDAP_STRONG_AUTH_REQUIRED = 0x08;
    const LDAP_PARTIAL_RESULTS = 0x09;
    const LDAP_REFERRAL = 0x0a;
    const LDAP_ADMINLIMIT_EXCEEDED = 0x0b;
    const LDAP_UNAVAILABLE_CRITICAL_EXTENSION = 0x0c;
    const LDAP_CONFIDENTIALITY_REQUIRED = 0x0d;
    const LDAP_SASL_BIND_INPROGRESS = 0x0e;
    const LDAP_NO_SUCH_ATTRIBUTE = 0x10;
    const LDAP_UNDEFINED_TYPE = 0x11;
    const LDAP_INAPPROPRIATE_MATCHING = 0x12;
    const LDAP_CONSTRAINT_VIOLATION = 0x13;
    const LDAP_TYPE_OR_VALUE_EXISTS = 0x14;
    const LDAP_INVALID_SYNTAX = 0x15;
    const LDAP_NO_SUCH_OBJECT = 0x20;
    const LDAP_ALIAS_PROBLEM = 0x21;
    const LDAP_INVALID_DN_SYNTAX = 0x22;
    const LDAP_IS_LEAF = 0x23;
    const LDAP_ALIAS_DEREF_PROBLEM = 0x24;
    const LDAP_INAPPROPRIATE_AUTH = 0x30;
    const LDAP_INVALID_CREDENTIALS = 0x31;
    const LDAP_INSUFFICIENT_ACCESS = 0x32;
    const LDAP_BUSY = 0x33;
    const LDAP_UNAVAILABLE = 0x34;
    const LDAP_UNWILLING_TO_PERFORM = 0x35;
    const LDAP_LOOP_DETECT = 0x36;
    const LDAP_SORT_CONTROL_MISSING = 0x3C;
    const LDAP_INDEX_RANGE_ERROR = 0x3D;
    const LDAP_NAMING_VIOLATION = 0x40;
    const LDAP_OBJECT_CLASS_VIOLATION = 0x41;
    const LDAP_NOT_ALLOWED_ON_NONLEAF = 0x42;
    const LDAP_NOT_ALLOWED_ON_RDN = 0x43;
    const LDAP_ALREADY_EXISTS = 0x44;
    const LDAP_NO_OBJECT_CLASS_MODS = 0x45;
    const LDAP_RESULTS_TOO_LARGE = 0x46;
    const LDAP_AFFECTS_MULTIPLE_DSAS = 0x47;
    const LDAP_OTHER = 0x50;
    const LDAP_SERVER_DOWN = 0x51;
    const LDAP_LOCAL_ERROR = 0x52;
    const LDAP_ENCODING_ERROR = 0x53;
    const LDAP_DECODING_ERROR = 0x54;
    const LDAP_TIMEOUT = 0x55;
    const LDAP_AUTH_UNKNOWN = 0x56;
    const LDAP_FILTER_ERROR = 0x57;
    const LDAP_USER_CANCELLED = 0x58;
    const LDAP_PARAM_ERROR = 0x59;
    const LDAP_NO_MEMORY = 0x5a;
    const LDAP_CONNECT_ERROR = 0x5b;
    const LDAP_NOT_SUPPORTED = 0x5c;
    const LDAP_CONTROL_NOT_FOUND = 0x5d;
    const LDAP_NO_RESULTS_RETURNED = 0x5e;
    const LDAP_MORE_RESULTS_TO_RETURN = 0x5f;
    const LDAP_CLIENT_LOOP = 0x60;
    const LDAP_REFERRAL_LIMIT_EXCEEDED = 0x61;

    protected $link;
    protected $base;
    protected $filter;
    protected $pla;
    protected $params;
    protected $sortFlags = array(
        'integer' => SORT_NUMERIC,
        'float' => SORT_NUMERIC,
        'string' => 10,
        'text' => SORT_FLAG_CASE
    );

    public function __construct($uri, array $options = array(), $domain = '__default__')
    {
        $this->params = array_merge(array(
          'host' => 'localhost',
          'port' => self::DEFAULT_PORT,
          'query' => self::FILTER_ALL
        ), parse_url($uri));
        $options = array_merge(array(
            'cache' => true,
            'debug' => false
        ), $options);
        $this->domain = $domain;
        $this->connect();
        $this->base = ltrim($this->params['path'], '/');
        parse_str($this->params['query'], $this->filter);
        $this->hydrationMap = new Map();
        $this->useCache = $options['cache'];
        $this->pla = new LDAP\PLA($this->link, $this->base);
    }

    public function __destruct() {
        ldap_close($this->link);
    }

    protected function connect()
    {
        if (!$this->link || !@ldap_get_option($this->link, LDAP_OPT_PROTOCOL_VERSION)) {
            if (!$this->link = ldap_connect($this->params['host'], $this->params['port'])) {
                throw new Exception("Cannot connect to LDAP server {$this->params['host']}");
            }
            ldap_set_option($this->link, LDAP_OPT_PROTOCOL_VERSION, 3);
            if (isset($this->params['user']) && isset($this->params['pass'])) {
                ldap_bind($this->link, $this->params['user'], $this->params['pass']);
            }
        }
    }

    public function store($object, $oid = null)
    {
        $this->connect();
        $instance = $this->hydrationMap->getInstance($object);
        $class = $instance->getClass();
        if (Configuration::get($class, 'storage.options.readonly', $this->domain)) {
            return When::resolve($object);
        }
        switch ($instance->getState()) {
            case Instance::STATE_NEW:
                foreach ((array) Configuration::get($class, 'properties', $this->domain) as $property => $options) {
                    foreach ((array) Configuration::get($class, "properties.$property.strategy", $this->domain) as $strategy => $callback) {
                        if(!$value = $object->$property) {
                            switch ($strategy) {
                                case 'autoincrement':
                                    if ($callback && is_null($object->$property)) {
                                        $autoincrement = (int) $this->autoincrement($class);
                                        if (is_callable($callback)) {
                                            $value = call_user_func($callback, $autoincrement);
                                        }
                                    }
                                    break;
                            }
                        }
                        $instance->setProperty($object, $property, $value);
                    }
                }
                $oid = $oid ? : $this->generateDN($object);
                $instance->setObjectId($oid);
                break;
            case Instance::STATE_MANAGED:
                $oid = $instance->getObjectId();
                break;
            default:
                throw new Exception('Object instance cannot be stored');
        }
        switch ($instance->getState()) {
            case Instance::STATE_NEW:
                return $this->denormalize($instance->getProperties($object), $class)->then(function ($properties) use ($instance, $object, $oid, $class) {
                    if (!$objectClass = Configuration::get($class, 'storage.options.objectClass', $this->domain)) {
                        throw new Exception("Missing 'storage.options.objectClass' option");
                    }
                    $properties['objectClass'] = $objectClass;
                    if (!ldap_add($this->link, $oid, array_filter($properties))) {
                        throw new Exception(ldap_error($this->link));
                    }
                    $this->invalidateCacheFor($instance->getClass());
                    $instance->setState(Instance::STATE_MANAGED);
                    return $this->getObject($class, $oid, $instance);
                });
            case Instance::STATE_MANAGED:
                return $this->denormalize($instance->getModifiedProperties($object), $class)->then(function ($properties) use ($instance, $object, $oid, $class) {
                    if (!ldap_modify($this->link, $oid, array_filter($properties, function ($value) {
                      return $value !== null;
                    }))) {
                        throw new Exception(ldap_error($this->link));
                    }
                    $this->invalidateCacheFor($instance->getClass());
                    return $this->getObject($class, $oid, $instance);
                });
        }

    }

    public function delete($object)
    {
        $this->connect();
        $instance = $this->hydrationMap->getInstance($object);
        switch ($instance->getState()) {
          case Instance::STATE_NEW:
              $instance->setState(Instance::STATE_DELETED);
              // fallback to next case
          case Instance::STATE_DELETED:
              break;
          case Instance::STATE_MANAGED:
              $oid = $instance->getObjectId();
              $this->deleteCascade($object);
              if (!ldap_delete($this->link, $oid)) {
                  throw new Exception(ldap_error($this->link));
              }
              $instance->setState(Instance::STATE_DELETED);
              $this->invalidateCacheFor($instance->getClass());
              break;
        }
        return When::resolve($object);
    }

    public function count($class, array $search = array(), array $options = array())
    {
        return $this->search($class, $search, $options)->then(function ($search) use($class, $options) {
            if (!$entry = ldap_first_entry($this->link, $search)) {
                return 0;
            }
            return count($this->applyOptions($class, $entry, $options));
        });
    }

    public function first($class, array $search = array(), array $options = array())
    {
        $options['limit'] = 1;
        return $this->fetch($class, $search, $options)->then(function ($results) {
            return current($results) ? : When::reject(new Exception('Model not found.'));
        });
    }

    public function fetch($class, array $search = array(), array $options = array())
    {
        // TODO Cache $oids
        //return $this->fetchFromCache($class, $search, $options)->then(null, function ($cacheKey) use ($class, $search, $options) {
            return $this->search($class, $search, $options)->then(function ($search) use($class, $options) {
                $promises = array();
                if (!$entry = ldap_first_entry($this->link, $search)) {
                    return $promises;
                }
                foreach($this->applyOptions($class, $entry, $options) as $oid=>$attributes) {
                    if ($object = $this->hydrationMap->objectExists($class, $oid)) {
                        $promises[$oid] = When::resolve($object);
                    } else {
                        $promises[$oid] = $this->createObjectPlaceholder($class, $oid)->then(function($object) use ($attributes, $class, $oid) {
                            return $this->hydrateObject($object, $attributes, $class, $oid);
                        });
                    }
                }
                return When::all($promises);
            /*})->then(function ($objects) use ($cacheKey) {
                return $this->storeToCache($cacheKey, $objects);
            });*/
        })->then(array($this, 'buildCollection'));
    }

    public function getObject($class, $oid, $instance = null)
    {
        if (!$object = $this->hydrationMap->objectExists($class, $oid)) {
            $object = $this->createObjectPlaceholder($class, $oid, $instance)->then(function ($object) use ($class, $oid) {
                return $this->fetchPropertiesFromCache($class, $oid)->then(null, function ($cacheKey) use ($class, $oid) {
                    $this->connect();
                    $attributes = $this->filterAttributes($class);
                    $attrsonly = static::ATTRSONLY;
                    $sizelimit = static::SIZELIMIT;
                    $timelimit = static::TIMELIMIT;
                    $read = ldap_read($this->link, $oid, self::FILTER_ALL, $attributes, $attrsonly, $sizelimit, $timelimit);
                    $entry = ldap_first_entry($this->link, $read);
                    $values = $this->getAttributes($entry, $class);
                    return $this->storePropertiesToCache($cacheKey, $values);
                })->then(function ($values) use ($object, $class, $oid) {
                    return $this->hydrateObject($object, $values, $class, $oid);
                });
            });
        }
        return ($object instanceof Promise) ? $object : When::resolve($object);
    }

    protected function getAttributes($entry, $class)
    {
        if ($attributes = ldap_get_attributes($this->link, $entry)) {
            if ($attributes = $this->normalizeAttributes($attributes, $class)) {
                return $attributes;
            }
        }
        return false;
    }

    protected function getEntry($object, $entry, $class, $oid)
    {
        if ($attributes = $this->getAttributes($entry, $class)) {
            return $this->hydrateObject($object, $attributes, $class, $oid);
        }
        return false;
    }

    public function purge($class, array $search = array(), array $options = array())
    {}

    protected function search($class, array $search = array(), array $options = array())
    {
        $this->connect();
        $filter = array_merge($this->filter, array(
            'objectClass' => array('$all' => Configuration::get($class, 'storage.options.objectClass', $this->domain)
        )));
        return $this->denormalizeSearch($class, array($filter, $search))->then(function ($filter) use ($class, $options) {
            $attributes = $this->filterAttributes($class);
            $attrsonly = static::ATTRSONLY;
            $sizelimit = static::SIZELIMIT;
            $timelimit = static::TIMELIMIT;
            $search = ldap_search($this->link, $this->getBase($class), "({$filter})", $attributes, $attrsonly, $sizelimit, $timelimit, LDAP_DEREF_ALWAYS);
            foreach ($options as $option => $value) {
                switch ($option) {
                  case 'sort':
                  case 'limit':
                  case 'skip':
                      break;
                  default:
                      throw new UnsupportedOption(__CLASS__, $option);
                }
            }
            return $search;
        });
    }

    protected function filterAttributes($class)
    {
        $reflector = new ReflectionClass($class);
        $attributes = array();
        foreach ($reflector->getProperties() as $property) {
            $attributes[] = $property->name;
        }
        return $attributes;
    }

    protected function applyOptions($class, $entry, array $options = array())
    {
        $results = array();
        $promises = array();
        do {
            $results[ldap_get_dn($this->link, $entry)] = $this->getAttributes($entry, $class);
        } while ($entry = ldap_next_entry($this->link, $entry));
        if (isset($options['sort'])) {
            $multisort = array();
            foreach($results as $k=>$v) {
                foreach ($options['sort'] as $attribute => $order) {
                    $sort[$attribute][$k] = isset($v[$attribute]) ? $v[$attribute] : null;
                }
            }
            foreach ($options['sort'] as $attribute => $order) {
                $multisort[] = $sort[$attribute];
                $multisort[] = $order > 0 ? SORT_ASC : SORT_DESC;
                $multisort[] = isset($this->sortFlags[Configuration::get($class, "properties.$attribute.type", $this->domain)]) ? $this->sortFlags[Configuration::get($class, "properties.$attribute.type", $this->domain)] : SORT_REGULAR;
            }
            $multisort[] = &$results;
            call_user_func_array('array_multisort', $multisort);
        }
        $indexes = array_keys($results);
        $skip = isset($options['skip']) ? $options['skip'] : 0;
        $limit = isset($options['limit']) ? $options['limit'] : count($results);
        foreach(array_slice($indexes, $skip, $limit) as $oid) {
            $promises[$oid] = $results[$oid];
        }
        return $promises;
    }

    protected function normalizeAttributes(array $attributes, $class)
    {
        $normalizedAttributes = array();
        foreach ($attributes as $property => $array) {
            if (!is_string($property) || !is_array($array)) {
                continue;
            }
            unset($array['count']);
            if (Configuration::get($class, "properties.$property.multiple", $this->domain)) {
                $normalizedAttributes[$property] = $array;
            } else {
                $normalizedAttributes[$property] = array_shift($array);
            }
        }
        return $normalizedAttributes;
    }

    protected function normalizeValue($name, $value, $class)
    {
        if (Reference::is($value)) {
            return Reference::fetch($value, $this->domain);
        }
        $type = Configuration::get($class, "properties.$name.type", $this->domain);
        switch ($type) {
            case 'integer':
                return (int) $value;
            case 'float':
                return (float) $value;
            case 'binary':
                return base64_encode($value);
            case 'DateTime':
                /* FIXME Too slow*/
                $attributeType = $this->pla->getSchemaAttribute($name);
                switch ($attributeType->getSyntax()) {
                  case AttributeType::TYPE_GENERALIZED_TIME:
                      $dateTimeFormat = AttributeType::FORMAT_GENERALIZED_TIME;
                      break;
                  default:
                      $dateTimeFormat = self::DATETIME_FORMAT;
                }
//                 $dateTimeFormat = self::DATETIME_FORMAT;
                return When::resolve(DateTime::createFromFormat($dateTimeFormat, $value));
            default:
                /* FIXME Too slow */
                $attributeType = $this->pla->getSchemaAttribute($name);
                switch ($attributeType->getSyntax()) {
                  case AttributeType::TYPE_DN:
                      // TODO Enforce check on LDAP driver/same instance?
                      // FIXME $type could be abstract, how to infer class from DN?
                      return $this->getObject($type, $value);
                }
                if (is_array($value)) {
                    $normalizedValues = array();
                    foreach ($value as $v) {
                        $normalizedValues[] = $this->normalizeValue($name, $v, $class);
                    }
                    return When::all($normalizedValues);
                }
                return When::resolve($value);
        }
    }

    protected function denormalize($values, $class)
    {
        $promises = array();
        foreach ($values as $field => $value) {
            $promises[$field] = $this->denormalizeValue($value, $field, $class);
        }
        return When::all($promises);
    }

    protected function denormalizeValue($value, $field, $class)
    {
        if ($value instanceof Promise) {
            return $value->then(function($value) use ($field, $class) {
                return $this->denormalizeValue($value, $field, $class);
            });
        } elseif (Reference::is($value)) {
            return When::resolve((string) $value);
        } elseif (is_object($value)) {
            $type = Configuration::get($class, "properties.$field.type", $this->domain);
            if ($value instanceof DateTime) {
                $attributeType = $this->pla->getSchemaAttribute($field);
                switch ($attributeType->getSyntax()) {
                    case AttributeType::TYPE_GENERALIZED_TIME:
                        $dateTimeFormat = AttributeType::FORMAT_GENERALIZED_TIME;
                        break;
                    default:
                        $dateTimeFormat = self::DATETIME_FORMAT;
                }
                return When::resolve($value->format($dateTimeFormat));
            } else {
                $driver = Manager::driver(get_class($value), $this->domain);
                return $driver->store($value)->then(function($object) use ($driver, $field) {
                    $attributeType = $this->pla->getSchemaAttribute($field);
                    switch ($attributeType->getSyntax()) {
                      case AttributeType::TYPE_DN:
                          // TODO Enforce check on LDAP driver/same instance?
                          return $this->generateDN($object);
                      default:
                          return (string) new Reference($object, $this->domain);
                    }
                });
            }
        } elseif (is_array($value)) {
            $denormalizedValuePromises = array();
            foreach ($value as $k => $v) {
                $denormalizedValuePromises[$k] = $this->denormalizeValue($v, $field, $class);
            }
            return When::all($denormalizedValuePromises);
        } else {
            return When::resolve($value);
        }
    }

    protected function denormalizeSearch($class, $search, $logic = '$and')
    {
        $filters = array();
        foreach ($search as $conditions) {
            $promises = array();
            foreach ($conditions as $property => $condition) {
                switch ($property) {
                  case '$and':
                  case '$or':
                  case '$nor':
                      $filters[] = $this->denormalizeSearch($class, $condition, $property);
                      continue 2;
                  default:
                      $promises[] = $this->denormalizeCondition($class, $property, $condition);
                }
            }
            $filters[] = When::all($promises, function($conditions) {
                return $this->buildFilter($conditions);
            });
        }
        switch (count($filters)) {
          case 0:
              return self::FILTER_ALL;
          case 1:
              return array_shift($filters);
          default:
              return When::all($filters, function($conditions) use ($logic) {
                  return $this->buildFilter($conditions, $logic);
              });
        }
    }

    protected function denormalizeCondition($class, $property, $condition, $negate = false)
    {
        if ($reference = Reference::is($condition)) {
            $condition = Reference::fetch($reference, $this->domain);
        } elseif (is_array($condition)) {
            foreach ($condition as $k => $v) {
                $op = '=';
                switch ($k) {
                    case '$in':
                        return $this->denormalizeValue($v, $property, $class)->then(function ($v) use ($property) {
                            return $this->buildFilter(array_map(function($value) use ($property) {
                                return $this->buildCondition($property, $value);
                            }, $v), '$or');
                        });
                    case '$nin':
                        return $this->denormalizeValue($v, $property, $class)->then(function ($v) use ($property) {
                            return $this->buildFilter(array_map(function($value) use ($property) {
                                return $this->buildCondition($property, $value);
                            }, $v), '$or', true);
                        });
                    case '$lt':
                        $op = '<';
                        break;
                    case '$lte':
                        $op = '<=';
                        break;
                    case '$gt':
                        $op = '>';
                        break;
                    case '$gte':
                        $op = '>=';
                        break;
                    case '$ne':
                        $negate = true;
                        break;
                    case '$all':
                        return $this->denormalizeValue($v, $property, $class)->then(function ($v) use ($property) {
                            return $this->buildFilter(array_map(function($value) use ($property) {
                                return $this->buildCondition($property, $value);
                            }, $v), '$and');
                        });
                    case '$not':
                        return $this->denormalizeCondition($class, $property, $v, true);
                    case '$regex':
                    case '$maxDistance':
                    case '$near':
                    case '$within':
                    case '$xeger':
                        throw new UnsupportedCondition(__CLASS__, $k);
                }
                return $this->denormalizeValue($v, $property, $class)->then(function ($value) use ($property, $op, $negate) {
                    // TODO Manage null values
                    return $this->buildCondition($property, $value, $op, $negate);
                });
            }
        }
        // TODO Check if this is redundant with the other one
        return $this->denormalizeValue($condition, $property, $class)->then(function ($value) use ($property, $negate) {
            return $this->buildCondition($property, $value, '=', $negate);
        });
    }

    protected function buildFilter(array $conditions, $logic = '$and', $negate = false)
    {
        switch ($logic) {
          case '$and':
              $logic = '&';
              break;
          case '$or':
              $logic = '|';
              break;
          case '$nor':
              $logic = '|';
              $negate = true;
              break;
          default:
              throw new UnsupportedLogic(__CLASS__, $logic);

        }
        if (!$conditions) {
            return null;
        }
        $filter = "{$logic}(" . implode(')(', array_filter($conditions)) . ")";
        return $negate ? "!($filter)" : $filter;
    }

    protected function buildCondition($property, $value, $operator = '=', $negate = false)
    {
        $condition = $property . $operator . $value;
        return $negate ? "!($condition)" : $condition;
    }

    protected function getBase($class)
    {
        $base = array(
            Configuration::get($class, 'storage.options.base', $this->domain),
            $this->base
        );
        return implode(',', $base);
    }

    protected function generateDN($object)
    {
        $class = get_class($object);
        if (!$rdn = Configuration::get($class, 'storage.options.rdn', $this->domain)) {
            throw new Exception("Option 'rdn' mandatory to store $class with " . __CLASS__);
        }
        $dn = array();
        $dn[] = "{$rdn}={$object->$rdn}";
        $dn[] = $this->getBase($class);
        return implode(',', $dn);
    }

    /*
     * PHP LDAP driver does not support atomic operations,
     * therefore inconsistencies may arise using autoincrement values.
     *
     * TODO Workaround using shell's ldapmodify delete/add operations
     */
    protected function autoincrement($class)
    {
        $dn = $this->getBase($class);
        $search = ldap_search($this->link, $dn, '(objectClass=breadSequence)');
        $entry = ldap_first_entry($this->link, $search);
        $sequence = ldap_get_attributes($this->link, $entry);
        $sequenceNumber = (int) $sequence['breadSequenceNumber'][0];
        ldap_modify($this->link, $dn, array(
            'breadSequenceNumber' => array(
                $sequenceNumber + 1
            )
        ));
        return $sequenceNumber;
    }

    protected function littleEndian($hex) {
        $result = '';
        for ($x = strlen($hex) - 2; $x >= 0; $x = $x - 2) {
            $result .= substr($hex, $x, 2);
        }
        return $result;
    }

    protected function binSIDtoText($binsid, $pop = true) {
        $hex_sid = bin2hex($binsid);
        $rev = hexdec(substr($hex_sid, 0, 2)); // Get revision-part of SID
        $subcount = hexdec(substr($hex_sid, 2, 2)); // Get count of sub-auth entries
        $auth = hexdec(substr($hex_sid, 4, 12)); // SECURITY_NT_AUTHORITY
        $result = "$rev-$auth";
        for ($x = 0; $x < $subcount; $x++) {
            $subauth[$x] = hexdec($this->littleEndian(substr($hex_sid, 16 + ($x * 8), 8))); // get all SECURITY_NT_AUTHORITY
            $result .= sprintf('-%s', $subauth[$x]);
        }
        $parts = explode('-', $result);
        return $pop ? array_pop($parts) : $result;
    }
}
