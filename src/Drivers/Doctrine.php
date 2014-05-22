<?php
namespace Bread\Storage\Drivers;

use Bread\Storage\Driver;
use Bread\Storage\Drivers\Doctrine\Types\BreadDateTime;
use Bread\Storage\Interfaces\Driver as DriverInterface;
use Bread\Storage\Hydration\Instance;
use Bread\Storage\Hydration\Map;
use Bread\Storage\Collection;
use Bread\Storage\Reference;
use Bread\Storage\Manager;
use Bread\Promises\When;
use Bread\Promises\Interfaces\Promise;
use Bread\Configuration\Manager as Configuration;
use Exception;
use Doctrine\DBAL\Connection;
use Doctrine\DBAL\DriverManager;
use Doctrine\DBAL\Types\Type;
use ReflectionClass;
use Bread\Types;
use DateTime;
use DateInterval;
use Doctrine\Common\Cache\FilesystemCache;
use Doctrine\Common\Cache\ArrayCache;
use PDO;

class Doctrine extends Driver implements DriverInterface
{
    const INDEX_TABLE = '_index';
    const OBJECTID_FIELD_NAME = '_id';
    const OBJECTID_FIELD_TYPE = 'guid';
    const MULTIPLE_PROPERTY_INDEX_FIELD_NAME = '_key';
    const MULTIPLE_PROPERTY_INDEX_FIELD_TYPE = Type::INTEGER;
    const MULTIPLE_PROPERTY_TABLE_SEPARATOR = '-';
    const TAGGABLE_PROPERTY_INDEX_FIELD_NAME = '_tag';
    const TAGGABLE_PROPERTY_INDEX_FIELD_TYPE = Type::STRING;
    const TAGGABLE_PROPERTY_TABLE_SEPARATOR = '-';

    protected $link;

    protected $schemaManager;

    protected $cache;

    protected $params;

    protected $options;

    // TODO Move to configuration?
    protected static $typesMap = array(
        'boolean' => Type::BOOLEAN,
        'integer' => Type::BIGINT,
        'float' => Type::FLOAT,
        'string' => Type::STRING,
        'text' => Type::TEXT,
        'array' => Type::TARRAY,
        'object' => Type::BLOB,
        'resource' => Type::BLOB,
        'DateTime' => Type::DATETIME,
        'DateInterval' => Type::INTEGER,
        'Bread\Types\DateTime' => BreadDateTime::NAME,
        'Bread\Types\DateInterval' => Type::INTEGER
    );

    public function __construct($uri, array $options = array(), $domain = '__default__')
    {
        $this->options = array_merge(array(
            'cache' => false,
            'debug' => false,
            'charset' => 'utf8'
        ), $options);
        $this->domain = $domain;
        $scheme = parse_url($uri, PHP_URL_SCHEME);
        switch ($scheme) {
            case 'sqlite':
                $this->params = array(
                    'user' => parse_url($uri, PHP_URL_USER),
                    'password' => parse_url($uri, PHP_URL_PASS),
                    'path' => parse_url($uri, PHP_URL_PATH),
                    'charset' => $this->options['charset'],
                    'driver' => 'pdo_sqlite'
                );
                break;
            case 'mysql':
                $this->params = array(
                    'user' => parse_url($uri, PHP_URL_USER),
                    'password' => parse_url($uri, PHP_URL_PASS),
                    'host' => parse_url($uri, PHP_URL_HOST),
                    'port' => parse_url($uri, PHP_URL_PORT),
                    'dbname' => ltrim(parse_url($uri, PHP_URL_PATH), '/'),
                    'charset' => $this->options['charset'],
                    'driver' => 'pdo_mysql'
                );
                break;
            case 'pgsql':
                $this->params = array(
                    'user' => parse_url($uri, PHP_URL_USER),
                    'password' => parse_url($uri, PHP_URL_PASS),
                    'host' => parse_url($uri, PHP_URL_HOST),
                    'port' => parse_url($uri, PHP_URL_PORT),
                    'dbname' => ltrim(parse_url($uri, PHP_URL_PATH), '/'),
                    'charset' => $this->options['charset'],
                    'driver' => 'pdo_pgsql'
                );
                break;
            case 'sqlsrv':
                $this->params = array(
                    'user' => parse_url($uri, PHP_URL_USER),
                    'password' => parse_url($uri, PHP_URL_PASS),
                    'host' => parse_url($uri, PHP_URL_HOST),
                    'port' => parse_url($uri, PHP_URL_PORT),
                    'dbname' => ltrim(parse_url($uri, PHP_URL_PATH), '/'),
                    'charset' => $this->options['charset'],
                    'driver' => 'pdo_sqlsrv'
                );
                break;
            case 'db2':
                $this->params = array(
                    'user' => parse_url($uri, PHP_URL_USER),
                    'password' => parse_url($uri, PHP_URL_PASS),
                    'host' => parse_url($uri, PHP_URL_HOST),
                    'port' => parse_url($uri, PHP_URL_PORT),
                    'dbname' => ltrim(parse_url($uri, PHP_URL_PATH), '/'),
                    'charset' => $this->options['charset'],
                    //'driver' => 'pdo_ibm'
                    'driverClass' => 'Bread\Storage\Drivers\Doctrine\DB2v5r1Driver'
                );
                break;
            default:
                throw new Exception(sprintf('Scheme %s not supported by %s driver', $scheme, __CLASS__));
        }
        $this->hydrationMap = new Map();
        $this->cache = new ArrayCache(); //new FilesystemCache(sys_get_temp_dir());
        $this->useCache = $this->options['cache'];
        $this->connect();
        $this->registerTypes();
    }

    protected function connect()
    {
        if (!$this->link || !$this->link->ping()) {
            $this->link = DriverManager::getConnection($this->params);
            $config = $this->link->getConfiguration();
            if ($this->options['debug']) {
                $config->setSQLLogger(new \Doctrine\DBAL\Logging\EchoSQLLogger());
            }
            $this->schemaManager = $this->link->getSchemaManager();
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
              $oid = $oid ? : $this->generateObjectId();
              $instance->setObjectId($oid);
              break;
          case Instance::STATE_MANAGED:
              $oid = $instance->getObjectId();
              break;
          default:
              throw new Exception('Object instance cannot be stored');
        }
        $this->link->beginTransaction();
        return $this->denormalize($instance->getModifiedProperties($object), $class)->then(function ($properties) use ($instance, $object, $oid, $class) {
            $objectIdFieldName = Configuration::get($class, 'storage.options.oid', $this->domain) ? : self::OBJECTID_FIELD_NAME;
            $objectIdFieldIdentifier = $this->link->quoteIdentifier($objectIdFieldName);
            $tables = $this->tablesFor($class);
            foreach ($tables as $tableName) {
                list(, $propertyName) = explode(self::MULTIPLE_PROPERTY_TABLE_SEPARATOR, $tableName) + array(null, null);
                $columns = $this->columns($tableName);
                $tableName = $this->link->quoteIdentifier($tableName);
                if (!$values = array_intersect_key($properties, array_flip($columns))) {
                    continue;
                }
                $fields = array_keys($values);
                $types = array();
                $placeholders = array();
                foreach ($values as $k => $v) {
                    $placeholder = $this->link->quoteIdentifier($k);
                    $placeholders[$placeholder] = $v;
                    if (is_resource($v)) {
                        $types[$placeholder] = Type::getType(Type::BLOB)->getBindingType();
                    }
                }
                $isMultiple = $propertyName ? Configuration::get($class, "properties.$propertyName.multiple", $this->domain) : false;
                $isTaggable = $propertyName ? Configuration::get($class, "properties.$propertyName.taggable", $this->domain) : false;
                switch ($instance->getState()) {
                  case Instance::STATE_NEW:
                      if ($isMultiple) {
                          foreach ((array) $values[$propertyName] as $key => $value) {
                              $this->link->insert($tableName, array(
                                  $objectIdFieldIdentifier => $oid,
                                  $this->link->quoteIdentifier(self::MULTIPLE_PROPERTY_INDEX_FIELD_NAME) => $key,
                                  $this->link->quoteIdentifier($propertyName) => $value
                              ), $types);
                          }
                      } elseif ($isTaggable) {
                          foreach ((array) $values[$propertyName] as $key => $value) {
                              $this->link->insert($tableName, array(
                                  $objectIdFieldName => $oid,
                                  $this->link->quoteIdentifier(self::TAGGABLE_PROPERTY_INDEX_FIELD_NAME) => $key,
                                  $this->link->quoteIdentifier($propertyName) => $value
                              ), $types);
                          }
                      } else {
                          $placeholders[$objectIdFieldName] = $oid;
                          $this->link->insert($tableName, $placeholders, $types);
                          // TODO replace strategy with computed attributes outside storage
                          foreach ((array) Configuration::get($class, "properties", $this->domain) as $property => $options) {
                              switch (Configuration::get($class, "properties.$property.strategy", $this->domain)) {
                                case 'autoincrement':
                                    $instance->setProperty($object, $property, (int) $this->link->lastInsertId());
                                    break;
                              }
                          }
                      }
                      break;
                  case Instance::STATE_MANAGED:
                      if ($isMultiple) {
                          $existingQueryBuilder = $this->link->createQueryBuilder();
                          $objectIdExpr = $existingQueryBuilder->expr()->eq($objectIdFieldName, $existingQueryBuilder->createNamedParameter($oid));
                          $existingQueryBuilder->select(self::MULTIPLE_PROPERTY_INDEX_FIELD_NAME)->from($tableName, 't')
                            ->where($objectIdExpr);
                          $existingCount = $existingQueryBuilder->execute()->rowCount();
                          foreach ((array) $values[$propertyName] as $key => $value) {
                              if ($key >= $existingCount) {
                                  $this->link->insert($tableName, array(
                                      $objectIdFieldName => $oid,
                                      self::MULTIPLE_PROPERTY_INDEX_FIELD_NAME => $key,
                                      $propertyName => $value
                                  ), $types);
                              } else {
                                  $this->link->update($tableName, array(
                                      $propertyName => $value
                                  ), array(
                                      $objectIdFieldName => $oid,
                                      self::MULTIPLE_PROPERTY_INDEX_FIELD_NAME => $key
                                  ), $types);
                              }
                          }
                          $queryBuilder = $this->link->createQueryBuilder();
                          $objectIdExpr = $queryBuilder->expr()->eq($objectIdFieldName, $queryBuilder->createNamedParameter($oid));
                          $queryBuilder->delete($tableName)->where($objectIdExpr, $queryBuilder->expr()->gte(
                              self::MULTIPLE_PROPERTY_INDEX_FIELD_NAME,
                              $queryBuilder->createNamedParameter(count($values[$propertyName]), PDO::PARAM_INT)
                          ))->execute();
                      } elseif ($isTaggable) {
                          $existingQueryBuilder = $this->link->createQueryBuilder();
                          $existingQueryBuilder->select(self::TAGGABLE_PROPERTY_INDEX_FIELD_NAME)->from($tableName, 't')
                              ->where($existingQueryBuilder->expr()->eq($objectIdFieldName, $existingQueryBuilder->createNamedParameter($oid)));
                          $existingTags = $existingQueryBuilder->execute()->fetchColumn(1);
                          $newTags = array();
                          foreach ((array) $values[$propertyName] as $tag => $value) {
                              $newTags[] = $tag;
                              if (in_array($tag, $existingTags)) {
                                  $this->link->update($tableName, array(
                                      $propertyName => $value
                                  ), array(
                                      $objectIdFieldName => $oid,
                                      self::TAGGABLE_PROPERTY_INDEX_FIELD_NAME => $tag
                                  ), $types);
                              } else {
                                  $this->link->insert($tableName, array(
                                      $objectIdFieldName => $oid,
                                      self::TAGGABLE_PROPERTY_INDEX_FIELD_NAME => $key,
                                      $propertyName => $value
                                  ), $types);
                              }
                          }
                          $deletedTags = array_diff($existingTags, $newTags);
                          array_walk($deletedTags, function($tag) use ($oid) {
                              $this->link->delete($tableName, array(
                                  $objectIdFieldName => $oid,
                                  self::TAGGABLE_PROPERTY_INDEX_FIELD_NAME => $tag
                              ));
                          });
                      } else {
                          $this->link->update($tableName, $values, array($objectIdFieldName => $oid), $types);
                      }
                      break;
                }
            }
            return $this->getObject($class, $oid, $instance);
        })->then(function ($object) use ($instance) {
            $this->link->commit();
            switch ($instance->getState()) {
                case Instance::STATE_NEW:
                    $instance->setState(Instance::STATE_MANAGED);
                    break;
            }
            $this->invalidateCacheFor($instance->getClass());
            return $object;
        }, function (Exception $exception) {
            $this->link->rollBack();
            throw $exception;
        });
    }

    public function delete($object)
    {
        $this->connect();
        $instance = $this->hydrationMap->getInstance($object);
        $class = get_class($object);
        $objectIdFieldName = Configuration::get($class, 'storage.options.oid', $this->domain) ? : self::OBJECTID_FIELD_NAME;
        switch ($instance->getState()) {
            case Instance::STATE_NEW:
                $instance->setState(Instance::STATE_DELETED);
                // fallback to next case
            case Instance::STATE_DELETED:
                break;
            case Instance::STATE_MANAGED:
                $oid = $instance->getObjectId();
                $tableNames = $this->tablesFor($class);
                $this->deleteCascade($object);
                $this->link->delete(array_shift($tableNames), array($objectIdFieldName => $oid));
                $instance->setState(Instance::STATE_DELETED);
                $this->invalidateCacheFor($class);
                break;
        }
        return When::resolve($object);
    }

    public function count($class, array $search = array(), array $options = array())
    {
      return $this->select($class, $search, $options)->then(function($result) {
          return $result->rowCount();
      });
    }

    public function first($class, array $search = array(), array $options = array())
    {
        $options['limit'] = 1;
        return $this->fetch($class, $search, $options)->then(function ($results) use ($class) {
            return current($results) ? : When::reject(new Exception('Model not found.'));
        });
    }

    public function each($resolver, $class, array $search = array(), array $options = array())
    {
        $this->fetchFromCache($class, $search, $options)->then(null, function ($cacheKey) use ($class, $search, $options, $resolver) {
            return $this->select($class, $search, $options)->then(function ($result) {
                return $result->fetchAll(PDO::FETCH_COLUMN, 0);
            })->then(function ($oids) use ($cacheKey) {
                return $this->storeToCache($cacheKey, $oids);
            });
        })->then(function ($oids) use ($class, $resolver) {
            return When::all(array_map(function ($oid) use ($class, $resolver) {
                return $this->getObject($class, $oid)->then(function ($object) use ($resolver) {
                    $this->getHydrationMap()->detach($object);
                    $resolver->progress($object);
                });
            }, $oids));
        });
    }

    public function fetch($class, array $search = array(), array $options = array())
    {
        return $this->fetchFromCache($class, $search, $options)->then(null, function ($cacheKey) use ($class, $search, $options) {
            return $this->select($class, $search, $options)->then(function ($result) use($class){
                return $result->fetchAll(PDO::FETCH_COLUMN, 0);
            })->then(function ($oids) use ($cacheKey, $class) {
                return $this->storeToCache($cacheKey, $oids);
            });
        })->then(function ($oids) use ($class) {
            return When::all(array_map(function ($oid) use ($class) {
                return $this->getObject($class, $oid);
            }, $oids));
        })->then(array($this, 'buildCollection'));
    }

    public function getObject($class, $oid, $instance = null)
    {
        if (!$object = $this->hydrationMap->objectExists($class, $oid)) {
            $object = $this->createObjectPlaceholder($class, $oid, $instance)->then(function ($object) use ($class, $oid) {
                return $this->fetchPropertiesFromCache($class, $oid)->then(null, function ($cacheKey) use ($class, $oid) {
                    $tableNames = $this->tablesFor($class);
                    $tableName = $this->link->quoteIdentifier(array_shift($tableNames));
                    $tableAlias = $this->link->quoteIdentifier('t');
                    $objectIdFieldName = Configuration::get($class, 'storage.options.oid', $this->domain) ? : self::OBJECTID_FIELD_NAME;
                    $oidIdentifier = $this->link->quoteIdentifier($objectIdFieldName);
                    $propertiesQueryBuilder = $this->link->createQueryBuilder();
                    $values = $propertiesQueryBuilder->select('*')->from($tableName, $tableAlias)
                        ->where($propertiesQueryBuilder->expr()->eq($oidIdentifier, $propertiesQueryBuilder->createNamedParameter($oid)))
                        ->execute()->fetch(PDO::FETCH_ASSOC);
                    foreach ($tableNames as $multiplePropertyTableName) {
                        list(, $propertyName) = explode(self::MULTIPLE_PROPERTY_TABLE_SEPARATOR, $multiplePropertyTableName) + array(null, null);
                        $multiplePropertyTableName = $this->link->quoteIdentifier($multiplePropertyTableName);
                        $multiplePropertyQueryBuilder = $this->link->createQueryBuilder();
                        $values[$propertyName] = $multiplePropertyQueryBuilder->select($this->link->quoteIdentifier($propertyName))->from($multiplePropertyTableName, $tableAlias)
                            ->where($multiplePropertyQueryBuilder->expr()->eq($oidIdentifier, $multiplePropertyQueryBuilder->createNamedParameter($oid)))
                            ->execute()->fetchAll(PDO::FETCH_COLUMN);
                    }
                    if ($values === false) {
                        var_dump(sprintf("Object %s (%s) does not exist.", $oid, $class));
                        throw new Exception(sprintf("Object %s (%s) does not exist.", $oid, $class));
                    }
                    return $this->storePropertiesToCache($cacheKey, $values);
                })->then(function ($values) use ($object, $class, $oid) {
                    return $this->hydrateObject($object, $values, $class, $oid);
                });
            });
        }
        return ($object instanceof Promise) ? $object : When::resolve($object);
    }

    public function purge($class, array $search = array(), array $options = array())
    {}

    protected function select($class, array $search = array(), array $options = array())
    {
        $this->connect();
        $objectIdFieldName = Configuration::get($class, 'storage.options.oid', $this->domain) ? : self::OBJECTID_FIELD_NAME;
        $queryBuilder = $this->link->createQueryBuilder();
        $tableNames = $this->tablesFor($class);
        $tableName = $this->link->quoteIdentifier(array_shift($tableNames));
        $tableAlias = $this->link->quoteIdentifier('t');
        $oidIdentifier = $this->link->quoteIdentifier($objectIdFieldName);
        $projection = "$tableAlias.$oidIdentifier";
        $queryBuilder->select($projection)->groupBy($projection)->from($tableName, $tableAlias);
        foreach ($tableNames as $i => $joinTableName) {
            $joinTableAlias = $this->link->quoteIdentifier('j' . $i);
            $queryBuilder->leftJoin($tableAlias, $this->link->quoteIdentifier($joinTableName), $joinTableAlias, "$tableAlias.$oidIdentifier = $joinTableAlias.$oidIdentifier");
        }
        return $this->denormalizeSearch($queryBuilder, array($search), $class)->then(function($where) use ($queryBuilder, $options) {
            $queryBuilder->where($where);
            $this->applyOptions($queryBuilder, $options);
            return $queryBuilder->execute();
      });
    }

    protected function applyOptions($queryBuilder, $options)
    {
        foreach ($options as $option => $value) {
            switch ($option) {
              case 'sort':
                  foreach ($value as $sort => $order) {
                      $field = $this->link->quoteIdentifier($sort);
                      $queryBuilder->addOrderBy($field, $order > 0 ? 'ASC' : 'DESC');
                  }
                  break;
              case 'limit':
                  $queryBuilder->setMaxResults($value);
                  break;
              case 'skip':
                  $queryBuilder->setFirstResult($value);
                  break;
            }
        }
    }

    protected function normalizeValue($name, $value, $class)
    {
        if (Reference::is($value)) {
            return Reference::fetch($value, $this->domain);
        }
        if (is_array($value)) {
            $normalizedValues = array();
            foreach ($value as $v) {
                $normalizedValues[] = $this->normalizeValue($name, $v, $class);
            }
            return When::all($normalizedValues);
        }
        $type = Configuration::get($class, "properties.$name.type", $this->domain);
        switch ($type) {
          default:
              if (isset(self::$typesMap[$type])) {
                  $doctrineType = self::$typesMap[$type];
                  // FIXME workaround to make BIGINTs convert to int instead of string
                  if ($doctrineType === Type::BIGINT) {
                      $doctrineType = Type::INTEGER;
                  }
                  return When::resolve($this->link->convertToPHPValue($value, $doctrineType));
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
        $type = Configuration::get($class, "properties.$field.type", $this->domain);
        if ($value instanceof Promise) {
            return $value->then(function($value) use ($field, $class) {
                return $this->denormalizeValue($value, $field, $class)->then(null, function() {
                    return null;
                });
            });
        } elseif (Reference::is($value)) {
            return When::resolve((string) $value);
        } elseif (is_array($value) || $value instanceof Collection) {
            $denormalizedValuePromises = array();
            foreach ($value as $k => $v) {
                $denormalizedValuePromises[$k] = $this->denormalizeValue($v, $field, $class);
            }
            return When::all($denormalizedValuePromises);
        } elseif (isset(self::$typesMap[$type])) {
            $doctrineType = self::$typesMap[$type];
            return When::resolve($this->link->convertToDatabaseValue($value, $doctrineType));
        } elseif (is_object($value)) {
            if ($value instanceof DateInterval) {
                $denormalizedValue = (int) Types\DateInterval::calculateSeconds($value);
                return When::resolve($denormalizedValue);
            } else {
                // TODO Consider to store $value in Reference constructor (promises?)
                $driver = Manager::driver(get_class($value), $this->domain);
                $hydrationMap = $driver->getHydrationMap();
                $instance = $hydrationMap->getInstance($value);
                switch ($instance->getState()) {
                    case Instance::STATE_NEW:
                        return $driver->store($value)->then(function($object) {
                            return (string) new Reference($object, $this->domain);
                        });
                    default:
                        return When::resolve((string) new Reference($value, $this->domain));
                }
            }
        } else {
            return When::resolve($value);
        }
    }

    protected function denormalizeSearch($queryBuilder, $search, $class, $logic = '$and')
    {
        $where = array();
        foreach ($search as $conditions) {
            $promises = array();
            foreach ($conditions as $property => $condition) {
                switch ($property) {
                  case '$and':
                  case '$or':
                  case '$nor':
                      $where[] = $this->denormalizeSearch($queryBuilder, $condition, $class, $property);
                      continue 2;
                  case '$match':
                      // TODO Check platform support
                      $fields = $condition['$fields'];
                      $against = $condition['$against'];
                      $fieldsIdentifiers = array_map(function ($field) {
                          return $this->link->quoteIdentifier($field);
                      }, $fields);
                      $match = implode(', ', $fieldsIdentifiers);
                      $againstPlaceholder = $queryBuilder->createNamedParameter($against);
                      $resolve = "MATCH ({$match}) AGAINST ({$againstPlaceholder} IN BOOLEAN MODE)";
                      $where[] = $resolve;
                      continue 2;
                  default:
                      $promises[] = $this->denormalizeCondition($queryBuilder, $property, $condition, $class);
                }
            }
            $where[] = When::all($promises, function($andWhere) use ($queryBuilder) {
                if (!$andWhere) {
                    return;
                }
                return call_user_func_array(array($queryBuilder->expr(), 'andX'), $andWhere);
            });
        }
        return When::all($where, function ($expressions) use ($logic, $queryBuilder) {
            $not = false;
            switch ($logic) {
              case '$and':
                  $method = 'andX';
                  break;
              case '$or':
                  $method = 'orX';
                  break;
              case '$nor':
                  $method = 'orX';
                  $not = true;
                  break;
            }
            $expression = call_user_func_array(array($queryBuilder->expr(), $method), $expressions);
            if (!$expression->count()) {
                return '1 = 1';
            }
            return $not ? $this->link->getDatabasePlatform()->getNotExpression($expression) : $expression;
        });
    }

    protected function denormalizeCondition($queryBuilder, $property, $condition, $class)
    {
        if ($reference = Reference::is($condition)) {
            $condition = Reference::fetch($reference, $this->domain);
        } elseif (is_array($condition)) {
            foreach ($condition as $k => $v) {
                $function = 'eq';
                switch ((string) $k) {
                    case '$in':
                        return $this->denormalizeValue($v, $property, $class)->then(function ($v) use ($queryBuilder, $property) {
                            if ($v) {
                                $placeholders = array_map(array($queryBuilder, 'createNamedParameter'), $v);
                                $field = $this->link->quoteIdentifier($property);
                                return "$field IN (" . implode(',', $placeholders) . ")";
                                return $this->link->getDatabasePlatform()->getInExpression($field, $placeholders);
                            }
                            return $queryBuilder->expr()->eq(0, 1);
                        });
                    case '$nin':
                        return $this->denormalizeValue($v, $property, $class)->then(function ($v) use ($queryBuilder, $property) {
                            if ($v) {
                                $placeholders = array_map(array($queryBuilder, 'createNamedParameter'), $v);
                                $field = $this->link->quoteIdentifier($property);
                                return "$field NOT IN (" . implode(',', $placeholders) . ")";
                                return $this->link->getDatabasePlatform()->getNotExpression(
                                    $this->link->getDatabasePlatform()->getInExpression($field, $placeholders)
                                );
                            }
                            return $queryBuilder->expr()->eq(0, 1);
                        });
                    case '$lt':
                        $function = 'lt';
                        break;
                    case '$lte':
                        $function = 'lte';
                        break;
                    case '$gt':
                        $function = 'gt';
                        break;
                    case '$gte':
                        $function = 'gte';
                        break;
                    case '$ne':
                        return $this->denormalizeValue($v, $property, $class)->then(function ($value) use ($queryBuilder, $property) {
                        $field = $this->link->quoteIdentifier($property);
                            $subQueryBuilder = $this->link->createQueryBuilder();
                            if($value) {
                                $subQueryBuilder
                                    ->add('select', $queryBuilder->getQueryPart('select'))
                                    ->add('from', $queryBuilder->getQueryPart('from'))
                                    ->add('join', $queryBuilder->getQueryPart('join'))
                                    ->where($queryBuilder->expr()->eq($field, $queryBuilder->createNamedParameter($value)));
                            } else {
                                $subQueryBuilder
                                    ->add('select', $queryBuilder->getQueryPart('select'))
                                    ->add('from', $queryBuilder->getQueryPart('from'))
                                    ->add('join', $queryBuilder->getQueryPart('join'))
                                    ->where($queryBuilder->expr()->isNull($field));
                            }
                            // TODO vvvvv -> guess alias and identifier
                            return "t._id NOT IN (" . $subQueryBuilder->getSQL() . ")";
                        });
                    case '$regex':
                        return $this->denormalizeValue($v, $property, $class)->then(function ($v) use ($queryBuilder, $property) {
                            $field = $this->link->quoteIdentifier($property);
                            return $queryBuilder->expr()->comparison(
                                $field,
                                $this->link->getDatabasePlatform()->getRegexpExpression(),
                                $queryBuilder->createNamedParameter($v)
                            );
                        });
                    case '$xeger':
                        return $this->denormalizeValue($v, $property, $class)->then(function ($v) use ($queryBuilder, $property) {
                            $field = $this->link->quoteIdentifier($property);
                            return $queryBuilder->expr()->comparison(
                                $queryBuilder->createNamedParameter($v),
                                $this->link->getDatabasePlatform()->getRegexpExpression(),
                                $field
                            );
                        });
                    case '$all':
                        return $this->denormalizeValue($v, $property, $class)->then(function ($value) use ($queryBuilder, $property) {
                            if ($value) {
                                $field = $this->link->quoteIdentifier($property);
                                $all = array_map(function ($value) use ($queryBuilder, $field) {
                                    $subQueryBuilder = $this->link->createQueryBuilder();
                                    $subQueryBuilder
                                        ->add('select', $queryBuilder->getQueryPart('select'))
                                        ->add('from', $queryBuilder->getQueryPart('from'))
                                        ->add('join', $queryBuilder->getQueryPart('join'))
                                        ->where($queryBuilder->expr()->eq($field, $queryBuilder->createNamedParameter($value)));
                                    // TODO vvvvv -> guess alias and identifier
                                    return 't._id IN (' . $subQueryBuilder->getSQL() . ')';
                                }, $value);
                                return implode(' AND ', $all);
                            }
                            return $queryBuilder->expr()->eq(0, 1);
                        });
                    case '$not':
                        $not = array(
                            $property => $v
                        );
                        $subQueryBuilder = $this->link->createQueryBuilder();
                        $subQueryBuilder
                            ->setParameters($queryBuilder->getParameters())
                            ->add('select', $queryBuilder->getQueryPart('select'))
                            ->add('from', $queryBuilder->getQueryPart('from'))
                            ->add('join', $queryBuilder->getQueryPart('join'));
                        // TODO Why not use denormalizeCondition? (could be a silly question, check)
                        return $this->denormalizeSearch($subQueryBuilder, array($not), $class)->then(function($expression) use ($queryBuilder, $subQueryBuilder) {
                            $queryBuilder->setParameters($subQueryBuilder->getParameters());
                            $subQueryBuilder->where($expression);
                            // TODO vvvvv -> guess alias and identifier
                            return "t._id NOT IN (" . $subQueryBuilder->getSQL() . ")";
                        });
                    case '$maxDistance':
                        // TODO Check platform support
                        continue;
                    case '$near':
                        // TODO Check platform support
                        $maxDistance = (double) $condition['$maxDistance'];
                        $coordinates = array_map('doubleval', $v);
                        $pointA = ($coordinates[0] - $maxDistance) . " " . $coordinates[1];
                        $pointB = $coordinates[0] . " " . ($coordinates[1] - $maxDistance);
                        $pointC = ($coordinates[0] + $maxDistance) . " " . $coordinates[1];
                        $pointD = $coordinates[0] . " " . ($coordinates[1] + $maxDistance);
                        $polygon = "($pointA,$pointB,$pointC,$pointD,$pointA)";
                        $field = $this->link->quoteIdentifier($property);
                        $shape = "GeomFromText('Polygon($polygon)')";
                        $shapePlaceholder = $queryBuilder->createNamedParameter($shape);
                        return When::resolve("Within($field,$shapePlaceholder)");
                    case '$within':
                        // TODO Check platform support
                        switch (key($v)) {
                          case '$box':
                              $minx = $v[key($v)][0][0];
                              $miny = $v[key($v)][0][1];
                              $maxx = $v[key($v)][1][0];
                              $maxy = $v[key($v)][1][1];
                              $polygon = "($minx $miny,$maxx $miny,$maxx $maxy,$minx $maxy,$minx $miny)";
                              break;
                          case '$center':
                              $r = $v[key($v)][1];
                              $pointA = ($v[key($v)][0][0] - $r) . " " . $v[key($v)][0][1];
                              $pointB = $v[key($v)][0][0] . " " . ($v[key($v)][0][1] - $r);
                              $pointC = ($v[key($v)][0][0] + $r) . " " . $v[key($v)][0][1];
                              $pointD = $v[key($v)][0][0] . " " . ($v[key($v)][0][1] + $r);
                              $polygon = "($pointA,$pointB,$pointC,$pointD,$pointA)";
                              break;
                          case '$polygon':
                              foreach ($v[key($v)] as $i => $point) {
                                  $v[key($v)][$i] = implode(" ", $point);
                              }
                              $v[key($v)][] = $v[key($v)][0];
                              $polygon = "(" . implode("  ", $v[key($v)]) . ")";
                              break;
                        }
                        $field = $this->link->quoteIdentifier($property);
                        $shape = "GeomFromText('Polygon($polygon)')";
                        $shapePlaceholder = $queryBuilder->createNamedParameter($shape);
                        return When::resolve("Within($field,$shapePlaceholder)");
                    default:
                        if (is_numeric($k)) {
                            return $this->denormalizeCondition($queryBuilder, $property, array('$in' => $condition), $class);
                        }

                }
                return $this->denormalizeValue($v, $property, $class)->then(function ($value) use ($queryBuilder, $property, $function) {
                    $field = $this->link->quoteIdentifier($property);
                    return null === $value ? ('eq' === $function ?
                            $this->link->getDatabasePlatform()->getIsNullExpression($field) :
                            $this->link->getDatabasePlatform()->getIsNotNullExpression($field)
                        ) : $queryBuilder->expr()->$function($field, $queryBuilder->createNamedParameter($value));
                });
            }
        }
        // TODO Check if this is redundant with the other one
        return $this->denormalizeValue($condition, $property, $class)->then(function ($value) use ($queryBuilder, $property) {
            $field = $this->link->quoteIdentifier($property);
            return null === $value ? $this->link->getDatabasePlatform()->getIsNullExpression($field) :
                $queryBuilder->expr()->eq($field, $queryBuilder->createNamedParameter($value));
        });
    }

    protected function createIndexTable()
    {
        $schema = $this->schemaManager->createSchema();
        $table = $schema->createTable(self::INDEX_TABLE);
        $table->addColumn('class', Type::STRING);
        $table->addColumn('table', Type::STRING);
        $table->setPrimaryKey(array('class'));
        $table->addUniqueIndex(array('table'));
        return $this->schemaManager->createTable($table);
    }

    protected function indexTable($class)
    {
        $split = explode('\\', $class);
        $tableNameRoot = $tableName = array_pop($split);
        $i = 1;
        while ($this->schemaManager->tablesExist($tableName)) {
            $tableName = $tableNameRoot . (string) ++$i;
        }
        $this->link->insert(self::INDEX_TABLE, array(
            $this->link->quoteIdentifier('class') => $class,
            $this->link->quoteIdentifier('table') => $tableName
        ));
        return $tableName;
    }

    protected function indexedTable($class)
    {
        if (!$this->schemaManager->tablesExist(self::INDEX_TABLE)) {
            $this->createIndexTable();
        }
        $queryBuilder = $this->link->createQueryBuilder();
        $where = $queryBuilder->expr()->eq($this->link->quoteIdentifier('class'), $queryBuilder->createNamedParameter($class));
        $queryBuilder->select($this->link->quoteIdentifier('table'))->from(self::INDEX_TABLE, 't')->where($where);
        return $queryBuilder->execute()->fetchColumn(0);
    }

    protected function tablesFor($class)
    {
        if ($this->cache->contains($class)) {
            return $this->cache->fetch($class);
        }
        if (!$tableName = Configuration::get($class, "storage.options.table", $this->domain)) {
            if (!$tableName = $this->indexedTable($class)) {
                $tableName = $this->indexTable($class);
            }
        }
        $objectIdFieldName = Configuration::get($class, 'storage.options.oid', $this->domain) ? : self::OBJECTID_FIELD_NAME;
        $schema = $this->schemaManager->createSchema();
        $views = $this->schemaManager->listViews();
        if (!$schema->hasTable($tableName) && !isset($views[$tableName])) {
            $table = $schema->createTable($tableName);
            $multiplePropertyTables = array();
            $taggablePropertyTables = array();
            $reflectionClass = new ReflectionClass($class);
            foreach ($reflectionClass->getProperties() as $property) {
                $columnName = $property->name;
                $propertyType = Configuration::get($class, "properties.$columnName.type", $this->domain);
                $isRequired = Configuration::get($class, "properties.$columnName.required", $this->domain);
                $isUnique = Configuration::get($class, "properties.$columnName.unique", $this->domain);
                $isMultiple = Configuration::get($class, "properties.$columnName.multiple", $this->domain);
                $isTaggable = Configuration::get($class, "properties.$columnName.taggable", $this->domain);
                $isIndexed = Configuration::get($class, "properties.$columnName.indexed", $this->domain);
                $strategy = Configuration::get($class, "properties.$columnName.strategy", $this->domain);
                $default = Configuration::get($class, "properties.$columnName.default", $this->domain);
                $columnType = $this->mapColumnType($propertyType);
                if ($isMultiple) {
                    $multiplePropertyTableName = $this->getMultiplePropertyTableName($tableName, $columnName);
                    $multiplePropertyTable = $schema->createTable($multiplePropertyTableName);
                    $multiplePropertyTable->addColumn($objectIdFieldName, self::OBJECTID_FIELD_TYPE);
                    $multiplePropertyTable->addColumn(self::MULTIPLE_PROPERTY_INDEX_FIELD_NAME, self::MULTIPLE_PROPERTY_INDEX_FIELD_TYPE);
                    $multiplePropertyTable->addColumn($columnName, $columnType)->setNotnull($isRequired)->setDefault($default);
                    if ($isUnique) {
                        $multiplePropertyTable->addUniqueIndex(array($columnName));
                    } elseif ($isIndexed) {
                        $multiplePropertyTable->addIndex(array($columnName));
                    }
                    $multiplePropertyTables[] = $multiplePropertyTable;
                } elseif ($isTaggable) {
                    $taggablePropertyTableName = $this->getMultiplePropertyTableName($tableName, $columnName);
                    $taggablePropertyTable = $schema->createTable($taggablePropertyTableName);
                    $taggablePropertyTable->addColumn($objectIdFieldName, self::OBJECTID_FIELD_TYPE);
                    $taggablePropertyTable->addColumn(self::TAGGABLE_PROPERTY_INDEX_FIELD_NAME, self::TAGGABLE_PROPERTY_INDEX_FIELD_TYPE);
                    $taggablePropertyTable->addColumn($columnName, $columnType)->setNotnull($isRequired)->setDefault($default);
                    if ($isUnique) {
                        $taggablePropertyTable->addUniqueIndex(array($columnName));
                    } elseif ($isIndexed) {
                        $taggablePropertyTable->addIndex(array($columnName));
                    }
                    $taggablePropertyTables[] = $taggablePropertyTable;
                }
                else {
                    $column = $table->addColumn($columnName, $columnType)->setNotnull($isRequired)->setDefault($default);
                    if ($isUnique) {
                        $table->addUniqueIndex(array($columnName));
                    } elseif ($isIndexed) {
                        $table->addIndex(array($columnName));
                    }
                    switch ($strategy) {
                      case 'autoincrement':
                          $column->setAutoincrement(true);
                          break;
                    }
                }
            }
            if ($uniques = Configuration::get($class, "uniques", $this->domain)) {
                $table->addUniqueIndex($uniques);
            }
            $table->addColumn($objectIdFieldName, self::OBJECTID_FIELD_TYPE);
            $table->setPrimaryKey(array($objectIdFieldName));
            $this->schemaManager->createTable($table);
            $foreignKeyOptions = array('onUpdate' => 'cascade', 'onDelete' => 'cascade');
            foreach ($multiplePropertyTables as $multiplePropertyTable) {
                $multiplePropertyTable->setPrimaryKey(array($objectIdFieldName, self::MULTIPLE_PROPERTY_INDEX_FIELD_NAME));
                $multiplePropertyTable->addForeignKeyConstraint($table, array($objectIdFieldName), array($objectIdFieldName), $foreignKeyOptions);
                $this->schemaManager->createTable($multiplePropertyTable);
            }
            foreach ($taggablePropertyTables as $taggablePropertyTable) {
                $taggablePropertyTable->setPrimaryKey(array($objectIdFieldName, self::TAGGABLE_PROPERTY_INDEX_FIELD_NAME));
                $taggablePropertyTable->addForeignKeyConstraint($table, array($objectIdFieldName), array($objectIdFieldName), $foreignKeyOptions);
                $this->schemaManager->createTable($taggablePropertyTable);
            }
        }
        $tableNames = array($tableName);
        $prefix = $tableName . self::MULTIPLE_PROPERTY_TABLE_SEPARATOR;
        foreach ($schema->getTables() as $table) {
            $currentTableName = $table->getName();
            if (strpos($currentTableName, $prefix) !== false) {
                $tableNames[] = $currentTableName;
            }
        }
        $this->cache->save($class, $tableNames);
        return $tableNames;
    }

    protected function mapColumnType($propertyType)
    {
        if (isset(self::$typesMap[$propertyType])) {
            return self::$typesMap[$propertyType];
        }
        return Type::STRING;
    }

    protected function getMultiplePropertyTableName($tableName, $propertyName)
    {
        return $this->link->quoteIdentifier($tableName . self::MULTIPLE_PROPERTY_TABLE_SEPARATOR . $propertyName);
    }

    protected function columns($table)
    {
        $columns = array();
        foreach ($this->schemaManager->listTableColumns($table) as $column) {
            $columns[] = $column->getName();
        }
        return $columns;
    }

    protected function generateObjectId()
    {
        return uniqid();
    }

    protected function registerTypes()
    {
        if (!Type::hasType(BreadDateTime::NAME)) {
            Type::addType(BreadDateTime::NAME, BreadDateTime::class);
        }
    }
}
