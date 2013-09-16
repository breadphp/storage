<?php
namespace Bread\Storage\Drivers;

use Bread\Storage\Driver;
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
use Doctrine\Common\Cache\ArrayCache;
use ReflectionClass;
use Bread\Types;
use DateTime;
use DateInterval;

class Doctrine extends Driver implements DriverInterface
{
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
        'Bread\Types\Date' => Type::DATE,
        'Bread\Types\Time' => Type::TIME,
        'Bread\Types\DateTime' => Type::DATETIME,
        'Bread\Types\DateInterval' => Type::INTEGER,
        'Bread\Types\Text' => Type::STRING,
        'Bread\Types\EmailAddress' => Type::STRING
    );
    
    public function __construct($uri, array $options = array())
    {
        $options = array_merge(array(
            'cache' => true,
            'debug' => false
        ), $options);
        $scheme = parse_url($uri, PHP_URL_SCHEME);
        switch ($scheme) {
            case 'sqlite':
                $params = array(
                    'user' => parse_url($uri, PHP_URL_USER),
                    'password' => parse_url($uri, PHP_URL_PASS),
                    'path' => parse_url($uri, PHP_URL_PATH),
                    'driver' => 'pdo_sqlite'
                );
                break;
            case 'mysql':
                $params = array(
                    'user' => parse_url($uri, PHP_URL_USER),
                    'password' => parse_url($uri, PHP_URL_PASS),
                    'host' => parse_url($uri, PHP_URL_HOST),
                    'port' => parse_url($uri, PHP_URL_PORT),
                    'dbname' => ltrim(parse_url($uri, PHP_URL_PATH), '/'),
                    'driver' => 'pdo_mysql'
                );
                break;
            case 'pgsql':
                $params = array(
                    'user' => parse_url($uri, PHP_URL_USER),
                    'password' => parse_url($uri, PHP_URL_PASS),
                    'host' => parse_url($uri, PHP_URL_HOST),
                    'port' => parse_url($uri, PHP_URL_PORT),
                    'dbname' => ltrim(parse_url($uri, PHP_URL_PATH), '/'),
                    'driver' => 'pdo_pgsql'
                );
                break;
            case 'sqlsrv':
                $params = array(
                    'user' => parse_url($uri, PHP_URL_USER),
                    'password' => parse_url($uri, PHP_URL_PASS),
                    'host' => parse_url($uri, PHP_URL_HOST),
                    'port' => parse_url($uri, PHP_URL_PORT),
                    'dbname' => ltrim(parse_url($uri, PHP_URL_PATH), '/'),
                    'driver' => 'pdo_sqlsrv'
                );
                break;
            case 'db2':
                $params = array(
                    'user' => parse_url($uri, PHP_URL_USER),
                    'password' => parse_url($uri, PHP_URL_PASS),
                    'host' => parse_url($uri, PHP_URL_HOST),
                    'port' => parse_url($uri, PHP_URL_PORT),
                    'dbname' => ltrim(parse_url($uri, PHP_URL_PATH), '/'),
                    'driverClass' => 'Bread\Storage\Drivers\Doctrine\DB2v5r1Driver'//driver' => 'pdo_ibm'
                );
                break;
            default:
                throw new Exception(sprintf('Scheme %s not supported by %s driver', $scheme, __CLASS__));
        }
        $this->link = DriverManager::getConnection($params);
        if ($options['debug']) {
            $this->link->getConfiguration()->setSQLLogger(new \Doctrine\DBAL\Logging\EchoSQLLogger());
        }
        $this->useCache = $options['cache'];
        $cache = new ArrayCache();
        $config = $this->link->getConfiguration();
        $config->setResultCacheImpl($cache);
        $this->schemaManager = $this->link->getSchemaManager();
        $this->hydrationMap = new Map();
    }

    public function store($object)
    {
        $instance = $this->hydrationMap->getInstance($object);
        switch ($instance->getState()) {
          case Instance::STATE_NEW:
              $oid = $this->generateObjectId();
              $instance->setObjectId($oid);
              break;
          case Instance::STATE_MANAGED:
              $oid = $instance->getObjectId();
              break;
          default:
              throw new Exception('Object instance cannot be stored');
        }
        $class = $instance->getClass();
        $this->link->beginTransaction();
        return $this->denormalize($instance->getModifiedProperties($object), $class)->then(function ($properties) use ($instance, $object, $oid, $class) {
            $objectIdFieldName = Configuration::get($class, 'storage.options.oid') ? : self::OBJECTID_FIELD_NAME;
            $tables = $this->tablesFor($class);
            foreach ($tables as $tableName) {
                list(, $propertyName) = explode(self::MULTIPLE_PROPERTY_TABLE_SEPARATOR, $tableName) + array(null, null);
                $columns = $this->columns($tableName);
                $tableName = $this->link->quoteIdentifier($tableName);
                if (!$values = array_intersect_key($properties, array_flip($columns))) {
                    continue;
                }
                $fields = array_keys($values);
                $isMultiple = $propertyName ? Configuration::get($class, "properties.$propertyName.multiple") : false;
                $isTaggable = $propertyName ? Configuration::get($class, "properties.$propertyName.taggable") : false;
                switch ($instance->getState()) {
                  case Instance::STATE_NEW:
                      if ($isMultiple) {
                          foreach ((array) $values[$propertyName] as $key => $value) {
                              $this->link->insert($tableName, array(
                                  $objectIdFieldName => $oid,
                                  self::MULTIPLE_PROPERTY_INDEX_FIELD_NAME => $key,
                                  $propertyName => $value
                              ));
                          }
                      } elseif ($isTaggable) {
                          foreach ((array) $values[$propertyName] as $key => $value) {
                              $this->link->insert($tableName, array(
                                  $objectIdFieldName => $oid,
                                  self::TAGGABLE_PROPERTY_INDEX_FIELD_NAME => $key,
                                  $propertyName => $value
                              ));
                          }
                      } else {
                          $values[$objectIdFieldName] = $oid;
                          $this->link->insert($tableName, $values);
                          // TODO replace strategy with computed attributes outside storage
                          foreach (Configuration::get($class, "keys") as $keyProperty) {
                              switch (Configuration::get($class, "properties.$keyProperty.strategy")) {
                                case 'autoincrement':
                                    $instance->setProperty($object, $keyProperty, (int) $this->link->lastInsertId());
                                    break;
                              }
                          }
                      }
                      break;
                  case Instance::STATE_MANAGED:
                      if ($isMultiple) {
                          $existingQueryBuilder = $this->link->createQueryBuilder();
                          $existingQueryBuilder->select(self::MULTIPLE_PROPERTY_INDEX_FIELD_NAME)->from($tableName, 't')
                            ->where($existingQueryBuilder->expr()->eq($objectIdFieldName, $existingQueryBuilder->createNamedParameter($oid)));
                          $existingCount = $existingQueryBuilder->execute()->rowCount();
                          foreach ((array) $values[$propertyName] as $key => $value) {
                              if ($key >= $existingCount) {
                                  $this->link->insert($tableName, array(
                                      $objectIdFieldName => $oid,
                                      self::MULTIPLE_PROPERTY_INDEX_FIELD_NAME => $key,
                                      $propertyName => $value
                                  ));
                              } else {
                                  $this->link->update($tableName, array(
                                      $propertyName => $value
                                  ), array(
                                      $objectIdFieldName => $oid,
                                      self::MULTIPLE_PROPERTY_INDEX_FIELD_NAME => $key
                                  ));
                              }
                          }
                          $queryBuilder = $this->link->createQueryBuilder();
                          $queryBuilder->delete($tableName)->where($queryBuilder->expr()->gte(
                              self::MULTIPLE_PROPERTY_INDEX_FIELD_NAME,
                              $queryBuilder->createNamedParameter(count($values[$propertyName]), \PDO::PARAM_INT)
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
                                  ));
                              } else {
                                  $this->link->insert($tableName, array(
                                      $objectIdFieldName => $oid,
                                      self::TAGGABLE_PROPERTY_INDEX_FIELD_NAME => $key,
                                      $propertyName => $value
                                  ));
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
                          $this->link->update($tableName, $values, array($objectIdFieldName => $oid));
                      }
                      break;
                }
            }
            return $object;
        })->then(function ($object) use ($instance) {
            $this->link->commit();
            $instance->setObject($object);
            switch ($instance->getState()) {
                case Instance::STATE_NEW:
                    $instance->setState(Instance::STATE_MANAGED);
                    $this->hydrationMap->attach($object, $instance);
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
      $instance = $this->hydrationMap->getInstance($object);
      $class = get_class($object);
      $objectIdFieldName = Configuration::get($class, 'storage.options.oid') ? : self::OBJECTID_FIELD_NAME;
      switch ($instance->getState()) {
        case Instance::STATE_NEW:
            $instance->setState(Instance::STATE_DELETED);
            // fallback to next case
        case Instance::STATE_DELETED:
            break;
        case Instance::STATE_MANAGED:
            $oid = $instance->getObjectId();
            $tableNames = $this->tablesFor($class);
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
            return current($results) ? : When::reject($class);
        });
    }
    
    public function fetch($class, array $search = array(), array $options = array())
    {
        return $this->fetchFromCache($class, $search, $options)->then(null, function($cacheKey) use ($class, $search, $options) {
            return $this->select($class, $search, $options)->then(function ($result) use ($class) {
                $objectIdFieldName = Configuration::get($class, 'storage.options.oid') ? : self::OBJECTID_FIELD_NAME;
                $tableNames = $this->tablesFor($class);
                $tableName = $this->link->quoteIdentifier(array_shift($tableNames));
                $tableAlias = $this->link->quoteIdentifier('t');
                $oidIdentifier = $this->link->quoteIdentifier($objectIdFieldName);
                $promises = array();
                foreach ($result->fetchAll() as $row) {
                    $oid = $row[$objectIdFieldName];
                    if ($object = $this->hydrationMap->objectExists($oid)) {
                        $promises[$oid] = When::resolve($object);
                    } else {
                        $propertiesQueryBuilder = $this->link->createQueryBuilder();
                        $values = $propertiesQueryBuilder->select('*')->from($tableName, $tableAlias)
                            ->where($propertiesQueryBuilder->expr()->eq($oidIdentifier, $propertiesQueryBuilder->createNamedParameter($oid)))
                            ->execute()->fetch(\PDO::FETCH_ASSOC);
                        foreach ($tableNames as $multiplePropertyTableName) {
                            list(, $propertyName) = explode(self::MULTIPLE_PROPERTY_TABLE_SEPARATOR, $multiplePropertyTableName) + array(null, null);
                            $multiplePropertyTableName = $this->link->quoteIdentifier($multiplePropertyTableName);
                            $multiplePropertyQueryBuilder = $this->link->createQueryBuilder();
                            $values[$propertyName] = $multiplePropertyQueryBuilder->select($this->link->quoteIdentifier($propertyName))->from($multiplePropertyTableName, $tableAlias)
                                ->where($multiplePropertyQueryBuilder->expr()->eq($oidIdentifier, $multiplePropertyQueryBuilder->createNamedParameter($oid)))
                                ->execute()->fetchAll(\PDO::FETCH_COLUMN);
                        }
                        $promises[$oid] = $this->hydrateObject($values, $class);
                    }
                }
                return When::all($promises);
            })->then(function ($objects) use ($cacheKey) {
                return $this->storeToCache($cacheKey, $objects);
            });
        })->then(array($this, 'buildCollection'));
    }

    public function purge($class, array $search = array(), array $options = array())
    {}
    
    protected function select($class, array $search = array(), array $options = array())
    {
        $objectIdFieldName = Configuration::get($class, 'storage.options.oid') ? : self::OBJECTID_FIELD_NAME;
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
            return Reference::fetch($value);
        }
        if (is_array($value)) {
            $normalizedValues = array();
            foreach ($value as $v) {
                $normalizedValues[] = $this->normalizeValue($name, $v, $class);
            }
            return When::all($normalizedValues);
        }
        $type = Configuration::get($class, "properties.$name.type");
        switch ($type) {
          default:
              if (isset(self::$typesMap[$type])) {
                  $doctrineType = self::$typesMap[$type];
                  // TODO workaround to make BIGINTs convert to int instead of string
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
        if ($value instanceof Promise) {
            return $value->then(function($value) use ($field, $class) {
                return $this->denormalizeValue($value, $field, $class);
            });
        } elseif (Reference::is($value)) {
            return When::resolve((string) $value);
        } elseif (is_object($value)) {
            $type = Configuration::get($class, "properties.$field.type");
            if (isset(self::$typesMap[$type])) {
                $doctrineType = self::$typesMap[$type];
                return When::resolve($this->link->convertToDatabaseValue($value, $doctrineType));
            }
            else {
                if ($value instanceof DateInterval) {
                    $denormalizedValue = (int) Types\DateInterval::calculateSeconds($value);
                    return When::resolve($denormalizedValue);
                } else {
                    return Manager::driver(get_class($value))->store($value)->then(function($object) {
                        return (string) new Reference($object);
                    });
                }
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
            $condition = Reference::fetch($reference);
        } elseif (is_array($condition)) {
            foreach ($condition as $k => $v) {
                $function = 'eq';
                switch ($k) {
                    case '$in':
                        return $this->denormalizeValue($v, $property, $class)->then(function ($v) use ($queryBuilder, $property) {
                            $placeholders = array_map(array($queryBuilder, 'createNamedParameter'), $v);
                            $field = $this->link->quoteIdentifier($property);
                            return $this->link->getDatabasePlatform()->getInExpression($field, $placeholders);
                        });
                    case '$nin':
                        return $this->denormalizeValue($v, $property, $class)->then(function ($v) use ($queryBuilder, $property) {
                            $placeholders = array_map(array($queryBuilder, 'createNamedParameter'), $v);
                            $field = $this->link->quoteIdentifier($property);
                            return $this->link->getDatabasePlatform()->getNotExpression(
                                $this->link->getDatabasePlatform()->getInExpression($field, $placeholders)
                            );
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
                            $subQueryBuilder
                                ->add('select', $queryBuilder->getQueryPart('select'))
                                ->add('from', $queryBuilder->getQueryPart('from'))
                                ->add('join', $queryBuilder->getQueryPart('join'))
                                ->where($queryBuilder->expr()->eq($field, $queryBuilder->createNamedParameter($value)));
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
    
    protected function tablesFor($class)
    {
        if (!$tableName = Configuration::get($class, "storage.options.table")) {
            // TODO _index table
            $split = explode('\\', $class);
            $tableName = array_pop($split);
        }
        $objectIdFieldName = Configuration::get($class, 'storage.options.oid') ? : self::OBJECTID_FIELD_NAME;
        $schema = $this->schemaManager->createSchema();
        if (!$schema->hasTable($tableName)) {
            $table = $schema->createTable($tableName);
            $multiplePropertyTables = array();
            $taggablePropertyTables = array();
            $reflectionClass = new ReflectionClass($class);
            foreach ($reflectionClass->getProperties() as $property) {
                $columnName = $property->name;
                $propertyType = Configuration::get($class, "properties.$columnName.type");
                $isRequired = Configuration::get($class, "properties.$columnName.required");
                $isMultiple = Configuration::get($class, "properties.$columnName.multiple");
                $isTaggable = Configuration::get($class, "properties.$columnName.taggable");
                $strategy = Configuration::get($class, "properties.$columnName.strategy");
                $default = Configuration::get($class, "properties.$columnName.default");
                $columnType = $this->mapColumnType($propertyType);
                if ($isMultiple) {
                    $multiplePropertyTableName = $this->getMultiplePropertyTableName($tableName, $columnName);
                    $multiplePropertyTable = $schema->createTable($multiplePropertyTableName);
                    $multiplePropertyTable->addColumn($objectIdFieldName, self::OBJECTID_FIELD_TYPE);
                    $multiplePropertyTable->addColumn(self::MULTIPLE_PROPERTY_INDEX_FIELD_NAME, self::MULTIPLE_PROPERTY_INDEX_FIELD_TYPE);
                    $multiplePropertyTable->addColumn($columnName, $columnType)->setNotnull($isRequired)->setDefault($default);
                    $multiplePropertyTables[] = $multiplePropertyTable;
                } elseif ($isTaggable) {
                    $taggablePropertyTableName = $this->getMultiplePropertyTableName($tableName, $columnName);
                    $taggablePropertyTable = $schema->createTable($taggablePropertyTableName);
                    $taggablePropertyTable->addColumn($objectIdFieldName, self::OBJECTID_FIELD_TYPE);
                    $taggablePropertyTable->addColumn(self::TAGGABLE_PROPERTY_INDEX_FIELD_NAME, self::TAGGABLE_PROPERTY_INDEX_FIELD_TYPE);
                    $taggablePropertyTable->addColumn($columnName, $columnType)->setNotnull($isRequired)->setDefault($default);
                    $taggablePropertyTables[] = $taggablePropertyTable;
                }
                else {
                    $column = $table->addColumn($columnName, $columnType)->setNotnull($isRequired)->setDefault($default);
                    switch ($strategy) {
                      case 'autoincrement':
                          $column->setAutoincrement(true);
                          break;
                    }
                }
            }
            if ($keys = Configuration::get($class, "keys")) {
                $table->addUniqueIndex($keys);
                // TODO Verify if necessary, if not delete
                foreach ($keys as $key) {
                    switch (Configuration::get($class, "properties.$key.strategy")) {
                      case 'autoincrement':
                          $table->getColumn($key)->setAutoincrement(true);
                          break;
                    }
                }
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
}
