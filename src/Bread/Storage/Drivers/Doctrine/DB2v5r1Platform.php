<?php
/*
 * THIS SOFTWARE IS PROVIDED BY THE COPYRIGHT HOLDERS AND CONTRIBUTORS
 * "AS IS" AND ANY EXPRESS OR IMPLIED WARRANTIES, INCLUDING, BUT NOT
 * LIMITED TO, THE IMPLIED WARRANTIES OF MERCHANTABILITY AND FITNESS FOR
 * A PARTICULAR PURPOSE ARE DISCLAIMED. IN NO EVENT SHALL THE COPYRIGHT
 * OWNER OR CONTRIBUTORS BE LIABLE FOR ANY DIRECT, INDIRECT, INCIDENTAL,
 * SPECIAL, EXEMPLARY, OR CONSEQUENTIAL DAMAGES (INCLUDING, BUT NOT
 * LIMITED TO, PROCUREMENT OF SUBSTITUTE GOODS OR SERVICES; LOSS OF USE,
 * DATA, OR PROFITS; OR BUSINESS INTERRUPTION) HOWEVER CAUSED AND ON ANY
 * THEORY OF LIABILITY, WHETHER IN CONTRACT, STRICT LIABILITY, OR TORT
 * (INCLUDING NEGLIGENCE OR OTHERWISE) ARISING IN ANY WAY OUT OF THE USE
 * OF THIS SOFTWARE, EVEN IF ADVISED OF THE POSSIBILITY OF SUCH DAMAGE.
 *
 * This software consists of voluntary contributions made by many individuals
 * and is licensed under the MIT license. For more information, see
 * <http://www.doctrine-project.org>.
*/

namespace Bread\Storage\Drivers\Doctrine;

use Doctrine\DBAL\DBALException;
use Doctrine\DBAL\Schema\Index;
use Doctrine\DBAL\Schema\TableDiff;
use Doctrine\DBAL\Platforms\AbstractPlatform;

class DB2v5r1Platform extends AbstractPlatform
{
    /**
     * {@inheritDoc}
     */
    public function getBlobTypeDeclarationSQL(array $field)
    {
        throw DBALException::notSupported(__METHOD__);
    }

    /**
     * {@inheritDoc}
     */
    public function initializeDoctrineTypeMappings()
    {
        $this->doctrineTypeMapping = array(
            'smallint'      => 'smallint',
            'bigint'        => 'bigint',
            'integer'       => 'integer',
            'time'          => 'time',
            'date'          => 'date',
            'varchar'       => 'string',
            'character'     => 'string',
            'clob'          => 'text',
            'decimal'       => 'decimal',
            'numeric'       => 'float',
            'double'        => 'float',
            'real'          => 'float',
            'timestamp'     => 'datetime',
        );
    }

    /**
     * {@inheritDoc}
     */
    protected function getVarcharTypeDeclarationSQLSnippet($length, $fixed)
    {
        return $fixed ? ($length ? 'CHAR(' . $length . ')' : 'CHAR(255)')
                : ($length ? 'VARCHAR(' . $length . ')' : 'VARCHAR(255)');
    }

    /**
     * {@inheritDoc}
     */
    public function getClobTypeDeclarationSQL(array $field)
    {
        // todo clob(n) with $field['length'];
        return 'CLOB(1M)';
    }

    /**
     * {@inheritDoc}
     */
    public function getName()
    {
        return 'db2';
    }

    /**
     * {@inheritDoc}
     */
    public function getBooleanTypeDeclarationSQL(array $columnDef)
    {
        return 'SMALLINT';
    }

    /**
     * {@inheritDoc}
     */
    public function getIntegerTypeDeclarationSQL(array $columnDef)
    {
        return 'INTEGER' . $this->_getCommonIntegerTypeDeclarationSQL($columnDef);
    }

    /**
     * {@inheritDoc}
     */
    public function getBigIntTypeDeclarationSQL(array $columnDef)
    {
        return 'BIGINT' . $this->_getCommonIntegerTypeDeclarationSQL($columnDef);
    }

    /**
     * {@inheritDoc}
     */
    public function getSmallIntTypeDeclarationSQL(array $columnDef)
    {
        return 'SMALLINT' . $this->_getCommonIntegerTypeDeclarationSQL($columnDef);
    }

    /**
     * {@inheritDoc}
     */
    protected function _getCommonIntegerTypeDeclarationSQL(array $columnDef)
    {
        $autoinc = '';
        if ( ! empty($columnDef['autoincrement'])) {
            $autoinc = ' GENERATED BY DEFAULT AS IDENTITY';
        }

        return $autoinc;
    }

    /**
     * {@inheritDoc}
     */
    public function getDateTimeTypeDeclarationSQL(array $fieldDeclaration)
    {
        if (isset($fieldDeclaration['version']) && $fieldDeclaration['version'] == true) {
            return "TIMESTAMP(0) WITH DEFAULT";
        }

        return 'TIMESTAMP(0)';
    }

    /**
     * {@inheritDoc}
     */
    public function getDateTypeDeclarationSQL(array $fieldDeclaration)
    {
        return 'DATE';
    }

    /**
     * {@inheritDoc}
     */
    public function getTimeTypeDeclarationSQL(array $fieldDeclaration)
    {
        return 'TIME';
    }

    public function getListDatabasesSQL()
    {
        throw DBALException::notSupported(__METHOD__);
    }

    public function getListSequencesSQL($database)
    {
        throw DBALException::notSupported(__METHOD__);
    }

    public function getListTableConstraintsSQL($table)
    {
        throw DBALException::notSupported(__METHOD__);
    }

    /**
     * This code fragment is originally from the Zend_Db_Adapter_Db2 class.
     *
     * @license New BSD License
     * @param  string $table
     * @param string $database
     * @return string
     */
    public function getListTableColumnsSQL($table, $database = null)
    {
        return "SELECT DISTINCT
                C.TABLE_SCHEMA AS TABSCHEMA,
                C.TABLE_NAME AS TABNAME,
                C.COLUMN_NAME AS COLNAME,
                C.ORDINAL_POSITION AS COLNO,
                C.DATA_TYPE AS TYPENAME,
                C.COLUMN_DEFAULT AS DEFAULT,
                C.IS_NULLABLE AS NULLS,
                C.LENGTH AS LENGTH,
                C.NUMERIC_SCALE AS SCALE,
                C.CCSID AS IDENTITY,
                TC.TYPE AS TABCONSTTYPE, K.COLSEQ
                FROM QSYS2.SYSCOLUMNS C
                LEFT JOIN (QSYS2.SYSKEYCST K JOIN QSYS2.SYSCST TC
                ON (K.TABLE_SCHEMA = TC.TABLE_SCHEMA
                    AND K.TABLE_NAME = TC.TABLE_NAME
                    AND TC.TYPE = 'PRIMARY KEY'))
                ON (C.TABLE_SCHEMA = K.TABLE_SCHEMA
                    AND C.TABLE_NAME = K.TABLE_NAME
                    AND C.COLUMN_NAME = K.COLUMN_NAME)
                WHERE C.TABLE_NAME = '" . $table . "' ORDER BY COLNO";
        
        return "SELECT DISTINCT c.tabschema, c.tabname, c.colname, c.colno,
                c.typename, c.default, c.nulls, c.length, c.scale,
                c.identity, tc.type AS tabconsttype, k.colseq
                FROM syscat.columns c
                LEFT JOIN (syscat.keycoluse k JOIN syscat.tabconst tc
                ON (k.tabschema = tc.tabschema
                    AND k.tabname = tc.tabname
                    AND tc.type = 'P'))
                ON (c.tabschema = k.tabschema
                    AND c.tabname = k.tabname
                    AND c.colname = k.colname)
                WHERE UPPER(c.tabname) = UPPER('" . $table . "') ORDER BY c.colno";
    }

    public function getListTablesSQL()
    {
        return "SELECT NAME FROM QSYS2.SYSTABLES WHERE TYPE = 'T'";
    }

    public function getListUsersSQL()
    {
        throw DBALException::notSupported(__METHOD__);
    }

    /**
     * {@inheritDoc}
     */
    public function getListViewsSQL($database)
    {
        return "SELECT NAME, TEXT FROM QSYS2.SYSVIEWS";
    }

    /**
     * {@inheritDoc}
     */
    public function getListTableIndexesSQL($table, $currentDatabase = null)
    {
        return "SELECT TABLE_NAME FROM QSYS2.SYSTABLES WHERE 0 = 1";
        return "SELECT NAME, COLNAMES, UNIQUERULE FROM QSYS2.SYSINDEXES WHERE TBNAME = UPPER('" . $table . "')";
    }

    public function getListTableForeignKeysSQL($table)
    {
        return "SELECT TABLE_NAME FROM QSYS2.SYSTABLES WHERE 0 = 1";
        return "SELECT TBNAME, RELNAME, REFTBNAME, DELETERULE, UPDATERULE, FKCOLNAMES, PKCOLNAMES ".
               "FROM QSYS2.SYSRELS WHERE TBNAME = UPPER('".$table."')";
    }

    public function getCreateViewSQL($name, $sql)
    {
        return "CREATE VIEW ".$name." AS ".$sql;
    }

    public function getDropViewSQL($name)
    {
        return "DROP VIEW ".$name;
    }

    /**
     * {@inheritDoc}
     */
    public function getDropSequenceSQL($sequence)
    {
        throw DBALException::notSupported(__METHOD__);
    }

    public function getSequenceNextValSQL($sequenceName)
    {
        throw DBALException::notSupported(__METHOD__);
    }

    /**
     * {@inheritDoc}
     */
    public function getCreateDatabaseSQL($database)
    {
        return "CREATE DATABASE ".$database;
    }

    /**
     * {@inheritDoc}
     */
    public function getDropDatabaseSQL($database)
    {
        return "DROP DATABASE ".$database.";";
    }

    /**
     * {@inheritDoc}
     */
    public function supportsCreateDropDatabase()
    {
        return false;
    }

    /**
     * {@inheritDoc}
     */
    public function supportsReleaseSavepoints()
    {
        return false;
    }

    /**
     * {@inheritDoc}
     */
    public function getCurrentDateSQL()
    {
        return 'VALUES CURRENT DATE';
    }

    /**
     * {@inheritDoc}
     */
    public function getCurrentTimeSQL()
    {
        return 'VALUES CURRENT TIME';
    }

    /**
     * {@inheritDoc}
     */
    public function getCurrentTimestampSQL()
    {
        return "VALUES CURRENT TIMESTAMP";
    }

    /**
     * {@inheritDoc}
     */
    public function getIndexDeclarationSQL($name, Index $index)
    {
        return $this->getUniqueConstraintDeclarationSQL($name, $index);
    }

    /**
     * {@inheritDoc}
     */
    protected function _getCreateTableSQL($tableName, array $columns, array $options = array())
    {
        $indexes = array();
        if (isset($options['indexes'])) {
            $indexes = $options['indexes'];
        }
        $options['indexes'] = array();

        $sqls = parent::_getCreateTableSQL($tableName, $columns, $options);

        foreach ($indexes as $definition) {
            $sqls[] = $this->getCreateIndexSQL($definition, $tableName);
        }
        return $sqls;
    }

    /**
     * {@inheritDoc}
     */
    public function getAlterTableSQL(TableDiff $diff)
    {
        $sql = array();
        $columnSql = array();

        $queryParts = array();
        foreach ($diff->addedColumns as $column) {
            if ($this->onSchemaAlterTableAddColumn($column, $diff, $columnSql)) {
                continue;
            }

            $queryParts[] = 'ADD COLUMN ' . $this->getColumnDeclarationSQL($column->getQuotedName($this), $column->toArray());
        }

        foreach ($diff->removedColumns as $column) {
            if ($this->onSchemaAlterTableRemoveColumn($column, $diff, $columnSql)) {
                continue;
            }

            $queryParts[] =  'DROP COLUMN ' . $column->getQuotedName($this);
        }

        foreach ($diff->changedColumns as $columnDiff) {
            if ($this->onSchemaAlterTableChangeColumn($columnDiff, $diff, $columnSql)) {
                continue;
            }

            /* @var $columnDiff \Doctrine\DBAL\Schema\ColumnDiff */
            $column = $columnDiff->column;
            $queryParts[] =  'ALTER ' . ($columnDiff->oldColumnName) . ' '
                    . $this->getColumnDeclarationSQL($column->getQuotedName($this), $column->toArray());
        }

        foreach ($diff->renamedColumns as $oldColumnName => $column) {
            if ($this->onSchemaAlterTableRenameColumn($oldColumnName, $column, $diff, $columnSql)) {
                continue;
            }

            $queryParts[] =  'RENAME ' . $oldColumnName . ' TO ' . $column->getQuotedName($this);
        }

        $tableSql = array();

        if ( ! $this->onSchemaAlterTable($diff, $tableSql)) {
            if (count($queryParts) > 0) {
                $sql[] = 'ALTER TABLE ' . $diff->name . ' ' . implode(" ", $queryParts);
            }

            $sql = array_merge($sql, $this->_getAlterTableIndexForeignKeySQL($diff));

            if ($diff->newName !== false) {
                $sql[] =  'RENAME TABLE TO ' . $diff->newName;
            }
        }

        return array_merge($sql, $tableSql, $columnSql);
    }

    /**
     * {@inheritDoc}
     */
    public function getDefaultValueDeclarationSQL($field)
    {
        if (isset($field['notnull']) && $field['notnull'] && !isset($field['default'])) {
            if (in_array((string)$field['type'], array("Integer", "BigInteger", "SmallInteger"))) {
                $field['default'] = 0;
            } else if((string)$field['type'] == "DateTime") {
                $field['default'] = "00-00-00 00:00:00";
            } else if ((string)$field['type'] == "Date") {
                $field['default'] = "00-00-00";
            } else if((string)$field['type'] == "Time") {
                $field['default'] = "00:00:00";
            } else {
                $field['default'] = '';
            }
        }

        unset($field['default']); // @todo this needs fixing
        if (isset($field['version']) && $field['version']) {
            if ((string)$field['type'] != "DateTime") {
                $field['default'] = "1";
            }
        }

        return parent::getDefaultValueDeclarationSQL($field);
    }

    /**
     * {@inheritDoc}
     */
    public function getEmptyIdentityInsertSQL($tableName, $identifierColumnName)
    {
        return 'INSERT INTO ' . $tableName . ' (' . $identifierColumnName . ') VALUES (DEFAULT)';
    }

    public function getCreateTemporaryTableSnippetSQL()
    {
        return "DECLARE GLOBAL TEMPORARY TABLE";
    }

    /**
     * {@inheritDoc}
     */
    public function getTemporaryTableName($tableName)
    {
        return "SESSION." . $tableName;
    }

    /**
     * {@inheritDoc}
     */
    protected function doModifyLimitQuery($query, $limit, $offset = null)
    {
        if ($limit === null && $offset === null) {
            return $query;
        }

        $limit = (int)$limit;
        $offset = (int)(($offset)?:0);
        
        $sql = "$query FETCH FIRST $limit ROWS ONLY";
        
        return $sql;

        // Todo OVER() needs ORDER BY data!
        $sql = 'SELECT db22.* FROM (SELECT ROW_NUMBER() OVER() AS DC_ROWNUM, db21.* '.
               'FROM (' . $query . ') db21) db22 WHERE db22.DC_ROWNUM BETWEEN ' . ($offset+1) .' AND ' . ($offset+$limit);

        return $sql;
    }

    /**
     * {@inheritDoc}
     */
    public function getLocateExpression($str, $substr, $startPos = false)
    {
        if ($startPos == false) {
            return 'LOCATE(' . $substr . ', ' . $str . ')';
        }

        return 'LOCATE(' . $substr . ', ' . $str . ', '.$startPos.')';
    }

    /**
     * {@inheritDoc}
     */
    public function getSubstringExpression($value, $from, $length = null)
    {
        if ($length === null) {
            return 'SUBSTR(' . $value . ', ' . $from . ')';
        }

        return 'SUBSTR(' . $value . ', ' . $from . ', ' . $length . ')';
    }

    /**
     * {@inheritDoc}
     */
    public function supportsIdentityColumns()
    {
        return true;
    }

    /**
     * {@inheritDoc}
     */
    public function prefersIdentityColumns()
    {
        return true;
    }

    /**
     * {@inheritDoc}
     *
     * DB2 returns all column names in SQL result sets in uppercase.
     */
    public function getSQLResultCasing($column)
    {
        return strtoupper($column);
    }

    public function getForUpdateSQL()
    {
        return ' WITH RR USE AND KEEP UPDATE LOCKS';
    }

    /**
     * {@inheritDoc}
     */
    public function getDummySelectSQL()
    {
        return 'SELECT 1 FROM qsys2.sysdummy1';
    }

    /**
     * {@inheritDoc}
     *
     * DB2 supports savepoints, but they work semantically different than on other vendor platforms.
     *
     * TODO: We have to investigate how to get DB2 up and running with savepoints.
     */
    public function supportsSavepoints()
    {
        return false;
    }

    /**
     * {@inheritDoc}
     */
    protected function getReservedKeywordsClass()
    {
        return 'Doctrine\DBAL\Platforms\Keywords\DB2Keywords';
    }
}
