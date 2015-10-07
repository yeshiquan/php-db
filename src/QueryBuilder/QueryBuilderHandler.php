<?php namespace Vega\Database\QueryBuilder;

use PDO;
use Vega\Database\Connection;
use Vega\Database\Exception;
use Vega\Database\PluginProxy;

class QueryBuilderHandler
{

    /**
     * @var \Vega\Database\Container
     */
    protected $container;

    /**
     * @var Connection
     */
    protected $connection;

    /**
     * @var PluginProxy
     */
    protected $plugin;


    /**
     * @var array
     */
    protected $statements = array();

    /**
     * @var bool
     */
    protected $isRaw = true;


    /**
     * @var PDO
     */
    protected $pdo;

    /**
     * @var null|string
     */
    protected $tablePrefix = null;

    /**
     * @var \Vega\Database\QueryBuilder\Adapters\BaseAdapter
     */
    protected $adapterInstance;

    /**
     * The PDO fetch parameters to use
     *
     * @var array
     */
    protected $fetchParameters = array(\PDO::FETCH_OBJ);

    /**
     * @param null|\Vega\Database\Connection $connection
     *
     * @throws \Vega\Database\Exception
     */
    public function __construct(Connection $connection = null)
    {
        if (is_null($connection)) {
            if (!$connection = Connection::getStoredConnection()) {
                throw new Exception('No database connection found.', 1);
            }
        }

        $this->connection = $connection;
        $this->plugin = $connection->getPlugin();
        $this->container = $this->connection->getContainer();
        $this->pdo = $this->connection->getPdoInstance();
        $this->adapter = $this->connection->getAdapter();
        $this->adapterConfig = $this->connection->getAdapterConfig();

        if (isset($this->adapterConfig['prefix'])) {
            $this->tablePrefix = $this->adapterConfig['prefix'];
        }

        // Query builder adapter instance
        $this->adapterInstance = $this->container->build(
                '\\Vega\Database\\QueryBuilder\\Adapters\\' . ucfirst($this->adapter), array($this->connection));

        $this->pdo->setAttribute(PDO::ATTR_ERRMODE, PDO::ERRMODE_EXCEPTION);
    }

    /**
     * Transaction begin
     * @return 
     */
    public function beginTransaction() {
        $this->pdo->beginTransaction();
    }

    /**
     * Transaction commit
     * @return 
     */
    public function commit() {
        $this->pdo->commit();
    }

    /**
     * Transaction rollback
     * @return 
     */
    public function rollback() {
        $this->pdo->rollback();
    }

    /**
     * Set the fetch mode
     *
     * @param $mode
     * @return $this
     */
    public function setFetchMode($mode)
    {
        $this->fetchParameters = func_get_args();
        return $this;
    }

    /**
     * Fetch query results as object of specified type
     *
     * @param $className
     * @param array $constructorArgs
     * @return QueryBuilderHandler
     */
    public function asObject($className, $constructorArgs = array())
    {
        return $this->setFetchMode(\PDO::FETCH_CLASS, $className, $constructorArgs);
    }

    /**
     * @param null|\Vega\Database\Connection $connection
     *
     * @return static
     */
    public function newQueryBuilder(Connection $connection = null)
    {
        if (is_null($connection)) {
            $connection = $this->connection;
        }

        return new static($connection);
    }

    /**
     * @param       $sql
     * @param array $bindings
     *
     * @return array PDOStatement and execution time as float
     */
    public function statement($sql, $bindings = array())
    {
        $start = microtime(true);

        $pdoStatement = $this->pdo->prepare($sql);
        if ($bindings) {
            foreach ($bindings as $key => $value) {
                $pdoStatement->bindValue(
                    is_int($key) ? $key + 1 : $key,
                    $value,
                    is_int($value) || is_bool($value) ? PDO::PARAM_INT : PDO::PARAM_STR
                );
            }
        }
        $pdoStatement->execute();

        $end = microtime(true);
        return array($pdoStatement, $end - $start);
    }

    /**
     * Get all rows
     *
     * @return \stdClass|null
     */
    public function get($sql = null, $bindings = null)
    {
        if (!$this->isRaw) {
            list($sql, $bindings) = $this->adapterInstance->select($this->statements);
        }
        $rawSql = $this->getRawSql($sql, $bindings);

        $start = microtime(true);
        try {
            list($pdoStatement, $executionTime) = $this->statement($sql, $bindings);
            $result = call_user_func_array(array($pdoStatement, 'fetchAll'), $this->fetchParameters);
            $end = microtime(true);
            $executionTime += ($end - $start);
            $this->plugin->afterSelect($rawSql, count($result), $executionTime);
            return $result;
        } catch (\Exception $e) {
            $this->plugin->onException($rawSql, 'select', $e);
            return null;
        }
    }

    /**
     * Get first row
     *
     * @return \stdClass|null
     */
    public function first($sql = null, $bindings = null)
    {
        $this->limit(1);
        $result = $this->get($sql, $bindings);
        return empty($result) ? null : $result[0];
    }

    /**
     * @param        $value
     * @param string $fieldName
     *
     * @return null|\stdClass
     */
    public function findAll($fieldName, $value)
    {
        $this->where($fieldName, '=', $value);
        return $this->get();
    }

    /**
     * @param        $value
     * @param string $fieldName
     *
     * @return null|\stdClass
     */
    public function find($value, $fieldName = 'id')
    {
        $this->where($fieldName, '=', $value);
        return $this->first();
    }

    /**
     * Get count of rows
     *
     * @return int
     */
    public function count()
    {
        // Get the current statements
        $originalStatements = $this->statements;

        unset($this->statements['orderBys']);
        unset($this->statements['limit']);
        unset($this->statements['offset']);

        $count = $this->aggregate('count');
        $this->statements = $originalStatements;

        return $count;
    }

    /**
     * @param $type
     *
     * @return int
     */
    protected function aggregate($type)
    {
        // Get the current selects
        $mainSelects = isset($this->statements['selects']) ? $this->statements['selects'] : null;
        // Replace select with a scalar value like `count`
        $this->statements['selects'] = array($this->raw($type . '(*) as field'));
        $row = $this->get();

        // Set the select as it was
        if ($mainSelects) {
            $this->statements['selects'] = $mainSelects;
        } else {
            unset($this->statements['selects']);
        }

        if ($row[0]['field']) {
            return (int) $row[0]['field'];
        } else if ($row[0]->field) {
            return (int) $row[0]->field;
        }

        return 0;
    }

    /**
     * @param $data
     *
     * @return array|string
     */
    private function doInsert($type, $sqlOrData, $bindings)
    {
        if ($this->isRaw) {
            $sql = $sqlOrData;
            $rawSql = $this->getRawSql($sql, $bindings);
            echo $rawSql . PHP_EOL;
            try {
                list($result, $executionTime) = $this->statement($sql, $bindings);
                $return = $result->rowCount() === 1 ? $this->pdo->lastInsertId() : null;
                $this->plugin->afterInsert($rawSql, $result->rowCount(), $return, $executionTime);
            } catch (\Exception $e) {
                $this->plugin->onException($rawSql, $type, $e);
            }
        } else {
            $data = $sqlOrData;
            // If first value is not an array
            // Its not a batch insert
            if (!is_array(current($data))) {
                list($sql, $bindings) = $this->adapterInstance->$type($this->statements, $data);
                $rawSql = $this->getRawSql($sql, $bindings);
                try {
                    list($result, $executionTime) = $this->statement($sql, $bindings);
                    $return = $result->rowCount() === 1 ? $this->pdo->lastInsertId() : null;
                    $this->plugin->afterInsert($rawSql, $result->rowCount(), $return, $executionTime);
                } catch (\Exception $e) {
                    $this->plugin->onException($rawSql, $type, $e);
                }
            } else {
                // Its a batch insert
                $return = array();
                foreach ($data as $subData) {
                    list($sql, $bindings) = $this->adapterInstance->$type($this->statements, $subData);
                    $rawSql = $this->getRawSql($sql, $bindings);
                    try {
                        list($result, $executionTime) = $this->statement($sql, $bindings);
                        $insertId = null;
                        if($result->rowCount() === 1){
                            $insertId = $this->pdo->lastInsertId();
                            $return[] = $insertId;
                        }
                        $this->plugin->afterInsert($rawSql, $result->rowCount(), $insertId, $executionTime);
                    } catch (\Exception $e) {
                        $this->plugin->onException($rawSql, $type, $e);
                    }
                }
            }
        }

        return $return;
    }

    /**
     * @param $data
     *
     * @return array|string
     */
    public function insert($sqlOrData, $bindings = null)
    {
        return $this->doInsert('insert', $sqlOrData, $bindings);
    }

    /**
     * @param $data
     *
     * @return array|string
     */
    public function insertIgnore($sqlOrData, $bindings = null)
    {
        return $this->doInsert('insertignore', $sqlOrData, $bindings);
    }

    /**
     * @param $data
     *
     * @return array|string
     */
    public function replace($sqlOrData, $bindings = null)
    {
        return $this->doInsert('replace', $sqlOrData, $bindings);
    }

    /**
     * @param $data
     *
     * @return $this
     */
    public function update($sqlOrData = null, $bindings = null)
    {
        if ($this->isRaw) {
            $sql = $sqlOrData;
        } else {
            list($sql, $bindings) = $this->adapterInstance->update($this->statements, $sqlOrData);
        }
        $rawSql = $this->getRawSql($sql, $bindings);
        try {
            list($result, $executionTime) = $this->statement($sql, $bindings);
            $this->plugin->afterUpdate($rawSql, $result->rowCount(), $executionTime);
        } catch (\Exception $e) {
            $this->plugin->onException($rawSql, 'udpate', $e);
        }

        return $result;
    }

    /**
     * @param $data
     *
     * @return array|string
     */
    public function updateOrInsert($data)
    {
        if ($this->first()) {
            return $this->update($data);
        } else {
            return $this->insert($data);
        }
    }

    /**
     * @param $data
     *
     * @return $this
     */
    public function onDuplicateKeyUpdate($data)
    {
        $this->addStatement('onduplicate', $data);
        return $this;
    }

    /**
     *
     */
    public function delete($sql = null, $bindings = null)
    {
        if (!$this->isRaw) {
            list($sql, $bindings) = $this->adapterInstance->delete($this->statements, $sqlOrData);
        }
        $rawSql = $this->getRawSql($sql, $bindings);

        try {
            list($result, $executionTime) = $this->statement($sql, $bindings);
            $this->plugin->afterDelete($rawSql, $result->rowCount(), $executionTime);
        } catch (\Exception $e) {
            $this->plugin->onException($rawSql, 'delete', $e);
        }

        return $result;
    }

    /**
     * @param $value
     *
     */
    public function setIsRaw($value) 
    {
        $this->isRaw = $value;
    }

    /**
     * @param $tables
     *
     * @return static
     */
    public function table($tables)
    {
        $instance = new static($this->connection);
        $instance->setIsRaw(false);
        $tables = $this->addTablePrefix($tables, false);
        $instance->addStatement('tables', $tables);
        return $instance;
    }

    /**
     * @param $tables
     *
     * @return $this
     */
    public function from($tables)
    {
        $tables = $this->addTablePrefix($tables, false);
        $this->addStatement('tables', $tables);
        return $this;
    }

    /**
     * @param $fields
     *
     * @return $this
     */
    public function select($fields)
    {
        if (!is_array($fields)) {
            $fields = func_get_args();
        }

        $fields = $this->addTablePrefix($fields);
        $this->addStatement('selects', $fields);
        return $this;
    }

    /**
     * @param $fields
     *
     * @return $this
     */
    public function selectDistinct($fields)
    {
        $this->select($fields);
        $this->addStatement('distinct', true);
        return $this;
    }

    /**
     * @param $field
     *
     * @return $this
     */
    public function groupBy($field)
    {
        $field = $this->addTablePrefix($field);
        $this->addStatement('groupBys', $field);
        return $this;
    }

    /**
     * @param        $fields
     * @param string $defaultDirection
     *
     * @return $this
     */
    public function orderBy($fields, $defaultDirection = 'ASC')
    {
        if (!is_array($fields)) {
            $fields = array($fields);
        }

        foreach ($fields as $key => $value) {
            $field = $key;
            $type = $value;
            if (is_int($key)) {
                $field = $value;
                $type = $defaultDirection;
            }
            if (!$field instanceof Raw) {
                $field = $this->addTablePrefix($field);
            }
            $this->statements['orderBys'][] = compact('field', 'type');
        }

        return $this;
    }

    /**
     * @param $limit
     *
     * @return $this
     */
    public function limit($limit)
    {
        $this->statements['limit'] = $limit;
        return $this;
    }

    /**
     * @param $offset
     *
     * @return $this
     */
    public function offset($offset)
    {
        $this->statements['offset'] = $offset;
        return $this;
    }

    /**
     * @param        $key
     * @param        $operator
     * @param        $value
     * @param string $joiner
     *
     * @return $this
     */
    public function having($key, $operator, $value, $joiner = 'AND')
    {
        $key = $this->addTablePrefix($key);
        $this->statements['havings'][] = compact('key', 'operator', 'value', 'joiner');
        return $this;
    }

    /**
     * @param        $key
     * @param        $operator
     * @param        $value
     *
     * @return $this
     */
    public function orHaving($key, $operator, $value)
    {
        return $this->having($key, $operator, $value, 'OR');
    }

    /**
     * @param $key
     * @param $operator
     * @param $value
     *
     * @return $this
     */
    public function where($key, $operator = null, $value = null)
    {
        // If two params are given then assume operator is =
        if (func_num_args() == 2) {
            $value = $operator;
            $operator = '=';
        }
        return $this->_where($key, $operator, $value);
    }

    /**
     * @param $key
     * @param $operator
     * @param $value
     *
     * @return $this
     */
    public function orWhere($key, $operator = null, $value = null)
    {
        // If two params are given then assume operator is =
        if (func_num_args() == 2) {
            $value = $operator;
            $operator = '=';
        }

        return $this->_where($key, $operator, $value, 'OR');
    }

    /**
     * @param $key
     * @param $operator
     * @param $value
     *
     * @return $this
     */
    public function whereNot($key, $operator = null, $value = null)
    {
        // If two params are given then assume operator is =
        if (func_num_args() == 2) {
            $value = $operator;
            $operator = '=';
        }
        return $this->_where($key, $operator, $value, 'AND NOT');
    }

    /**
     * @param $key
     * @param $operator
     * @param $value
     *
     * @return $this
     */
    public function orWhereNot($key, $operator = null, $value = null)
    {
        // If two params are given then assume operator is =
        if (func_num_args() == 2) {
            $value = $operator;
            $operator = '=';
        }
        return $this->_where($key, $operator, $value, 'OR NOT');
    }

    /**
     * @param       $key
     * @param array $values
     *
     * @return $this
     */
    public function whereIn($key, array $values)
    {
        return $this->_where($key, 'IN', $values, 'AND');
    }

    /**
     * @param       $key
     * @param array $values
     *
     * @return $this
     */
    public function whereNotIn($key, array $values)
    {
        return $this->_where($key, 'NOT IN', $values, 'AND');
    }

    /**
     * @param       $key
     * @param array $values
     *
     * @return $this
     */
    public function orWhereIn($key, array $values)
    {
        return $this->_where($key, 'IN', $values, 'OR');
    }

    /**
     * @param       $key
     * @param array $values
     *
     * @return $this
     */
    public function orWhereNotIn($key, array $values)
    {
        return $this->_where($key, 'NOT IN', $values, 'OR');
    }

    /**
     * @param $key
     * @param $valueFrom
     * @param $valueTo
     *
     * @return $this
     */
    public function whereBetween($key, $valueFrom, $valueTo)
    {
        return $this->_where($key, 'BETWEEN', array($valueFrom, $valueTo), 'AND');
    }

    /**
     * @param $key
     * @param $valueFrom
     * @param $valueTo
     *
     * @return $this
     */
    public function orWhereBetween($key, $valueFrom, $valueTo)
    {
        return $this->_where($key, 'BETWEEN', array($valueFrom, $valueTo), 'OR');
    }

    /**
     * @param $key
     * @return QueryBuilderHandler
     */
    public function whereNull($key)
    {
        return $this->_whereNull($key);
    }

    /**
     * @param $key
     * @return QueryBuilderHandler
     */
    public function whereNotNull($key)
    {
        return $this->_whereNull($key, 'NOT');
    }

    /**
     * @param $key
     * @return QueryBuilderHandler
     */
    public function orWhereNull($key)
    {
        return $this->_whereNull($key, '', 'or');
    }

    /**
     * @param $key
     * @return QueryBuilderHandler
     */
    public function orWhereNotNull($key)
    {
        return $this->_whereNull($key, 'NOT', 'or');
    }

    protected function _whereNull($key, $prefix = '', $operator = '')
    {
        $key = $this->adapterInstance->wrapSanitizer($this->addTablePrefix($key));
        return $this->{$operator . 'Where'}($this->raw("{$key} IS {$prefix} NULL"));
    }

    /**
     * @param        $table
     * @param        $key
     * @param        $operator
     * @param        $value
     * @param string $type
     *
     * @return $this
     */
    public function join($table, $key, $operator = null, $value = null, $type = 'inner')
    {
        if (!$key instanceof \Closure) {
            $key = function($joinBuilder) use ($key, $operator, $value) {
                $joinBuilder->on($key, $operator, $value);
            };
        }

        // Build a new JoinBuilder class, keep it by reference so any changes made
        // in the closure should reflect here
        $joinBuilder = $this->container->build('\\Vega\Database\\QueryBuilder\\JoinBuilder', array($this->connection));
        $joinBuilder = & $joinBuilder;
        // Call the closure with our new joinBuilder object
        $key($joinBuilder);
        $table = $this->addTablePrefix($table, false);
        // Get the criteria only query from the joinBuilder object
        $this->statements['joins'][] = compact('type', 'table', 'joinBuilder');

        return $this;
    }

    /**
     * @param      $table
     * @param      $key
     * @param null $operator
     * @param null $value
     *
     * @return $this
     */
    public function leftJoin($table, $key, $operator = null, $value = null)
    {
        return $this->join($table, $key, $operator, $value, 'left');
    }

    /**
     * @param      $table
     * @param      $key
     * @param null $operator
     * @param null $value
     *
     * @return $this
     */
    public function rightJoin($table, $key, $operator = null, $value = null)
    {
        return $this->join($table, $key, $operator, $value, 'right');
    }

    /**
     * @param      $table
     * @param      $key
     * @param null $operator
     * @param null $value
     *
     * @return $this
     */
    public function innerJoin($table, $key, $operator = null, $value = null)
    {
        return $this->join($table, $key, $operator, $value, 'inner');
    }

    /**
     * Add a raw query
     *
     * @param $value
     * @param $bindings
     *
     * @return mixed
     */
    public function raw($value, $bindings = array())
    {
        return $this->container->build('\\Vega\Database\\QueryBuilder\\Raw', array($value, $bindings));
    }

    /**
     * Return PDO instance
     *
     * @return PDO
     */
    public function pdo()
    {
        return $this->pdo;
    }

    /**
     * @param Connection $connection
     *
     * @return $this
     */
    public function setConnection(Connection $connection)
    {
        $this->connection = $connection;
        return $this;
    }

    /**
     * @return Connection
     */
    public function getConnection()
    {
        return $this->connection;
    }

    /**
     * @param        $key
     * @param        $operator
     * @param        $value
     * @param string $joiner
     *
     * @return $this
     */
    protected function _where($key, $operator = null, $value = null, $joiner = 'AND')
    {
        $key = $this->addTablePrefix($key);
        $this->statements['wheres'][] = compact('key', 'operator', 'value', 'joiner');
        return $this;
    }

    /**
     * Add table prefix (if given) on given string.
     *
     * @param      $values
     * @param bool $tableFieldMix If we have mixes of field and table names with a "."
     *
     * @return array|mixed
     */
    public function addTablePrefix($values, $tableFieldMix = true)
    {
        if (is_null($this->tablePrefix)) {
            return $values;
        }

        // $value will be an array and we will add prefix to all table names

        // If supplied value is not an array then make it one
        $single = false;
        if (!is_array($values)) {
            $values = array($values);
            // We had single value, so should return a single value
            $single = true;
        }

        $return = array();

        foreach ($values as $key => $value) {
            // It's a raw query, just add it to our return array and continue next
            if ($value instanceof Raw || $value instanceof \Closure) {
                $return[$key] = $value;
                continue;
            }

            // If key is not integer, it is likely a alias mapping,
            // so we need to change prefix target
            $target = &$value;
            if (!is_int($key)) {
                $target = &$key;
            }

            if (!$tableFieldMix || ($tableFieldMix && strpos($target, '.') !== false)) {
                $target = $this->tablePrefix . $target;
            }

            $return[$key] = $value;
        }

        // If we had single value then we should return a single value (end value of the array)
        return $single ? end($return) : $return;
    }

    /**
     * @param $key
     * @param $value
     */
    protected function addStatement($key, $value)
    {
        if (!is_array($value)) {
            $value = array($value);
        }

        if (!array_key_exists($key, $this->statements)) {
            $this->statements[$key] = $value;
        } else {
            $this->statements[$key] = array_merge($this->statements[$key], $value);
        }
    }

    /**
     * @return array
     */
    public function getStatements()
    {
        return $this->statements;
    }

    /**
     * Replaces any parameter placeholders in a query with the value of that
     * parameter. Useful for debugging. Assumes anonymous parameters from
     * $params are are in the same order as specified in $query
     *
     * Reference: http://stackoverflow.com/a/1376838/656489
     *
     * @param string $query  The sql query with parameter placeholders
     * @param array  $params The array of substitution parameters
     *
     * @return string The interpolated query
     */
    protected function getRawSql($query, $params)
    {
        $keys = array();
        $values = $params;

        if ($params === null) {
            return $query;
        }

        # build a regular expression for each parameter
        foreach ($params as $key => $value) {
            if (is_string($key)) {
                $keys[] = '/:' . $key . '/';
            } else {
                $keys[] = '/[?]/';
            }

            if (is_string($value)) {
                $values[$key] = $this->pdo->quote($value);
            }

            if (is_array($value)) {
                $values[$key] = implode(',', $this->pdo->quote($value));
            }

            if (is_null($value)) {
                $values[$key] = 'NULL';
            }
        }

        $query = preg_replace($keys, $values, $query, 1, $count);

        return $query;
    }
}
