<?php namespace Vega\Database;

use Vega\Database\QueryBuilder\Raw;
use Vega\Database\Container;

class Connection
{

    /**
     * @var Container
     */
    protected $container;

    /**
     * @var string
     */
    protected $adapter;

    /**
     * @var PluginProxy
     */
    protected $plugin;

    /**
     * @var array
     */
    protected $adapterConfig;

    /**
     * @var \PDO
     */
    protected $pdoInstance;

    /**
     * @var Connection
     */
    protected static $storedConnection;

    /**
     * @param               $adapter
     * @param array         $adapterConfig
     * @param null|string   $alias
     * @param Container     $container
     */
    public function __construct($adapter, array $adapterConfig, $plugin = null, $alias = null)
    {
        $this->container = new Container();
        if (is_string($plugin)) {
            $alias = $plugin;
            $plugin = null;
        }
        $this->plugin = new PluginProxy($plugin);
        $this->setAdapter($adapter)->setAdapterConfig($adapterConfig)->connect();

        if ($alias) {
            $this->createAlias($alias);
        }
    }

    /**
     * Create an easily accessible query builder alias
     *
     * @param $alias
     */
    public function createAlias($alias)
    {
        class_alias('Vega\Database\\AliasFacade', $alias);
        $builder = $this->container->build('\\Vega\Database\\QueryBuilder\\QueryBuilderHandler', array($this));
        AliasFacade::setQueryBuilderInstance($builder);
    }

    /**
     * Returns an instance of Query Builder
     */
    public function getQueryBuilder()
    {
        return $this->container->build('\\Vega\Database\\QueryBuilder\\QueryBuilderHandler', array($this));
    }


    /**
     * Create the connection adapter
     */
    public function connect()
    {
        // Build a database connection if we don't have one connected

        $adapter = '\\Vega\Database\\ConnectionAdapters\\' . ucfirst(strtolower($this->adapter));

        $adapterInstance = $this->container->build($adapter, array($this->container));

        $start = microtime(true);
        $pdo = $adapterInstance->connect($this->adapterConfig);
        $executionTime = microtime(true) - $start;
        $this->plugin->afterConnect($executionTime);
        $this->setPdoInstance($pdo);

        // Preserve the first database connection with a static property
        if (!static::$storedConnection) {
            static::$storedConnection = $this;
        }
    }

    /**
     * @param \PDO $pdo
     *
     * @return $this
     */
    public function setPdoInstance(\PDO $pdo)
    {
        $this->pdoInstance = $pdo;
        return $this;
    }

    /**
     * @return \PDO
     */
    public function getPdoInstance()
    {
        return $this->pdoInstance;
    }

    /**
     * @param $adapter
     *
     * @return $this
     */
    public function setAdapter($adapter)
    {
        $this->adapter = $adapter;
        return $this;
    }

    /**
     * @return string
     */
    public function getAdapter()
    {
        return $this->adapter;
    }

    /**
     * @param array $adapterConfig
     *
     * @return $this
     */
    public function setAdapterConfig(array $adapterConfig)
    {
        $this->adapterConfig = $adapterConfig;
        return $this;
    }

    /**
     * @return array
     */
    public function getAdapterConfig()
    {
        return $this->adapterConfig;
    }

    /**
     * @return Container
     */
    public function getContainer()
    {
        return $this->container;
    }

    /**
     * @return PluginProxy
     */
    public function getPlugin()
    {
        return $this->plugin;
    }

    /**
     * @return Connection
     */
    public static function getStoredConnection()
    {
        return static::$storedConnection;
    }
}
