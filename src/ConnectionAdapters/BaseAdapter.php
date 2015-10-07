<?php namespace Vega\Database\ConnectionAdapters;

abstract class BaseAdapter
{
    /**
     * @var \Vega\Database\Container
     */
    protected $container;

    /**
     * @param \Vega\Database\Container $container
     */
    public function __construct(\Vega\Database\Container $container)
    {
        $this->container = $container;
    }

    /**
     * @param $config
     *
     * @return \PDO
     */
    public function connect($config)
    {
        if (!isset($config['options'])) {
            $config['options'] = array();
        }
        return $this->doConnect($config);
    }

    /**
     * @param $config
     *
     * @return mixed
     */
    abstract protected function doConnect($config);
}
