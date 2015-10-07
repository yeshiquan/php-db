<?php namespace Vega\Database;

class Container
{
    /**
     * Singleton instances
     *
     * @var array
     */
    public $singletons = array();

    /**
     * If we have a registry for the given key
     *
     * @param  string $key
     *
     * @return bool
     */
    public function has($key)
    {
        return array_key_exists($key, $this->singletons);
    }

    /**
     * Build from the given key.
     * If there is a class registered with Container::set() then it's instance
     * will be returned. If a closure is registered, a closure's return value
     * will be returned. If nothing is registered then it will try to build an
     * instance with new $key(...).
     *
     * $parameters will be passed to closure or class constructor.
     *
     *
     * @param  string $key
     * @param  array  $parameters
     *
     * @return mixed
     */
    public function build($key, $parameters = array())
    {
        // If we have a singleton instance registered the just return it
        if (array_key_exists($key, $this->singletons)) {
            return $this->singletons[$key];
        }
        $object = $key;
        $instance = $this->instanciate($object, $parameters);
        $this->singletons[$key] = $instance;

        return $instance;
    }

    /**
     * Instantiate an instance of the given type.
     *
     * @param  string $key
     * @param  array  $parameters
     *
     * @throws \Exception
     * @return mixed
     */
    protected function instanciate($key, $parameters = null)
    {

        if ($key instanceof \Closure) {
            return call_user_func_array($key, $parameters);
        }

        $reflection = new \ReflectionClass($key);
        return $reflection->newInstanceArgs($parameters);
    }
}
