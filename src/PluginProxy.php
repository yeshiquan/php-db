<?php namespace Vega\Database;

class PluginProxy {

    protected $plugin;

    public function __construct($plugin) 
    {
        $this->plugin = $plugin;
    }

    public function __call($method, $args) 
    {
        if ($this->plugin) {
            call_user_func_array(array($this->plugin, $method), $args);
        }
    }
}
