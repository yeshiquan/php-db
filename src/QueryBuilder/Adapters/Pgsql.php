<?php namespace Vega\Database\QueryBuilder\Adapters;


class Pgsql extends BaseAdapter
{
    /**
     * @var string
     */
    protected $sanitizer = '"';
}