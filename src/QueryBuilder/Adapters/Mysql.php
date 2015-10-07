<?php namespace Vega\Database\QueryBuilder\Adapters;


class Mysql extends BaseAdapter
{
    /**
     * @var string
     */
    protected $sanitizer = '`';
}