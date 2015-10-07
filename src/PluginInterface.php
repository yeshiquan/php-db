<?php namespace Vega\Database;

interface PluginInterface 
{
/**
 * @param $executionTime 连接耗时
 */
public function afterConnect($executionTime);

/**
 * @param $sql 执行的sql
 * @param $resNum 记录条数
 * @param $executionTime 连接耗时
 */
public function afterSelect($sql, $resNum, $executionTime);

/**
 * @param $sql 执行的sql
 * @param $lastInsertId 自增的id
 * @param $affectedNum 影响的条数
 * @param $executionTime 连接耗时
 */
public function afterInsert($sql, $lastInsertId, $affectedNum, $executionTime);

/**
 * @param $sql 执行的sql
 * @param $affectedNum 影响的条数
 * @param $executionTime 连接耗时
 */
public function afterUpdate($sql, $affectedNum, $executionTime);

/**
 * @param $sql 执行的sql
 * @param $affectedNum 影响的条数
 * @param $executionTime 连接耗时
 */
public function afterDelete($sql, $affectedNum, $executionTime);

/**
 * @param $sql 执行的sql
 * @param $type sql的类型，取值为：select, insert, update, delete
 * @param $executionTime 连接耗时
 */
public function onException($sql, $type, $exception);
}
