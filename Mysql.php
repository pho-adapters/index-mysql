<?php

/*
 * This file is part of the Pho package.
 *
 * (c) phonetworks.net
 *
 * For the full copyright and license information, please view the LICENSE
 * file that was distributed with this source code.
 */

namespace Pho\Kernel\Services\Index\Adapters;

use Pho\Kernel\Kernel;
use Pho\Kernel\Services\Index\IndexInterface;
use Pho\Kernel\Services\ServiceInterface;
use Pho\Lib\Graph\EntityInterface;

/**
 * File based logging. The log files specified at the kernel
 * constructor level or via the *configure* function of the kernel.
 *
 * @author Andrii Cherytsya <poratuk@gmail.com>
 */
class Mysql implements IndexInterface, ServiceInterface
{
    /**
     * Pho-kernel
     * @var \Pimple
     */
    protected $kernel;

    /**
     *  MySqli client connection for Storage
     * @var \Mysqli
     */
    protected $client;

    /**
     * Db connection data
     */
    private $host;
    private $port;
    private $user;
    private $pass;
    private $dbname = 'phonetworks';
    private $table  = 'kernel_index';

    /**
     * Setup function.
     * Init elasticsearch connection. Run indexing on runned events
     * @param Kernel $kernel Kernel of pho
     * @param array  $params Sended params to the index.
     */
    public function __construct(Kernel $kernel, array $params = [])
    {
        $this->kernel = $kernel;
        $this->connectDatabase($params);
    }

    /**
     * Indexes an entity.
     *
     * @param EntityInterface $entity An entity object; node or edge.
     * @param bool $new Whether the object has just been initialized, hence never been indexed before.
     *
     * @return void
     */
    public function index(EntityInterface $entity, bool $new = false): void
    {
        if (!$this->client) {
            return;
        }

        $classes = [get_class($entity) => get_class($entity)] + class_parents($entity);

        $insert = [];
        if ($new == false) {
            $remove = $this->client->prepare('DELETE FROM `'.$this->table.'` WHERE uuid = ?');
            $remove->bind_param('s', $entity->id()->toString());
            $remove->execute();
            $remove->close();
        }

        $key = ''; 
        $value = '';

        $insert = $this->client->prepare('INSERT INTO `'.$this->table.'` (`uuid`, `class`, `key`, `value`) VALUES (?, ?, ?, ?)');

        foreach ($entity->attributes()->toArray() as $key => $value) {
            $insert->bind_param('ssss', $entity->id()->toString(), (new \ReflectionClass($entity))->getShortName(), $key, $value);
            $insert->execute();
        }
        $insert->close();
    }

    /**
     * Searches through the index with given key and its value.
     *
     * @param string $value Value to search
     * @param string $key The key to search for. Optional.
     * @param array $classes The object classes to search for. Optional.
     *
     * @return array
     */
    public function search(string $value, string $key = null, array $classes = array()): array
    {
        if (!$this->client) {
            return [];
        }

        $select = false;
        if (empty($classes)) {
            $select = $this->client->prepare('SELECT `uuid` FROM `'.$this->table.'` WHERE `value` = ? AND `key` = ?');
            $select->bind_param('ss', $value, $key);
        }
        $params = [];
        $query = [];
        if (!empty($value)) {
            $where[] = ' `value` LIKE ?';
            $params[] = "%{$value}%";
        }
        if (!empty($key)) {
            $where[] = ' `key` = ?';
            $params[] = $key;
        }

        if (!empty($classes)) {
            if (is_string($classes)) {
                $where[] = ' `class` = ?';
                $params[] = $classes;
            } else if (is_array($classes)) {
                $clause = implode(',', array_fill(0, count($classes), '?'));
                $where[] = '`class` IN (' . $clause . ')';
                $params = array_merge($params, $classes);
            }
        }
        
        $select = $this->client->prepare('SELECT `uuid` FROM `'.$this->table.'` WHERE '.implode(' AND ', $where).' GROUP BY `uuid`');
        
        $select->bind_param(str_repeat('s', count($params)), ...$params);
        $select->execute();
        $result = $select->get_result();
        $select->close();

        $ids    = [];

        while ($row = $result->fetch_assoc()) {
            $ids[] = $row['uuid'];
        }
        return $ids;
    }

    /**
     * Searches through the index with given key and its value.
     *
     * Returns the entity IDs as string
     *
     * @param string $value Value to search
     * @param string $key The key to search for. Optional.
     * @param array $classes The object classes to search for. Optional.
     *
     * @return array Entity IDs (in string format) in question
     */
    public function searchFlat(string $value, string $key = "", array $classes = array()): array
    {
        return [];
    }

    /**
     * Function must return mysqli connection for database
     */
    private function connectDatabase($params)
    {
        if (!function_exists('mysqli_connect')) {
            $this->kernel->logger()->warning('MySQLi extention not installed. Index not working.');
            return false;
        }

        $query = [];
        parse_str($params['query'], $query);

        $this->host     = !empty($params['host']) ? $params['host'] : ini_get("mysqli.default_host");
        $this->user     = !empty($params['user']) ? $params['user'] : ini_get("mysqli.default_user");
        $this->pass     = isset($params['pass']) ? $params['pass'] : ini_get("mysqli.default_pw");
        $this->port     = isset($params['port']) ? (int) $params['port'] : ini_get("mysqli.default_port");
        $this->database = !empty($query['database']) ? $this->prepareName($query['database']) : $this->database;
        $this->table    = !empty($query['table']) ? $this->prepareName($query['table']) : $this->table;

        $this->client = new \mysqli($this->host, $this->user, $this->pass, $this->database, $this->port);

        if (!$this->client) {
            $this->kernel->logger()->warning("Could not connect to the MySQL database %s", $this->dbname);
            return false;
        }

        $this->client->set_charset('utf8');

        $this->client->query("CREATE TABLE IF NOT EXISTS `".$this->table."`( `uuid` VARCHAR(32) NOT NULL, `class` VARCHAR(255) NOT NULL, `key` VARCHAR(255) NOT NULL, `value` TEXT NOT NULL, INDEX (`uuid`, `class`) ) ENGINE=INNODB;");

        if ($result = $this->client->query(sprintf("SHOW TABLES LIKE `".$this->table."`"))) {
            if ($result->num_rows == 0) {
                $this->kernel->logger()->Warning("Could not create MySQL table %s", $this->dbtable);
                return false;
            }
        }

        return true;
    }

    /**
     * Get current main class
     * @param  [type] $classes [description]
     * @return [type]          [description]
     */
    public function getTypeFromClass($classes)
    {
        $type  = 'entity';
        $class = false;

        if (is_array($classes)) {
            $class = array_shift($classes);
        }

        if (is_string($classes)) {
            $class = $classes;
        }

        if ($class) {
            $type = (new \ReflectionClass($class))->getShortName();
        }

        return $type;
    }

    private function prepareName(string $name)
    {

        return preg_replace("/[^A-Za-z0-9_$]+/", '', $name);
    }

}
