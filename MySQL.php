<?php

/*
 * This file is part of the Pho package.
 *
 * (c) Emre Sokullu <emre@phonetworks.org>
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
 * @author Emre Sokullu
 */
class MySQL implements IndexInterface, ServiceInterface
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
    private $table  = 'index';

    /**
     * Setup function.
     * Init elasticsearch connection. Run indexing on runned events
     * @param Kernel $kernel Kernel of pho
     * @param array  $params Sended params to the index.
     */
    public function __construct(Kernel $kernel, array $params = [])
    {
        $this->kernel = $kernel;
        if ($this->connectDatabase($params)) {
            $kernel->on('kernel.booted_up', array($this, 'kernelBooted'));
        }

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
            $this->client->query(sprintf('DELETE FROM `%s` WHERE uuid = %s', $this->table, $this->client->escape_string($entity->id()->toString())));
        }
        foreach ($entity->attributes()->toArray() as $key => $value) {
            $insert[] = '("' . $this->client->escape_string($entity->id()->toString()) . '","'
            . $this->client->escape_string((new \ReflectionClass($entity))->getShortName()) . '","'
            . $this->client->escape_string($key) . '","'
            . $this->client->escape_string($value) . '")';
        }

        if (!empty($insert)) {
            $this->client->query(sprintf('INSERT INTO `%s` (`uuid`, `class`, `key`, `value`) VALUES %s', $this->table, implode(',', $insert)));
        }
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

        $query = [];
        if (!empty($value)) {
            $where[] = ' `value` = "' . $this->client->escape_string($value) . '"';
        }
        if (!empty($key)) {
            $where[] = ' `key` = "' . $this->client->escape_string($key) . '"';
        }

        if (!empty($classes)) {
            if (is_string($classes)) {
                $where[] = ' `class` = "' . $this->client->escape_string($key) . '"';
            } else if (is_array($classes)) {
                $cls = [];
                foreach ($classes as $class) {
                    $cls[] = '"' . $this->client->escape_string($class) . '"';
                }
                $where[] = '`class` IN (' . implode(',', $cls) . ')';
            }
        }

        $result = $this->client->query(sprintf('SELECT `uuid` FROM `%s` WHERE %s GROUP BY `uuid`', $this->table, implode(' AND ', $where)));
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
        if (!$this->client) {
            return null;
        }

        $query                                               = ['query' => ['bool' => ['must' => []]]];
        $query['query']['bool']['must'][]['match']['attr.v'] = $value;
        if (!is_null($key)) {
            $query['query']['bool']['must'][]['match']['attr.k'] = $key;
        }

        $params  = $this->createQuery(implode(',', (array) $classes), null, $query);
        $results = $this->client->search($params);
        return $this->getIdsList($this->remapReturn($results));
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
        $this->database = !empty($query['database']) ? $query['database'] : '';
        $this->table    = !empty($query['table']) ? $query['table'] : 'index';

        $this->client = new \mysqli($this->host, $this->user, $this->pass, $this->database, $this->port);

        if (!$this->client) {
            $this->kernel->logger()->warning("Could not connect to the MySQL database %s", $this->dbname);
            return false;
        }

        $this->client->query(sprintf("CREATE TABLE IF NOT EXISTS `%s`( `uuid` VARCHAR(32) NOT NULL, `class` VARCHAR(255) NOT NULL, `key` VARCHAR(255) NOT NULL, `value` MEDIUMTEXT NOT NULL, INDEX (`uuid`, `class`) ) ENGINE=INNODB;", $this->table));

        if ($result = $this->client->query(sprintf("SHOW TABLES LIKE `%s`", $this->table))) {
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

}
