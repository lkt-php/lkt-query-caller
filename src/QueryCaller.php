<?php

namespace Lkt\QueryCaller;

use Lkt\DatabaseConnectors\DatabaseConnections;
use Lkt\Factory\Schemas\Schema;
use Lkt\QueryBuilding\Query;
use function Lkt\Tools\Pagination\getTotalPages;

class QueryCaller extends Query
{
    protected $connector = '';
    protected $forceRefresh = false;

    /**
     * @param bool $status
     * @return $this
     */
    public function setForceRefresh(bool $status): self
    {
        $this->forceRefresh= $status;
        return $this;
    }

    /**
     * @param string $name
     * @return $this
     */
    public function setDatabaseConnector(string $name): self
    {
        $this->connector = $name;
        return $this;
    }

    /**
     * @return array
     * @throws \Exception
     */
    final public function select(): array
    {
        $connector = $this->connector;
        if ($connector === '') {
            $connector = DatabaseConnections::$defaultConnector;
        }
        $connection = DatabaseConnections::get($connector);
        if ($this->forceRefresh) {
            $connection->forceRefreshNextQuery();
        }
        $r = $connection->query($this->getSelectQuery());

        if (!is_array($r)) {
            $r = [];
        }
        return $r;
    }

    /**
     * @return array
     * @throws \Exception
     */
    final public function selectDistinct(): array
    {
        $connector = $this->connector;
        if ($connector === '') {
            $connector = DatabaseConnections::$defaultConnector;
        }
        $connection = DatabaseConnections::get($connector);
        if ($this->forceRefresh) {
            $connection->forceRefreshNextQuery();
        }
        return $connection->query($this->getSelectDistinctQuery());
    }

    /**
     * @param string $countableField
     * @return int
     * @throws \Exception
     */
    final public function count(string $countableField): int
    {
        $connector = $this->connector;
        if ($connector === '') {
            $connector = DatabaseConnections::$defaultConnector;
        }
        $connection = DatabaseConnections::get($connector);
        if ($this->forceRefresh) {
            $connection->forceRefreshNextQuery();
        }
        $results = $connection->query($this->getCountQuery($countableField));
        return (int)$results[0]['Count'];
    }

    /**
     * @param string $countableField
     * @return int
     * @throws \Exception
     */
    final public function pages(string $countableField): int
    {
        return getTotalPages($this->count($countableField), $this->limit);
    }

    /**
     * @return bool
     * @throws \Exception
     */
    final public function insert(): bool
    {
        $connector = $this->connector;
        if ($connector === '') {
            $connector = DatabaseConnections::$defaultConnector;
        }
        $connection = DatabaseConnections::get($connector);
        if ($this->forceRefresh) {
            $connection->forceRefreshNextQuery();
        }
        $connection->query($this->getInsertQuery());
        return true;
    }

    /**
     * @return bool
     * @throws \Exception
     */
    final public function update(): bool
    {
        $connector = $this->connector;
        if ($connector === '') {
            $connector = DatabaseConnections::$defaultConnector;
        }
        $connection = DatabaseConnections::get($connector);
        if ($this->forceRefresh) {
            $connection->forceRefreshNextQuery();
        }
        $connection->query($this->getUpdateQuery());
        return true;
    }

    final public function extractSchemaColumns(Schema $schema)
    {
        $connector = $this->connector;
        if ($connector === '') {
            $connector = DatabaseConnections::$defaultConnector;
        }
        $connection = DatabaseConnections::get($connector);
        if ($this->forceRefresh) {
            $connection->forceRefreshNextQuery();
        }
        $this->setColumns($connection->extractSchemaColumns($schema));
    }
}