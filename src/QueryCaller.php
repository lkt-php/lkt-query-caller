<?php

namespace Lkt\QueryCaller;

use Exception;
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
        $r = $connection->query($connection->getSelectQuery($this));

        if (!is_array($r)) {
            $r = [];
        }
        return $r;
    }

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
        return $connection->query($connection->getSelectDistinctQuery($this));
    }

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
        $results = $connection->query($connection->getCountQuery($this, $countableField));
        return (int)$results[0]['Count'];
    }

    final public function pages(string $countableField): int
    {
        return getTotalPages($this->count($countableField), $this->limit);
    }

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
        $connection->query($connection->getInsertQuery($this));
        return true;
    }

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
        $connection->query($connection->getUpdateQuery($this));
        return true;
    }

    final public function delete(): bool
    {
        $connector = $this->connector;
        if ($connector === '') {
            $connector = DatabaseConnections::$defaultConnector;
        }
        $connection = DatabaseConnections::get($connector);
        if ($this->forceRefresh) {
            $connection->forceRefreshNextQuery();
        }
        $connection->query($connection->getDeleteQuery($this));
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

    final public function getSelectQuery(): string
    {
        $connector = $this->connector;
        if ($connector === '') {
            $connector = DatabaseConnections::$defaultConnector;
        }
        $connection = DatabaseConnections::get($connector);
        if ($this->forceRefresh) {
            $connection->forceRefreshNextQuery();
        }
        return $connection->getSelectQuery($this);
    }

    final public function getSelectDistinctQuery(): string
    {
        $connector = $this->connector;
        if ($connector === '') {
            $connector = DatabaseConnections::$defaultConnector;
        }
        $connection = DatabaseConnections::get($connector);
        if ($this->forceRefresh) {
            $connection->forceRefreshNextQuery();
        }
        return $connection->getSelectDistinctQuery($this);
    }

    final public function getCountQuery(string $countableField): string
    {
        $connector = $this->connector;
        if ($connector === '') {
            $connector = DatabaseConnections::$defaultConnector;
        }
        $connection = DatabaseConnections::get($connector);
        if ($this->forceRefresh) {
            $connection->forceRefreshNextQuery();
        }
        return $connection->getCountQuery($this, $countableField);
    }

    final public function getInsertQuery(): string
    {
        $connector = $this->connector;
        if ($connector === '') {
            $connector = DatabaseConnections::$defaultConnector;
        }
        $connection = DatabaseConnections::get($connector);
        if ($this->forceRefresh) {
            $connection->forceRefreshNextQuery();
        }
        return $connection->getInsertQuery($this);
    }

    final public function getUpdateQuery(): string
    {
        $connector = $this->connector;
        if ($connector === '') {
            $connector = DatabaseConnections::$defaultConnector;
        }
        $connection = DatabaseConnections::get($connector);
        if ($this->forceRefresh) {
            $connection->forceRefreshNextQuery();
        }
        return $connection->getUpdateQuery($this);
    }

    final public function getDeleteQuery(): string
    {
        $connector = $this->connector;
        if ($connector === '') {
            $connector = DatabaseConnections::$defaultConnector;
        }
        $connection = DatabaseConnections::get($connector);
        if ($this->forceRefresh) {
            $connection->forceRefreshNextQuery();
        }
        return $connection->getDeleteQuery($this);
    }
}