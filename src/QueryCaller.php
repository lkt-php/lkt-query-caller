<?php

namespace Lkt\QueryCaller;

use Lkt\DatabaseConnectors\DatabaseConnections;
use Lkt\Factory\Schemas\Schema;
use Lkt\QueryBuilding\Constraints\SubQueryCountEqualConstraint;
use Lkt\QueryBuilding\Constraints\FieldInSubQueryConstraint;
use Lkt\QueryBuilding\Constraints\FieldNotInSubQueryConstraint;
use Lkt\QueryBuilding\Query;
use function Lkt\Tools\Pagination\getTotalPages;

class QueryCaller extends Query
{
    const COMPONENT = null;

    protected string $connector = '';
    protected bool $forceRefresh = false;

    /**
     * @param bool $status
     * @return $this
     */
    public function setForceRefresh(bool $status): static
    {
        $this->forceRefresh= $status;
        return $this;
    }

    /**
     * @param string $name
     * @return $this
     */
    public function setDatabaseConnector(string $name): static
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
        $r = $connection->query($this->getSelectQuery());

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
        return $connection->query($this->getSelectDistinctQuery());
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
        $results = $connection->query($this->getCountQuery($countableField));
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
        $connection->query($this->getInsertQuery());
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
        $connection->query($this->getUpdateQuery());
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
        $connection->query($this->getDeleteQuery());
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
        return $connection->getSelectQuery($this);
    }

    final public function getSelectDistinctQuery(): string
    {
        $connector = $this->connector;
        if ($connector === '') {
            $connector = DatabaseConnections::$defaultConnector;
        }
        $connection = DatabaseConnections::get($connector);
        return $connection->getSelectDistinctQuery($this);
    }

    final public function getCountQuery(string $countableField): string
    {
        $connector = $this->connector;
        if ($connector === '') {
            $connector = DatabaseConnections::$defaultConnector;
        }
        $connection = DatabaseConnections::get($connector);
        return $connection->getCountQuery($this, $countableField);
    }

    final public function getInsertQuery(): string
    {
        $connector = $this->connector;
        if ($connector === '') {
            $connector = DatabaseConnections::$defaultConnector;
        }
        $connection = DatabaseConnections::get($connector);
        return $connection->getInsertQuery($this);
    }

    final public function getUpdateQuery(): string
    {
        $connector = $this->connector;
        if ($connector === '') {
            $connector = DatabaseConnections::$defaultConnector;
        }
        $connection = DatabaseConnections::get($connector);
        return $connection->getUpdateQuery($this);
    }

    final public function getDeleteQuery(): string
    {
        $connector = $this->connector;
        if ($connector === '') {
            $connector = DatabaseConnections::$defaultConnector;
        }
        $connection = DatabaseConnections::get($connector);
        return $connection->getDeleteQuery($this);
    }

    final public function andSubQueryCountEqual(QueryCaller $query, int $value, string $countableField): static
    {
        $this->and[] = SubQueryCountEqualConstraint::define($query->getCountQuery($countableField), $value);
        return $this;
    }

    final public function orSubQueryCountEqual(QueryCaller $query, int $value, string $countableField): static
    {
        $this->and[] = SubQueryCountEqualConstraint::define($query->getCountQuery($countableField), $value);
        return $this;
    }

    final public function andFieldInSubQuery(string $value, QueryCaller $query): static
    {
        $this->and[] = FieldInSubQueryConstraint::define($value, $query->getSelectDistinctQuery());
        return $this;
    }

    final public function orFieldInSubQuery(string $value, QueryCaller $query): static
    {
        $this->and[] = FieldInSubQueryConstraint::define($value, $query->getSelectDistinctQuery());
        return $this;
    }

    final public function andFieldNotInSubQuery(string $value, QueryCaller $query): static
    {
        $this->and[] = FieldNotInSubQueryConstraint::define($value, $query->getSelectDistinctQuery());
        return $this;
    }

    final public function orFieldNotInSubQuery(string $value, QueryCaller $query): static
    {
        $this->and[] = FieldNotInSubQueryConstraint::define($value, $query->getSelectDistinctQuery());
        return $this;
    }
}