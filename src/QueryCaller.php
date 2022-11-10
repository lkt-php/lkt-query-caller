<?php

namespace Lkt\QueryCaller;

use Lkt\DatabaseConnectors\DatabaseConnections;
use Lkt\Factory\Schemas\Schema;
use Lkt\QueryBuilding\Query;
use function Lkt\Tools\Pagination\getTotalPages;

class QueryCaller extends Query
{
    protected $connector = '';

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
        $connection = DatabaseConnections::get($this->connector);
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
        $connection = DatabaseConnections::get($this->connector);
        return $connection->query($this->getSelectDistinctQuery());
    }

    /**
     * @param string $countableField
     * @return int
     * @throws \Exception
     */
    final public function count(string $countableField): int
    {
        $connection = DatabaseConnections::get($this->connector);
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
        $connection = DatabaseConnections::get($this->connector);
        $connection->query($this->getInsertQuery());
        return true;
    }

    /**
     * @return bool
     * @throws \Exception
     */
    final public function update(): bool
    {
        $connection = DatabaseConnections::get($this->connector);
        $connection->query($this->getUpdateQuery());
        return true;
    }

    final public function extractSchemaColumns(Schema $schema)
    {
        $connection = DatabaseConnections::get($this->connector);
        $this->setColumns($connection->extractSchemaColumns($schema));
    }
}