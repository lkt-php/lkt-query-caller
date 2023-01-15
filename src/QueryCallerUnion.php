<?php

namespace Lkt\QueryCaller;

use Lkt\DatabaseConnectors\DatabaseConnections;
use Lkt\QueryBuilding\Constraints\SubQueryCountEqualConstraint;
use Lkt\QueryBuilding\QueryUnion;

class QueryCallerUnion extends QueryUnion
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
        return $connection->query($this->toString());
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
}