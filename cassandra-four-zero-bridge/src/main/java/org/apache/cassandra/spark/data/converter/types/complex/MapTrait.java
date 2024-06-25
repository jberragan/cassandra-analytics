package org.apache.cassandra.spark.data.converter.types.complex;

import org.apache.cassandra.spark.data.converter.types.SparkType;

public interface MapTrait extends SparkType, CollectionTrait
{

    default SparkType keyType()
    {
        return sparkType();
    }

    default SparkType valueType()
    {
        return sparkType(1);
    }
}
