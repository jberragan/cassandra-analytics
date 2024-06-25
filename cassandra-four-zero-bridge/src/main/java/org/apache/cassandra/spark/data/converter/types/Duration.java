package org.apache.cassandra.spark.data.converter.types;

import org.apache.cassandra.spark.data.CqlField;

public class Duration implements NotImplementedTrait
{
    public static final Duration INSTANCE = new Duration();

    public CqlField.CqlType cqlType()
    {
        return org.apache.cassandra.spark.data.types.Duration.INSTANCE;
    }
}
