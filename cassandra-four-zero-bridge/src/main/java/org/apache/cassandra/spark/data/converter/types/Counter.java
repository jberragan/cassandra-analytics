package org.apache.cassandra.spark.data.converter.types;

import org.apache.cassandra.spark.data.CqlField;

public class Counter implements NotImplementedTrait
{
    public static final Counter INSTANCE = new Counter();

    public CqlField.CqlType cqlType()
    {
        return org.apache.cassandra.spark.data.types.Counter.INSTANCE;
    }
}
