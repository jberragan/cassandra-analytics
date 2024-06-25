package org.apache.cassandra.spark.data.converter.types;

import org.apache.cassandra.spark.data.CqlField;

public class Int implements IntTrait
{
    public static final Int INSTANCE = new Int();

    public CqlField.CqlType cqlType()
    {
        return org.apache.cassandra.spark.data.types.Int.INSTANCE;
    }
}
