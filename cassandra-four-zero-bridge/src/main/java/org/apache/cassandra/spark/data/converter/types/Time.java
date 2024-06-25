package org.apache.cassandra.spark.data.converter.types;

import org.apache.cassandra.spark.data.CqlField;

public class Time implements LongTraits
{
    public static final Time INSTANCE = new Time();

    public CqlField.CqlType cqlType()
    {
        return org.apache.cassandra.spark.data.types.Time.INSTANCE;
    }
}
