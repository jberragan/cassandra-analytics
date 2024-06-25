package org.apache.cassandra.spark.data.converter.types;

import org.apache.cassandra.spark.data.CqlField;

public class Ascii implements StringTraits
{
    public static final Ascii INSTANCE = new Ascii();

    public CqlField.CqlType cqlType()
    {
        return org.apache.cassandra.spark.data.types.Ascii.INSTANCE;
    }
}
