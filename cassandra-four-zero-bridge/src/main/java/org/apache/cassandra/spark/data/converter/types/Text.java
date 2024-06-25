package org.apache.cassandra.spark.data.converter.types;

import org.apache.cassandra.spark.data.CqlField;

public class Text implements StringTraits
{
    public static final Text INSTANCE = new Text();

    public CqlField.CqlType cqlType()
    {
        return org.apache.cassandra.spark.data.types.Text.INSTANCE;
    }
}
