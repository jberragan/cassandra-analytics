package org.apache.cassandra.spark.data.converter.types;

import org.apache.cassandra.spark.data.CqlField;

public class VarChar implements StringTraits
{
    public static final VarChar INSTANCE = new VarChar();

    public CqlField.CqlType cqlType()
    {
        return org.apache.cassandra.spark.data.types.VarChar.INSTANCE;
    }
}
