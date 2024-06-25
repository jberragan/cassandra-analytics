package org.apache.cassandra.spark.data.converter.types;

import org.apache.cassandra.spark.data.CqlField;

public class BigInt implements LongTraits
{
    public static final BigInt INSTANCE = new BigInt();

    public CqlField.CqlType cqlType()
    {
        return org.apache.cassandra.spark.data.types.BigInt.INSTANCE;
    }
}
