package org.apache.cassandra.spark.data.converter.types;

import org.apache.cassandra.spark.data.CqlField;

public class UUID implements UUIDTraits
{
    public static final UUID INSTANCE = new UUID();

    public CqlField.CqlType cqlType()
    {
        return org.apache.cassandra.spark.data.types.UUID.INSTANCE;
    }
}
