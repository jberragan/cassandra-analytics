package org.apache.cassandra.spark.data.converter.types;

import org.apache.cassandra.spark.data.CqlField;

public class TimeUUID implements UUIDTraits
{
    public static final TimeUUID INSTANCE = new TimeUUID();

    public CqlField.CqlType cqlType()
    {
        return org.apache.cassandra.spark.data.types.TimeUUID.INSTANCE;
    }
}
