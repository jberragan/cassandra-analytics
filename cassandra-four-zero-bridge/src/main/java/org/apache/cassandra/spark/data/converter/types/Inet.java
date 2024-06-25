package org.apache.cassandra.spark.data.converter.types;

import java.net.InetAddress;
import java.net.UnknownHostException;

import org.apache.cassandra.spark.data.CqlField;
import org.jetbrains.annotations.NotNull;

public class Inet implements BinaryTrait
{
    public static Inet INSTANCE = new Inet();

    @Override
    public Object toSparkSqlType(@NotNull Object o, boolean isFrozen)
    {
        return ((InetAddress) o).getAddress(); // byte[]
    }

    @Override
    public Object toTestRowType(Object value)
    {
        try
        {
            return InetAddress.getByAddress((byte[]) value);
        }
        catch (final UnknownHostException e)
        {
            throw new RuntimeException(e);
        }
    }

    public CqlField.CqlType cqlType()
    {
        return org.apache.cassandra.spark.data.types.Inet.INSTANCE;
    }
}
