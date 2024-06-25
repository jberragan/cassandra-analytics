package org.apache.cassandra.spark.data.converter.types;

import java.nio.ByteBuffer;

import org.apache.cassandra.spark.data.CqlField;
import org.apache.cassandra.spark.utils.ByteBufferUtils;
import org.jetbrains.annotations.NotNull;

public class Blob implements BinaryTrait
{
    public static final Blob INSTANCE = new Blob();

    public Object toSparkSqlType(@NotNull Object o, boolean isFrozen)
    {
        return ByteBufferUtils.getArray((ByteBuffer) o); // byte[]
    }

    public CqlField.CqlType cqlType()
    {
        return org.apache.cassandra.spark.data.types.Blob.INSTANCE;
    }
}
