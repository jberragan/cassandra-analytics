package org.apache.cassandra.spark.data.converter.types;

import org.apache.cassandra.bridge.BigNumberConfig;
import org.apache.cassandra.spark.data.CqlField;
import org.apache.spark.sql.Row;
import org.apache.spark.sql.types.DataType;
import org.apache.spark.sql.types.DataTypes;
import org.jetbrains.annotations.NotNull;

public class Timestamp implements LongTraits
{
    public static final Timestamp INSTANCE = new Timestamp();

    @Override
    public DataType dataType(BigNumberConfig bigNumberConfig)
    {
        return DataTypes.TimestampType;
    }

    @Override
    public Object toSparkSqlType(@NotNull Object o, boolean isFrozen)
    {
        return ((java.util.Date) o).getTime() * 1000L; // long
    }

    @Override
    public Object nativeSparkSqlRowValue(Row row, int pos)
    {
        return new java.util.Date(row.getTimestamp(pos).getTime());
    }

    @Override
    public Object toTestRowType(Object value)
    {
        if (value instanceof java.util.Date)
        {
            return value;
        }
        return new java.util.Date((long) value / 1000L);
    }

    public CqlField.CqlType cqlType()
    {
        return null;
    }
}
