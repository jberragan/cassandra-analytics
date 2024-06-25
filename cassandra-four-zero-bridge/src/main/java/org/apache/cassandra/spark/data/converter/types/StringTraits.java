package org.apache.cassandra.spark.data.converter.types;

import org.apache.cassandra.bridge.BigNumberConfig;
import org.apache.spark.sql.Row;
import org.apache.spark.sql.catalyst.expressions.GenericInternalRow;
import org.apache.spark.sql.types.DataType;
import org.apache.spark.sql.types.DataTypes;
import org.apache.spark.unsafe.types.UTF8String;
import org.jetbrains.annotations.NotNull;

public interface StringTraits extends SparkType
{
    @Override
    default Object toSparkSqlType(@NotNull Object o, boolean isFrozen)
    {
        return UTF8String.fromString(o.toString()); // UTF8String
    }

    @Override
    default Object nativeSparkSqlRowValue(Row row, int pos)
    {
        return row.getString(pos);
    }

    @Override
    default Object nativeSparkSqlRowValue(final GenericInternalRow row, final int pos)
    {
        return row.getString(pos);
    }

    @Override
    default Object toTestRowType(Object value)
    {
        if (value instanceof UTF8String)
        {
            return ((UTF8String) value).toString();
        }
        return value;
    }

    @Override
    default DataType dataType(BigNumberConfig bigNumberConfig)
    {
        return DataTypes.StringType;
    }
}
