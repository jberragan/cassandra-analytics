package org.apache.cassandra.spark.data.converter.types;

import java.util.Comparator;

import org.apache.cassandra.bridge.BigNumberConfig;
import org.apache.cassandra.spark.data.CqlField;
import org.apache.spark.sql.Row;
import org.apache.spark.sql.catalyst.expressions.GenericInternalRow;
import org.apache.spark.sql.types.DataType;
import org.jetbrains.annotations.NotNull;

public interface SparkType extends Comparator<Object>
{
    DataType dataType(BigNumberConfig bigNumberConfig);

    default Object toSparkSqlType(@NotNull Object o, boolean isFrozen)
    {
        return o;
    }

    default Object sparkSqlRowValue(GenericInternalRow row, int pos)
    {
        // we need to convert native types to TestRow types
        return row.isNullAt(pos) ? null : toTestRowType(nativeSparkSqlRowValue(row, pos));
    }

    default Object nativeSparkSqlRowValue(final GenericInternalRow row, final int pos)
    {
        // we need to convert native types to TestRow types
        return row.isNullAt(pos) ? null : toTestRowType(nativeSparkSqlRowValue(row, pos));
    }

    default Object sparkSqlRowValue(Row row, int pos)
    {
        // we need to convert native types to TestRow types
        return row.isNullAt(pos) ? null : toTestRowType(nativeSparkSqlRowValue(row, pos));
    }

    default Object nativeSparkSqlRowValue(Row row, int pos)
    {
        // we need to convert native types to TestRow types
        return row.isNullAt(pos) ? null : toTestRowType(nativeSparkSqlRowValue(row, pos));
    }

    default Object toTestRowType(Object value)
    {
        return value;
    }

    default boolean equals(Object o1, Object o2)
    {
        if (o1 == o2)
        {
            return true;
        }
        else if (o1 == null || o2 == null)
        {
            return false;
        }
        return equalsTo(o1, o2);
    }

    default boolean equalsTo(Object o1, Object o2)
    {
        return compare(o1, o2) == 0;
    }

    default int compare(Object o1, Object o2)
    {
        if (o1 == null || o2 == null)
        {
            return o1 == o2 ? 0 : (o1 == null ? -1 : 1);
        }
        return compareTo(o1, o2);
    }

    CqlField.CqlType cqlType();

    default int compareTo(Object o1, Object o2)
    {
        return cqlType().compare(o1, o2);
    }
}
