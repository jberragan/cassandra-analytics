package org.apache.cassandra.spark.data.converter.types;

import java.util.Comparator;

import org.apache.cassandra.bridge.BigNumberConfig;
import org.apache.spark.sql.Row;
import org.apache.spark.sql.catalyst.expressions.GenericInternalRow;
import org.apache.spark.sql.types.DataType;
import org.apache.spark.sql.types.DataTypes;

public interface LongTraits extends SparkType
{
    Comparator<Long> LONG_COMPARATOR = Long::compareTo;

    @Override
    default DataType dataType(BigNumberConfig bigNumberConfig)
    {
        return DataTypes.LongType;
    }

    @Override
    default Object nativeSparkSqlRowValue(final GenericInternalRow row, final int pos)
    {
        return row.getLong(pos);
    }

    @Override
    default Object nativeSparkSqlRowValue(Row row, int pos)
    {
        return row.getLong(pos);
    }

    @Override
    default int compareTo(Object o1, Object o2)
    {
        return LONG_COMPARATOR.compare((Long) o1, (Long) o2);
    }
}
