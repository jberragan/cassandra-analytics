package org.apache.cassandra.spark.data.converter.types;

import org.apache.cassandra.bridge.BigNumberConfig;
import org.apache.spark.sql.Row;
import org.apache.spark.sql.catalyst.expressions.GenericInternalRow;
import org.apache.spark.sql.types.DataType;
import org.apache.spark.sql.types.DataTypes;

public interface IntTrait extends SparkType
{
    @Override
    default DataType dataType(BigNumberConfig bigNumberConfig)
    {
        return DataTypes.IntegerType;
    }

    @Override
    default Object nativeSparkSqlRowValue(final GenericInternalRow row, final int pos)
    {
        return row.getInt(pos);
    }

    @Override
    default Object nativeSparkSqlRowValue(Row row, int pos)
    {
        return row.getInt(pos);
    }
}
