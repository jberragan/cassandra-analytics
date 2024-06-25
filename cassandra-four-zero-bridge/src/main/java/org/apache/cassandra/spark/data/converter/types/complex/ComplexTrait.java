package org.apache.cassandra.spark.data.converter.types.complex;

import org.apache.cassandra.spark.data.converter.types.SparkType;
import org.apache.spark.sql.Row;
import org.apache.spark.sql.catalyst.expressions.GenericInternalRow;

public interface ComplexTrait extends SparkType
{
    @Override
    default Object nativeSparkSqlRowValue(GenericInternalRow row, int pos)
    {
        throw new IllegalStateException("nativeSparkSqlRowValue should not be called for collection");
    }

    @Override
    default Object nativeSparkSqlRowValue(Row row, int pos)
    {
        throw new IllegalStateException("nativeSparkSqlRowValue should not be called for collection");
    }
}
