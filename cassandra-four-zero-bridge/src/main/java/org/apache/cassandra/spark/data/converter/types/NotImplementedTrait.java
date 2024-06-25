package org.apache.cassandra.spark.data.converter.types;

import org.apache.cassandra.bridge.BigNumberConfig;
import org.apache.cassandra.spark.data.CqlField;
import org.apache.spark.sql.Row;
import org.apache.spark.sql.catalyst.expressions.GenericInternalRow;
import org.apache.spark.sql.types.DataType;

public interface NotImplementedTrait extends SparkType
{
    @Override
    default DataType dataType(BigNumberConfig bigNumberConfig)
    {
        throw CqlField.notImplemented(cqlType());
    }

    @Override
    default Object nativeSparkSqlRowValue(GenericInternalRow row, int pos)
    {
        throw CqlField.notImplemented(cqlType());
    }

    @Override
    default Object nativeSparkSqlRowValue(Row row, int pos)
    {
        return row.isNullAt(pos) ? null : toTestRowType(nativeSparkSqlRowValue(row, pos));
    }
}
