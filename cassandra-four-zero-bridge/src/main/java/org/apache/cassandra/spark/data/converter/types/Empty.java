package org.apache.cassandra.spark.data.converter.types;

import org.apache.cassandra.bridge.BigNumberConfig;
import org.apache.cassandra.spark.data.CqlField;
import org.apache.spark.sql.Row;
import org.apache.spark.sql.catalyst.expressions.GenericInternalRow;
import org.apache.spark.sql.types.DataType;
import org.apache.spark.sql.types.DataTypes;

public class Empty implements SparkType
{
    public static Empty INSTANCE = new Empty();

    @Override
    public DataType dataType(BigNumberConfig bigNumberConfig)
    {
        return DataTypes.NullType;
    }

    @Override
    public Object nativeSparkSqlRowValue(final GenericInternalRow row, final int pos)
    {
        return null;
    }

    @Override
    public Object nativeSparkSqlRowValue(Row row, int pos)
    {
        return null;
    }

    public CqlField.CqlType cqlType()
    {
        return org.apache.cassandra.spark.data.types.Empty.INSTANCE;
    }
}
