package org.apache.cassandra.spark.data.converter.types;

import org.apache.cassandra.bridge.BigNumberConfig;
import org.apache.cassandra.spark.data.CqlField;
import org.apache.spark.sql.Row;
import org.apache.spark.sql.catalyst.expressions.GenericInternalRow;
import org.apache.spark.sql.types.DataType;
import org.apache.spark.sql.types.DataTypes;

public class Float implements SparkType
{
    public static final Float INSTANCE = new Float();

    @Override
    public DataType dataType(BigNumberConfig bigNumberConfig)
    {
        return DataTypes.FloatType;
    }

    @Override
    public Object nativeSparkSqlRowValue(final GenericInternalRow row, final int pos)
    {
        return row.getFloat(pos);
    }

    @Override
    public Object nativeSparkSqlRowValue(Row row, int pos)
    {
        return row.getFloat(pos);
    }

    public CqlField.CqlType cqlType()
    {
        return org.apache.cassandra.spark.data.types.Float.INSTANCE;
    }
}
