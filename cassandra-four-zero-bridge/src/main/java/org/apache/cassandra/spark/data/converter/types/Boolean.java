package org.apache.cassandra.spark.data.converter.types;

import org.apache.cassandra.bridge.BigNumberConfig;
import org.apache.cassandra.spark.data.CqlField;
import org.apache.spark.sql.Row;
import org.apache.spark.sql.catalyst.expressions.GenericInternalRow;
import org.apache.spark.sql.types.DataType;
import org.apache.spark.sql.types.DataTypes;

public class Boolean implements SparkType
{
    public static final Boolean INSTANCE = new Boolean();

    public DataType dataType(BigNumberConfig bigNumberConfig)
    {
        return DataTypes.BooleanType;
    }

    public CqlField.CqlType cqlType()
    {
        return org.apache.cassandra.spark.data.types.Boolean.INSTANCE;
    }

    @Override
    public Object nativeSparkSqlRowValue(final GenericInternalRow row, final int pos)
    {
        return row.getBoolean(pos);
    }

    @Override
    public Object nativeSparkSqlRowValue(Row row, int pos)
    {
        return row.getBoolean(pos);
    }
}
