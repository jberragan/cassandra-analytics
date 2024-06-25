package org.apache.cassandra.spark.data.converter.types;

import java.math.BigDecimal;

import org.apache.cassandra.bridge.BigNumberConfig;
import org.apache.cassandra.spark.data.CqlField;
import org.apache.spark.sql.Row;
import org.apache.spark.sql.catalyst.expressions.GenericInternalRow;
import org.apache.spark.sql.types.DataType;
import org.apache.spark.sql.types.DataTypes;
import org.jetbrains.annotations.NotNull;

public class Decimal implements DecimalTraits
{
    public static Decimal INSTANCE = new Decimal();

    @Override
    public DataType dataType(BigNumberConfig bigNumberConfig)
    {
        return DataTypes.createDecimalType(bigNumberConfig.bigDecimalPrecision(), bigNumberConfig.bigDecimalScale());
    }

    @Override
    public Object toSparkSqlType(@NotNull Object o, boolean isFrozen)
    {
        return org.apache.spark.sql.types.Decimal.apply((BigDecimal) o);
    }

    @Override
    public Object nativeSparkSqlRowValue(final GenericInternalRow row, final int pos)
    {
        return row.getDecimal(pos, BigNumberConfig.DEFAULT.bigIntegerPrecision(), BigNumberConfig.DEFAULT.bigIntegerScale());
    }

    @Override
    public Object nativeSparkSqlRowValue(Row row, int pos)
    {
        return row.getDecimal(pos);
    }

    @Override
    public Object toTestRowType(Object value)
    {
        if (value instanceof BigDecimal)
        {
            return value;
        }
        return ((org.apache.spark.sql.types.Decimal) value).toJavaBigDecimal();
    }

    @Override
    public CqlField.CqlType cqlType()
    {
        return org.apache.cassandra.spark.data.types.Decimal.INSTANCE;
    }
}
