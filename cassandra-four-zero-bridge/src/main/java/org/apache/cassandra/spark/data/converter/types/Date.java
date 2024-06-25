package org.apache.cassandra.spark.data.converter.types;

import org.apache.cassandra.bridge.BigNumberConfig;
import org.apache.cassandra.spark.data.CqlField;
import org.apache.spark.sql.Row;
import org.apache.spark.sql.types.DataType;
import org.apache.spark.sql.types.DataTypes;
import org.jetbrains.annotations.NotNull;

public class Date implements SparkType
{
    public static final Date INSTANCE = new Date();

    public DataType dataType(BigNumberConfig bigNumberConfig)
    {
        return DataTypes.DateType;
    }

    public Object toSparkSqlType(@NotNull Object o, boolean isFrozen)
    {
        // SparkSQL date type is an int incrementing from day 0 on 1970-01-01
        // Cassandra stores date as "days since 1970-01-01 plus Integer.MIN_VALUE"
        final int days = (Integer) o;
        return days - Integer.MIN_VALUE;
    }

    public Object nativeSparkSqlRowValue(Row row, int pos)
    {
        return row.getDate(pos);
    }

    public CqlField.CqlType cqlType()
    {
        return org.apache.cassandra.spark.data.types.Date.INSTANCE;
    }
}
