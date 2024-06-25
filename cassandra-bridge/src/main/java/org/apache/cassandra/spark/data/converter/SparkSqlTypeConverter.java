package org.apache.cassandra.spark.data.converter;

import org.apache.cassandra.bridge.BigNumberConfig;
import org.apache.cassandra.spark.data.CqlField;
import org.apache.cassandra.spark.data.TypeConverter;
import org.apache.spark.sql.Row;
import org.apache.spark.sql.catalyst.expressions.GenericInternalRow;
import org.apache.spark.sql.types.DataType;

public interface SparkSqlTypeConverter extends TypeConverter
{
    Object nativeSparkSqlRowValue(CqlField.CqlType cqlType, GenericInternalRow row, int position);

    Object nativeSparkSqlRowValue(CqlField.CqlType cqlType, Row row, int position);

    Object sparkSqlRowValue(CqlField.CqlType cqlType, GenericInternalRow row, int position);

    Object sparkSqlRowValue(CqlField.CqlType cqlType, Row row, int position);


    default DataType sparkSqlType(CqlField.CqlType cqlType)
    {
        return sparkSqlType(cqlType, BigNumberConfig.DEFAULT);
    }

    DataType sparkSqlType(CqlField.CqlType cqlType, BigNumberConfig bigNumberConfig);
}
