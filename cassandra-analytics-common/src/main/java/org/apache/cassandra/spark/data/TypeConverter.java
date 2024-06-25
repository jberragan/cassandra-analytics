package org.apache.cassandra.spark.data;

import org.jetbrains.annotations.NotNull;

public interface TypeConverter
{
    /**
     * Converts Cassandra native value to desried equivalent.
     * E.g. SparkSQL uses `org.apache.spark.unsafe.types.UTF8String` to wrap strings.
     * E.g. SparkSQL starts counting dates from 1970-01-01 = 0, but Cassandra starts at 1970-01-01 = Integer.MIN_VALUE.
     *
     * @param value cassandra value
     * @return equivalent value in new data format.
     */
    Object convert(CqlField.CqlType cqlType, @NotNull Object value);
}
