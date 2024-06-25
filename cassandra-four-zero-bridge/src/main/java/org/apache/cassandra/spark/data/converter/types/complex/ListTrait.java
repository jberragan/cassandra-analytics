package org.apache.cassandra.spark.data.converter.types.complex;

import java.util.Arrays;
import java.util.Collection;
import java.util.stream.Collector;
import java.util.stream.Collectors;
import java.util.stream.Stream;

import org.apache.cassandra.bridge.BigNumberConfig;
import org.apache.spark.sql.Row;
import org.apache.spark.sql.catalyst.expressions.GenericInternalRow;
import org.apache.spark.sql.catalyst.util.ArrayData;
import org.apache.spark.sql.catalyst.util.GenericArrayData;
import org.apache.spark.sql.types.DataType;
import org.apache.spark.sql.types.DataTypes;
import org.jetbrains.annotations.NotNull;
import scala.collection.mutable.WrappedArray;

public interface ListTrait extends CollectionTrait, ComplexTrait
{
    @Override
    default DataType dataType(BigNumberConfig bigNumberConfig)
    {
        return DataTypes.createArrayType(sparkType().dataType(bigNumberConfig));
    }

    @SuppressWarnings("unchecked")
    @Override
    default Object toSparkSqlType(@NotNull Object o, boolean isFrozen)
    {
        return ArrayData.toArrayData(((Collection<Object>) o)
                                     .stream()
                                     .map(a -> sparkType().toSparkSqlType(a, isFrozen)).toArray());
    }

    @Override
    default Object sparkSqlRowValue(GenericInternalRow row, int pos)
    {
        return Arrays.stream(row.getArray(pos).array())
                     .map(o -> sparkType().toTestRowType(o))
                     .collect(collector());
    }

    @Override
    default Object sparkSqlRowValue(Row row, int pos)
    {
        return row.getList(pos).stream()
                  .map(o -> sparkType().toTestRowType(o))
                  .collect(collector());
    }

    @SuppressWarnings({ "unchecked", "RedundantCast" }) // redundant cast to (Object[]) is required
    @Override
    default Object toTestRowType(Object value)
    {
        return Stream.of((Object[]) ((WrappedArray<Object>) value).array())
                     .map(v -> sparkType().toTestRowType(v))
                     .collect(Collectors.toList());
    }

    @SuppressWarnings("rawtypes")
    default Collector collector()
    {
        return Collectors.toList();
    }

    @Override
    default boolean equals(Object o1, Object o2)
    {
        return CollectionTrait.equalsArrays(((GenericArrayData) o1).array(), ((GenericArrayData) o2).array(), (pos) -> sparkType());
    }

    @Override
    default int compare(Object o1, Object o2)
    {
        return CollectionTrait.compareArrays(((GenericArrayData) o1).array(), ((GenericArrayData) o2).array(), (pos) -> sparkType());
    }
}
