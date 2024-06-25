package org.apache.cassandra.spark.data.converter.types.complex;

import java.util.List;
import java.util.function.Function;
import java.util.stream.Collectors;

import org.apache.cassandra.spark.data.CqlField;
import org.apache.cassandra.spark.data.converter.SparkSqlTypeConverterImplementation;
import org.apache.cassandra.spark.data.converter.types.SparkType;

public interface CollectionTrait extends SparkType
{
    default List<SparkType> sparkTypes()
    {
        return collection()
               .types().stream()
               .map(SparkSqlTypeConverterImplementation::toSparkType)
               .collect(Collectors.toList());
    }

    CqlField.CqlCollection collection();

    default int size()
    {
        return collection().types().size();
    }

    default SparkType sparkType()
    {
        return sparkType(0);
    }

    default SparkType sparkType(int position)
    {
        return SparkSqlTypeConverterImplementation.toSparkType(collection().type(position));
    }

    default CqlField.CqlType cqlType()
    {
        return collection();
    }

    static boolean equalsArrays(final Object[] lhs, final Object[] rhs, final Function<Integer, SparkType> types)
    {
        for (int pos = 0; pos < Math.min(lhs.length, rhs.length); pos++)
        {
            if (!types.apply(pos).equals(lhs[pos], rhs[pos]))
            {
                return false;
            }
        }
        return lhs.length == rhs.length;
    }

    static int compareArrays(final Object[] lhs, final Object[] rhs, final Function<Integer, SparkType> types)
    {
        for (int pos = 0; pos < Math.min(lhs.length, rhs.length); pos++)
        {
            final int c = types.apply(pos).compare(lhs[pos], rhs[pos]);
            if (c != 0)
            {
                return c;
            }
        }
        return Integer.compare(lhs.length, rhs.length);
    }
}
