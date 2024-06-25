package org.apache.cassandra.spark.data.converter.types;

import java.util.Comparator;

import org.apache.spark.sql.types.Decimal;

public interface DecimalTraits extends SparkType
{
    Comparator<org.apache.spark.sql.types.Decimal> DECIMAL_COMPARATOR = Comparator.naturalOrder();
//    Comparator<Decimal> DECIMAL_COMPARATOR = Comparator.naturalOrder();

    @Override
    default int compareTo(Object o1, Object o2)
    {
        return DECIMAL_COMPARATOR.compare((org.apache.spark.sql.types.Decimal) o1, (org.apache.spark.sql.types.Decimal) o2);
    }
}
