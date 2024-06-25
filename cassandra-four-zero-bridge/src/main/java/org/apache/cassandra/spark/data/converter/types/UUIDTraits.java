package org.apache.cassandra.spark.data.converter.types;

public interface UUIDTraits extends StringTraits
{
    @Override
    default Object toTestRowType(Object value)
    {
        return java.util.UUID.fromString(value.toString());
    }
}
