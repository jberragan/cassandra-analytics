package org.apache.cassandra.spark.data.converter.types.complex;

import java.util.stream.Collector;
import java.util.stream.Collectors;

import org.apache.cassandra.spark.data.CqlField;

public class Set extends List
{
    public Set(CqlField.CqlSet type)
    {
        super(type);
    }

    @Override
    public Collector collector()
    {
        return Collectors.toSet();
    }
}
