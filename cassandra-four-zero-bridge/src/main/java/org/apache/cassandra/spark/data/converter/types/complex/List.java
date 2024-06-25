package org.apache.cassandra.spark.data.converter.types.complex;

import org.apache.cassandra.spark.data.CqlField;

public class List implements ListTrait
{
    final CqlField.CqlCollection list;

    public List(CqlField.CqlCollection list)
    {
        this.list = list;
    }

    public CqlField.CqlCollection collection()
    {
        return list;
    }
}
