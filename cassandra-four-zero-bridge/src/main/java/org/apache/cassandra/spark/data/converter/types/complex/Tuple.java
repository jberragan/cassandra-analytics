package org.apache.cassandra.spark.data.converter.types.complex;

import java.util.stream.IntStream;

import org.apache.cassandra.bridge.BigNumberConfig;
import org.apache.cassandra.spark.data.CqlField;
import org.apache.spark.sql.Row;
import org.apache.spark.sql.catalyst.InternalRow;
import org.apache.spark.sql.catalyst.expressions.GenericInternalRow;
import org.apache.spark.sql.catalyst.expressions.GenericRowWithSchema;
import org.apache.spark.sql.types.DataType;
import org.apache.spark.sql.types.DataTypes;
import org.apache.spark.sql.types.StructField;
import org.jetbrains.annotations.NotNull;

public class Tuple implements CollectionTrait
{
    private final CqlField.CqlTuple tuple;

    public Tuple(CqlField.CqlTuple tuple)
    {
        this.tuple = tuple;
    }

    @Override
    public DataType dataType(BigNumberConfig bigNumberConfig)
    {
        return DataTypes.createStructType(
        IntStream.range(0, size())
                 .mapToObj(i -> DataTypes.createStructField(Integer.toString(i), sparkType(i).dataType(bigNumberConfig), true))
                 .toArray(StructField[]::new)
        );
    }

    public CqlField.CqlCollection collection()
    {
        return tuple;
    }

    public Object toSparkSqlType(@NotNull Object o, boolean isFrozen)
    {
        return new GenericInternalRow((Object[]) o);
    }

    @Override
    public Object sparkSqlRowValue(GenericInternalRow row, int pos)
    {
        final InternalRow tupleStruct = row.getStruct(pos, size());
        return IntStream.range(0, size()).boxed()
                        .map(i -> sparkType(i).toTestRowType(tupleStruct.get(i, sparkType(i).dataType(BigNumberConfig.DEFAULT)))) //FIXME: pass in BigNumberConfig
                        .toArray();
    }

    @Override
    public Object sparkSqlRowValue(Row row, int pos)
    {
        final Row tupleStruct = row.getStruct(pos);
        return IntStream.range(0, tupleStruct.size()).boxed()
                        .filter(i -> !tupleStruct.isNullAt(i))
                        .map(i -> sparkType(i).toTestRowType(tupleStruct.get(i)))
                        .toArray();
    }

    @Override
    public Object toTestRowType(Object value)
    {
        final GenericRowWithSchema tupleRow = (GenericRowWithSchema) value;
        final Object[] tupleResult = new Object[tupleRow.size()];
        for (int i = 0; i < tupleRow.size(); i++)
        {
            tupleResult[i] = sparkType(i).toTestRowType(tupleRow.get(i));
        }
        return tupleResult;
    }

    @Override
    public boolean equals(Object o1, Object o2)
    {
        return CollectionTrait.equalsArrays(((GenericInternalRow) o1).values(), ((GenericInternalRow) o2).values(), this::sparkType);
    }

    @Override
    public int compare(Object o1, Object o2)
    {
        return CollectionTrait.compareArrays(((GenericInternalRow) o1).values(), ((GenericInternalRow) o2).values(), this::sparkType);
    }
}
