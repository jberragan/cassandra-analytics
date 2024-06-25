package org.apache.cassandra.spark.data.converter.types.complex;

import java.util.List;
import java.util.stream.Collectors;
import java.util.stream.IntStream;

import org.apache.cassandra.bridge.BigNumberConfig;
import org.apache.cassandra.spark.data.CqlField;
import org.apache.cassandra.spark.data.converter.SparkSqlTypeConverterImplementation;
import org.apache.cassandra.spark.data.converter.types.SparkType;
import org.apache.spark.sql.Row;
import org.apache.spark.sql.catalyst.InternalRow;
import org.apache.spark.sql.catalyst.expressions.GenericInternalRow;
import org.apache.spark.sql.types.DataType;
import org.apache.spark.sql.types.DataTypes;
import org.apache.spark.sql.types.StructField;

public class Udt implements ComplexTrait
{
    private final CqlField.CqlUdt udt;

    public Udt(CqlField.CqlUdt udt)
    {
        this.udt = udt;
    }

    public CqlField field(int position)
    {
        return this.udt.field(position);
    }

    @Override
    public DataType dataType(BigNumberConfig bigNumberConfig)
    {
        return DataTypes.createStructType(
        udt.fields().stream()
           .map(f -> DataTypes.createStructField(f.name(), SparkSqlTypeConverterImplementation.INSTANCE.sparkSqlType(f.type(), bigNumberConfig), true))
           .toArray(StructField[]::new)
        );
    }

    @Override
    public Object sparkSqlRowValue(GenericInternalRow row, int pos)
    {
        final InternalRow struct = row.getStruct(pos, size());
        return IntStream.range(0, size()).boxed()
                        .collect(Collectors.toMap(
                        i -> field(i).name(),
                        i -> sparkType(i).toTestRowType(struct.get(i, sparkType(i).dataType(BigNumberConfig.DEFAULT)) //FIXME: pass in BigNumberConfig
                        )));
    }

    @Override
    public Object sparkSqlRowValue(Row row, int pos)
    {
        final Row struct = row.getStruct(pos);
        return IntStream.range(0, struct.size()).boxed()
                        .filter(i -> !struct.isNullAt(i))
                        .collect(Collectors.toMap(
                        i -> struct.schema().fields()[i].name(),
                        i -> field(i).type().toTestRowType(struct.get(i))
                        ));
    }

    public List<SparkType> sparkTypes()
    {
        return udt
               .fields().stream()
               .map(CqlField::type)
               .map(SparkSqlTypeConverterImplementation::toSparkType)
               .collect(Collectors.toList());
    }

    public int size()
    {
        return udt.fields().size();
    }

    public SparkType sparkType()
    {
        return sparkType(0);
    }

    public SparkType sparkType(int position)
    {
        return SparkSqlTypeConverterImplementation.toSparkType(field(position).type());
    }

    @Override
    public CqlField.CqlType cqlType()
    {
        return udt;
    }
}
