package org.apache.cassandra.spark.data.converter.types.complex;

import java.util.HashMap;
import java.util.stream.Collectors;

import org.apache.cassandra.bridge.BigNumberConfig;
import org.apache.cassandra.spark.data.CqlField;
import org.apache.spark.sql.Row;
import org.apache.spark.sql.catalyst.expressions.GenericInternalRow;
import org.apache.spark.sql.catalyst.util.ArrayBasedMapData;
import org.apache.spark.sql.catalyst.util.ArrayData;
import org.apache.spark.sql.catalyst.util.MapData;
import org.apache.spark.sql.types.DataType;
import org.apache.spark.sql.types.DataTypes;
import org.jetbrains.annotations.NotNull;
import scala.collection.JavaConverters;

public class Map implements MapTrait
{
    private final CqlField.CqlMap map;

    public Map(CqlField.CqlMap map)
    {
        this.map = map;
    }

    public DataType dataType(BigNumberConfig bigNumberConfig)
    {
        return DataTypes.createMapType(keyType().dataType(bigNumberConfig), valueType().dataType(bigNumberConfig));
    }

    @SuppressWarnings("unchecked")
    @Override
    public Object toSparkSqlType(@NotNull Object o, boolean isFrozen)
    {
        return mapToSparkSqlType((java.util.Map<Object, Object>) o, isFrozen);
    }

    private ArrayBasedMapData mapToSparkSqlType(final java.util.Map<Object, Object> map, boolean isFrozen)
    {
        final Object[] keys = new Object[map.size()];
        final Object[] values = new Object[map.size()];
        int pos = 0;
        for (final java.util.Map.Entry<Object, Object> entry : map.entrySet())
        {
            keys[pos] = keyType().toSparkSqlType(entry.getKey(), isFrozen);
            values[pos] = valueType().toSparkSqlType(entry.getValue(), isFrozen);
            pos++;
        }
        return new ArrayBasedMapData(ArrayData.toArrayData(keys), ArrayData.toArrayData(values));
    }

    @Override
    public Object sparkSqlRowValue(GenericInternalRow row, int pos)
    {
        final MapData map = row.getMap(pos);
        final ArrayData keys = map.keyArray();
        final ArrayData values = map.valueArray();
        final java.util.Map<Object, Object> result = new HashMap<>(keys.numElements());
        for (int i = 0; i < keys.numElements(); i++)
        {
            final Object key = keyType().toTestRowType(keys.get(i, keyType().dataType(BigNumberConfig.DEFAULT))); //FIXME: pass in BigNumberConfig?
            final Object value = valueType().toTestRowType(values.get(i, valueType().dataType(BigNumberConfig.DEFAULT)));
            result.put(key, value);
        }
        return result;
    }

    @Override
    public Object sparkSqlRowValue(Row row, int pos)
    {
        return row.getJavaMap(pos).entrySet().stream()
                  .collect(Collectors.toMap(
                  e -> keyType().toTestRowType(e.getKey()),
                  e -> valueType().toTestRowType(e.getValue())
                  ));
    }

    @SuppressWarnings("unchecked")
    @Override
    public Object toTestRowType(Object value)
    {
        return ((java.util.Map<Object, Object>) JavaConverters.mapAsJavaMapConverter(((scala.collection.immutable.Map<?, ?>) value)).asJava())
               .entrySet().stream()
               .collect(Collectors.toMap(
                        e -> keyType().toTestRowType(e.getKey()),
                        e -> valueType().toTestRowType(e.getValue()))
               );
    }

    public CqlField.CqlCollection collection()
    {
        return map;
    }

    @Override
    public boolean equals(Object o1, Object o2)
    {
        return CollectionTrait.equalsArrays(((MapData) o1).valueArray().array(), ((MapData) o2).valueArray().array(), (pos) -> valueType());
    }

    @Override
    public int compare(Object o1, Object o2)
    {
        return CollectionTrait.compareArrays(((MapData) o1).valueArray().array(), ((MapData) o2).valueArray().array(), (pos) -> valueType());
    }
}
