package org.apache.cassandra.spark.data;

import java.util.Map;
import java.util.function.Function;
import java.util.stream.Collectors;

import com.esotericsoftware.kryo.io.Input;
import org.apache.cassandra.cql3.ColumnIdentifier;
import org.apache.cassandra.spark.data.types.Ascii;
import org.apache.cassandra.spark.data.types.BigInt;
import org.apache.cassandra.spark.data.types.Blob;
import org.apache.cassandra.spark.data.types.Boolean;
import org.apache.cassandra.spark.data.types.Counter;
import org.apache.cassandra.spark.data.types.Date;
import org.apache.cassandra.spark.data.types.Decimal;
import org.apache.cassandra.spark.data.types.Double;
import org.apache.cassandra.spark.data.types.Duration;
import org.apache.cassandra.spark.data.types.Empty;
import org.apache.cassandra.spark.data.types.Float;
import org.apache.cassandra.spark.data.types.Inet;
import org.apache.cassandra.spark.data.types.Int;
import org.apache.cassandra.spark.data.types.SmallInt;
import org.apache.cassandra.spark.data.types.Text;
import org.apache.cassandra.spark.data.types.Time;
import org.apache.cassandra.spark.data.types.TimeUUID;
import org.apache.cassandra.spark.data.types.Timestamp;
import org.apache.cassandra.spark.data.types.TinyInt;
import org.apache.cassandra.spark.data.types.VarChar;
import org.apache.cassandra.spark.data.types.VarInt;
import org.apache.cassandra.spark.data.types.complex.CqlCollection;
import org.apache.cassandra.spark.data.types.complex.CqlFrozen;
import org.apache.cassandra.spark.data.types.complex.CqlList;
import org.apache.cassandra.spark.data.types.complex.CqlMap;
import org.apache.cassandra.spark.data.types.complex.CqlSet;
import org.apache.cassandra.spark.data.types.complex.CqlTuple;
import org.apache.cassandra.spark.data.types.complex.CqlUdt;

public class CassandraTypesImplementation extends CassandraTypes
{

    public static final CassandraTypesImplementation INSTANCE = new CassandraTypesImplementation();

    private final Map<String, CqlField.NativeType> nativeTypes;

    public CassandraTypesImplementation()
    {
        nativeTypes = allTypes().stream().collect(Collectors.toMap(CqlField.CqlType::name, Function.identity()));
    }

    @Override
    public String maybeQuoteIdentifier(String identifier)
    {
        if (isAlreadyQuoted(identifier))
        {
            return identifier;
        }
        return ColumnIdentifier.maybeQuote(identifier);
    }

    @Override
    public Map<String, ? extends CqlField.NativeType> nativeTypeNames()
    {
        return nativeTypes;
    }

    @Override
    public CqlField.CqlType readType(CqlField.CqlType.InternalType type, Input input)
    {
        switch (type)
        {
            case NativeCql:
                return nativeType(input.readString());
            case Set:
            case List:
            case Map:
            case Tuple:
                return CqlCollection.read(type, input, this);
            case Frozen:
                return CqlFrozen.build(CqlField.CqlType.read(input, this));
            case Udt:
                return CqlUdt.read(input, this);
            default:
                throw new IllegalStateException("Unknown CQL type, cannot deserialize");
        }
    }

    @Override
    public Ascii ascii()
    {
        return Ascii.INSTANCE;
    }

    @Override
    public Blob blob()
    {
        return Blob.INSTANCE;
    }

    @Override
    public Boolean bool()
    {
        return Boolean.INSTANCE;
    }

    @Override
    public Counter counter()
    {
        return Counter.INSTANCE;
    }

    @Override
    public BigInt bigint()
    {
        return BigInt.INSTANCE;
    }

    @Override
    public Date date()
    {
        return Date.INSTANCE;
    }

    @Override
    public Decimal decimal()
    {
        return Decimal.INSTANCE;
    }

    @Override
    public Double aDouble()
    {
        return Double.INSTANCE;
    }

    @Override
    public Duration duration()
    {
        return Duration.INSTANCE;
    }

    @Override
    public Empty empty()
    {
        return Empty.INSTANCE;
    }

    @Override
    public Float aFloat()
    {
        return Float.INSTANCE;
    }

    @Override
    public Inet inet()
    {
        return Inet.INSTANCE;
    }

    @Override
    public Int aInt()
    {
        return Int.INSTANCE;
    }

    @Override
    public SmallInt smallint()
    {
        return SmallInt.INSTANCE;
    }

    @Override
    public Text text()
    {
        return Text.INSTANCE;
    }

    @Override
    public Time time()
    {
        return Time.INSTANCE;
    }

    @Override
    public Timestamp timestamp()
    {
        return Timestamp.INSTANCE;
    }

    @Override
    public TimeUUID timeuuid()
    {
        return TimeUUID.INSTANCE;
    }

    @Override
    public TinyInt tinyint()
    {
        return TinyInt.INSTANCE;
    }

    @Override
    public org.apache.cassandra.spark.data.types.UUID uuid()
    {
        return org.apache.cassandra.spark.data.types.UUID.INSTANCE;
    }

    @Override
    public VarChar varchar()
    {
        return VarChar.INSTANCE;
    }

    @Override
    public VarInt varint()
    {
        return VarInt.INSTANCE;
    }

    @Override
    public CqlField.CqlType collection(String name, CqlField.CqlType... types)
    {
        return CqlCollection.build(name, types);
    }

    @Override
    public CqlList list(CqlField.CqlType type)
    {
        return CqlCollection.list(type);
    }

    @Override
    public CqlSet set(CqlField.CqlType type)
    {
        return CqlCollection.set(type);
    }

    @Override
    public CqlMap map(CqlField.CqlType keyType, CqlField.CqlType valueType)
    {
        return CqlCollection.map(keyType, valueType);
    }

    @Override
    public CqlTuple tuple(CqlField.CqlType... types)
    {
        return CqlCollection.tuple(types);
    }

    @Override
    public CqlField.CqlType frozen(CqlField.CqlType type)
    {
        return CqlFrozen.build(type);
    }

    @Override
    public CqlField.CqlUdtBuilder udt(String keyspace, String name)
    {
        return CqlUdt.builder(keyspace, name);
    }
}
