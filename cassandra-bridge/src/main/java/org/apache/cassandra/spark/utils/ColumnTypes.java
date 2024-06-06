/*
 * Licensed to the Apache Software Foundation (ASF) under one
 * or more contributor license agreements.  See the NOTICE file
 * distributed with this work for additional information
 * regarding copyright ownership.  The ASF licenses this file
 * to you under the Apache License, Version 2.0 (the
 * "License"); you may not use this file except in compliance
 * with the License.  You may obtain a copy of the License at
 *
 *   http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing,
 * software distributed under the License is distributed on an
 * "AS IS" BASIS, WITHOUT WARRANTIES OR CONDITIONS OF ANY
 * KIND, either express or implied.  See the License for the
 * specific language governing permissions and limitations
 * under the License.
 */

package org.apache.cassandra.spark.utils;

import java.nio.ByteBuffer;
import java.nio.charset.CharacterCodingException;
import java.nio.charset.CharsetDecoder;
import java.nio.charset.StandardCharsets;
import java.util.List;

import io.netty.util.concurrent.FastThreadLocal;
import org.apache.cassandra.spark.data.CqlField;

public final class ColumnTypes extends BaseColumnTypes
{

    private ColumnTypes()
    {
        throw new IllegalStateException(getClass() + " is static utility class and shall not be instantiated");
    }

    private final static FastThreadLocal<CharsetDecoder> UTF8_DECODER = new FastThreadLocal<>()
    {
        @Override
        protected CharsetDecoder initialValue()
        {
            return StandardCharsets.UTF_8.newDecoder();
        }
    };

    public static String string(ByteBuffer buffer) throws CharacterCodingException
    {
        return ByteBufferUtils.string(buffer, ColumnTypes.UTF8_DECODER::get);
    }

    public static String stringThrowRuntime(ByteBuffer buffer)
    {
        try
        {
            return string(buffer);
        }
        catch (final CharacterCodingException e)
        {
            throw new RuntimeException(e);
        }
    }

    public static ByteBuffer buildPartitionKey(List<CqlField> partitionKeys, Object... values)
    {
        if (partitionKeys.size() == 1)
        {
            // Only 1 partition key
            CqlField key = partitionKeys.get(0);
            return key.serialize(values[key.position()]);
        }
        else
        {
            // Composite partition key
            ByteBuffer[] buffers = partitionKeys.stream()
                                                .map(key -> key.serialize(values[key.position()]))
                                                .toArray(ByteBuffer[]::new);
            return ColumnTypes.build(false, buffers);
        }
    }

    public static ByteBuffer build(boolean isStatic, ByteBuffer... buffers)
    {
        int totalLength = isStatic ? 2 : 0;
        for (ByteBuffer buffer : buffers)
        {
            // 2 bytes short length + data length + 1 byte for end-of-component marker
            totalLength += 2 + buffer.remaining() + 1;
        }

        ByteBuffer out = ByteBuffer.allocate(totalLength);
        if (isStatic)
        {
            out.putShort((short) ByteBufferUtils.STATIC_MARKER);
        }

        for (ByteBuffer buffer : buffers)
        {
            ByteBufferUtils.writeShortLength(out, buffer.remaining());  // Short length
            out.put(buffer.duplicate());  // Data
            out.put((byte) 0);  // End-of-component marker
        }
        out.flip();
        return out;
    }
}
