package org.apache.cassandra.spark.utils;

import java.nio.ByteBuffer;
import java.util.Arrays;

import static org.apache.cassandra.spark.utils.ByteBufferUtils.readBytesWithShortLength;
import static org.apache.cassandra.spark.utils.ByteBufferUtils.readStatic;

public class BaseColumnTypes
{
    // Extract component position from buffer; return null if there are not enough components
    public static ByteBuffer extractComponent(ByteBuffer buffer, int position)
    {
        buffer = buffer.duplicate();
        readStatic(buffer);
        int index = 0;
        while (buffer.remaining() > 0)
        {
            ByteBuffer c = readBytesWithShortLength(buffer);
            if (index == position)
            {
                return c;
            }

            buffer.get();  // Skip end-of-component
            ++index;
        }
        return null;
    }

    public static ByteBuffer[] split(ByteBuffer name, int numKeys)
    {
        // Assume all components, we'll trunk the array afterwards if need be, but most names will be complete
        ByteBuffer[] l = new ByteBuffer[numKeys];
        ByteBuffer buffer = name.duplicate();
        readStatic(buffer);
        int index = 0;
        while (buffer.remaining() > 0)
        {
            l[index++] = readBytesWithShortLength(buffer);
            buffer.get();  // Skip end-of-component
        }
        return index == l.length ? l : Arrays.copyOfRange(l, 0, index);
    }
}
