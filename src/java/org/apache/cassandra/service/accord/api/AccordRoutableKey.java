/*
 * Licensed to the Apache Software Foundation (ASF) under one
 * or more contributor license agreements.  See the NOTICE file
 * distributed with this work for additional information
 * regarding copyright ownership.  The ASF licenses this file
 * to you under the Apache License, Version 2.0 (the
 * "License"); you may not use this file except in compliance
 * with the License.  You may obtain a copy of the License at
 *
 *     http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 */

package org.apache.cassandra.service.accord.api;

import java.io.IOException;

import javax.annotation.Nonnull;

import accord.primitives.RoutableKey;
import org.apache.cassandra.dht.Token;
import org.apache.cassandra.io.IVersionedSerializer;
import org.apache.cassandra.io.util.DataInputPlus;
import org.apache.cassandra.io.util.DataOutputPlus;
import org.apache.cassandra.schema.TableId;

public abstract class AccordRoutableKey implements RoutableKey
{
    public interface AccordKeySerializer<K> extends IVersionedSerializer<K>
    {
        void skip(DataInputPlus in, int version) throws IOException;
    }

    public interface AccordSearchableKeySerializer<K> extends AccordKeySerializer<K>
    {
        boolean keysWithSamePrefixAreFixedLength(K key);
        int lengthWithoutPrefix(K key);
        void serializePrefix(Object prefix, DataOutputPlus out, int version) throws IOException;
        void serializeWithoutPrefixOrLength(K key, DataOutputPlus out, int version) throws IOException;
        void skipPrefix(DataInputPlus in, int version) throws IOException;
        void skipKeyWithoutPrefixOrLength(DataInputPlus in, int version) throws IOException;
        Object deserializePrefix(DataInputPlus in, int version) throws IOException;
        K deserializeWithPrefix(Object prefix, DataInputPlus in, int version) throws IOException;
    }

    static final byte NORMAL_SENTINEL = 0x60;
    static final byte BEFORE_TOKEN_SENTINEL = 0x50;
    static final byte MIN_TABLE_SENTINEL = 0x20;
    static final byte MAX_TABLE_SENTINEL = (byte) 0xE0;
    static final int PREFIX_MASK = 0xC0;
    static final int POSTFIX_MASK = 0x3F;

    final TableId table; // TODO (desired): use an id (TrM)

    protected AccordRoutableKey(TableId table)
    {
        this.table = table;
    }

    public TableId table()
    {
        return table;
    }

    public abstract Token token();
    abstract byte sentinel();

    @Override
    public Object prefix()
    {
        return table;
    }

    @Override
    public String toString()
    {
        return prefix() + ":" + suffix();
    }

    @Override
    public int hashCode()
    {
        return table.hashCode() * 31 + token().tokenHash();
    }

    @Override
    public final int compareTo(RoutableKey that)
    {
        return compareTo((AccordRoutableKey) that);
    }

    @Override
    public int compareAsRoutingKey(@Nonnull RoutableKey that)
    {
        return compareAsRoutingKey((AccordRoutableKey) that);
    }

    public final int compareAsRoutingKey(@Nonnull AccordRoutableKey that)
    {
        int thisSentinel = this.sentinel(), thatSentinel = that.sentinel();
        int c = (thisSentinel & PREFIX_MASK) - (thatSentinel & PREFIX_MASK);
        if (c == 0) c = this.token().compareTo(that.token());
        if (c == 0) c = (thisSentinel & POSTFIX_MASK) - (thatSentinel & POSTFIX_MASK);
        return c;
    }

    public final int compareTo(AccordRoutableKey that)
    {
        int c = compareAsRoutingKey(that);
        if (c != 0)
            return c;

        boolean thisIsRoutingKey = this.getClass() == TokenKey.class;
        boolean thatIsRoutingKey = that.getClass() == TokenKey.class;
        if (thisIsRoutingKey | thatIsRoutingKey)
        {
            if (thisIsRoutingKey & thatIsRoutingKey)
                return 0;

            return thisIsRoutingKey ? 1 : -1;
        }

        return ((PartitionKey)this).key.compareBytesOnly(((PartitionKey)that).key);
    }

    @Override
    public final boolean equals(Object o)
    {
        if (this == o) return true;
        if (o == null || getClass() != o.getClass()) return false;
        AccordRoutableKey that = (AccordRoutableKey) o;
        return compareTo(that) == 0;
    }
}
