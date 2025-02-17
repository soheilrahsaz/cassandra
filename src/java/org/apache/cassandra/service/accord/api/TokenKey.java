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
import java.math.BigInteger;
import java.nio.ByteBuffer;
import java.util.ArrayList;
import java.util.List;
import java.util.Map;
import java.util.TreeMap;

import accord.api.RoutingKey;
import accord.local.ShardDistributor;
import accord.primitives.Range;
import accord.primitives.RangeFactory;
import accord.primitives.Ranges;
import accord.utils.Invariants;
import org.apache.cassandra.db.marshal.ByteBufferAccessor;
import org.apache.cassandra.dht.IPartitioner;
import org.apache.cassandra.dht.Token;
import org.apache.cassandra.io.util.DataInputPlus;
import org.apache.cassandra.io.util.DataOutputPlus;
import org.apache.cassandra.schema.TableId;
import org.apache.cassandra.service.accord.TokenRange;
import org.apache.cassandra.utils.ObjectSizes;
import org.apache.cassandra.utils.bytecomparable.ByteSource;

import static org.apache.cassandra.config.DatabaseDescriptor.getPartitioner;

public final class TokenKey extends AccordRoutableKey implements RoutingKey, RangeFactory
{
    public enum RoutingKeyKind
    {
        TOKEN, SENTINEL, MIN_TOKEN
    }

    private static final long EMPTY_SIZE = ObjectSizes.measure(new TokenKey(null, null));

    @Override
    public Range asRange()
    {
        return TokenRange.create(before(), this);
    }

    // we use the first 2 bits as a prefix, and the last 6 bits as a postfix comparison
    final byte sentinel;
    final Token token;
    public TokenKey(TableId tableId, byte sentinel, Token token)
    {
        super(tableId);
        this.sentinel = sentinel;
        this.token = token;
    }

    public TokenKey(TableId tableId, Token token)
    {
        this(tableId, NORMAL_SENTINEL, token);
    }

    public TokenKey withToken(Token token)
    {
        return new TokenKey(table, sentinel, token);
    }

    @Override
    public Token token()
    {
        return token;
    }

    @Override
    public byte sentinel()
    {
        return sentinel;
    }

    public ByteSource prefixSentinel()
    {
        return ByteSource.oneByte(sentinel & PREFIX_MASK);
    }

    public ByteSource suffixSentinel()
    {
        return ByteSource.oneByte(sentinel & POSTFIX_MASK);
    }

    // this can be invoked to a depth of 5 from a real token
    TokenKey before()
    {
        int lowestBit = Integer.lowestOneBit(sentinel);
        Invariants.require(lowestBit != 1);
        byte newSentinel = (byte)((sentinel ^ lowestBit) | (lowestBit >>> 1));
        return new TokenKey(table, newSentinel, token);
    }

    // this can be invoked to a depth of 5 from a real token
    TokenKey after()
    {
        int lowestBit = Integer.lowestOneBit(sentinel);
        Invariants.require(lowestBit != 1);
        byte newSentinel = (byte)(sentinel | (lowestBit >>> 1));
        return new TokenKey(table, newSentinel, token);
    }

    @Override
    public Object suffix()
    {
        return token;
    }

    public boolean isMin()
    {
        return sentinel == MIN_TABLE_SENTINEL;
    }

    public boolean isMax()
    {
        return sentinel == MAX_TABLE_SENTINEL;
    }

    public long estimatedSizeOnHeap()
    {
        return EMPTY_SIZE + token().getHeapSize();
    }

    public TokenKey withTable(TableId table)
    {
        return new TokenKey(table, sentinel, token);
    }

    @Override
    public RangeFactory rangeFactory()
    {
        return this;
    }

    @Override
    public Range newRange(RoutingKey start, RoutingKey end)
    {
        return TokenRange.create((TokenKey) start, (TokenKey) end);
    }

    @Override
    public Range newAntiRange(RoutingKey start, RoutingKey end)
    {
        return TokenRange.createUnsafe((TokenKey) start, (TokenKey) end);
    }

    @Override
    public RoutingKey toUnseekable()
    {
        return this;
    }

    public boolean isSentinel()
    {
        return sentinel != NORMAL_SENTINEL;
    }

    public boolean isTableSentinel()
    {
        return (sentinel & PREFIX_MASK) != (NORMAL_SENTINEL & PREFIX_MASK);
    }

    public boolean isTokenSentinel()
    {
        return (sentinel & POSTFIX_MASK) != (NORMAL_SENTINEL & POSTFIX_MASK);
    }

    public static TokenKey min(TableId table, IPartitioner partitioner)
    {
        return new TokenKey(table, MIN_TABLE_SENTINEL, partitioner.getMinimumToken());
    }

    public static TokenKey max(TableId table, IPartitioner partitioner)
    {
        return new TokenKey(table, MAX_TABLE_SENTINEL, partitioner.getMinimumToken());
    }

    public static TokenKey before(TableId table, Token token)
    {
        return new TokenKey(table, BEFORE_TOKEN_SENTINEL, token);
    }

    public static class Serializer implements AccordSearchableKeySerializer<TokenKey>
    {
        private Serializer() {}

        @Override
        public void serialize(TokenKey key, DataOutputPlus out, int version) throws IOException
        {
            key.table.serializeCompact(out);
            Invariants.require(key.token.getPartitioner() == getPartitioner());
            Token.compactSerializer.serialize(key.token, out, version);
        }

        @Override
        public void skip(DataInputPlus in, int version) throws IOException
        {
            TableId.skipCompact(in);
            Token.compactSerializer.skip(in, getPartitioner(), version);
        }

        @Override
        public boolean keysWithSamePrefixAreFixedLength(TokenKey key)
        {
            return key.token.getPartitioner().isFixedLength();
        }

        @Override
        public int lengthWithoutPrefix(TokenKey key)
        {
            return key.token.tokenFactory().byteSize(key.token);
        }

        @Override
        public void serializePrefix(Object prefix, DataOutputPlus out, int version) throws IOException
        {
            ((TableId)prefix).serialize(out);
        }

        @Override
        public void serializeWithoutPrefixOrLength(TokenKey key, DataOutputPlus out, int version) throws IOException
        {
            key.token.tokenFactory().serialize(key.token, out);
        }

        @Override
        public void skipPrefix(DataInputPlus in, int version) throws IOException
        {
            TableId.skip(in);
        }

        @Override
        public void skipKeyWithoutPrefixOrLength(DataInputPlus in, int version) throws IOException
        {
//            key.token.tokenFactory().serialize(key.token, out);
        }

        @Override
        public Object deserializePrefix(DataInputPlus in, int version) throws IOException
        {
            return null;
        }

        @Override
        public TokenKey deserializeWithPrefix(Object prefix, DataInputPlus in, int version) throws IOException
        {
            return null;
        }

        @Override
        public TokenKey deserialize(DataInputPlus in, int version) throws IOException
        {
            TableId table = TableId.deserializeCompact(in).intern();
            byte sentinel = in.readByte();
            Token token = Token.compactSerializer.deserialize(in, getPartitioner(), version);
            return new TokenKey(table, sentinel, token);
        }

        public TokenKey fromBytes(ByteBuffer bytes, IPartitioner partitioner)
        {
            TableId tableId = TableId.deserializeCompact(bytes, ByteBufferAccessor.instance, 0).intern();
            bytes.position(tableId.serializedCompactSize());
            byte sentinel = bytes.get();
            Token token = Token.compactSerializer.deserialize(bytes, partitioner);
            return new TokenKey(tableId, sentinel, token);
        }

        public ByteBuffer toBytes(TokenKey routingKey)
        {
            int size = (int) (routingKey.table.serializedCompactSize() + 1 + Token.compactSerializer.serializedSize(routingKey.token));
            ByteBuffer out = ByteBuffer.allocate(size);
            int position = routingKey.table.serializeCompact(out, ByteBufferAccessor.instance, 0);
            out.position(position);
            out.put(routingKey.sentinel);
            Token.compactSerializer.serialize(routingKey.token, out);
            out.flip();
            return out;
        }

        @Override
        public long serializedSize(TokenKey key, int version)
        {
            return key.table.serializedCompactSize() + Token.compactSerializer.serializedSize(key.token(), version);
        }
    }

    public static final Serializer serializer = new Serializer();

    public static class KeyspaceSplitter implements ShardDistributor
    {
        final EvenSplit<BigInteger> subSplitter;
        public KeyspaceSplitter(EvenSplit<BigInteger> subSplitter)
        {
            this.subSplitter = subSplitter;
        }

        @Override
        public List<Ranges> split(Ranges ranges)
        {
            Map<TableId, List<Range>> byTable = new TreeMap<>();
            for (Range range : ranges)
            {
                byTable.computeIfAbsent(((AccordRoutableKey)range.start()).table, ignore -> new ArrayList<>())
                          .add(range);
            }

            List<Ranges> results = new ArrayList<>();
            for (List<Range> keyspaceRanges : byTable.values())
            {
                List<Ranges> splits = subSplitter.split(Ranges.ofSortedAndDeoverlapped(keyspaceRanges.toArray(new Range[0])));

                for (int i = 0; i < splits.size(); i++)
                {
                    if (i == results.size()) results.add(Ranges.EMPTY);
                    results.set(i, results.get(i).with(splits.get(i)));
                }
            }
            return results;
        }

        @Override
        public Range splitRange(Range range, int from, int to, int numSplits)
        {
            return subSplitter.splitRange(range, from, to, numSplits);
        }

        @Override
        public int numberOfSplitsPossible(Range range)
        {
            return subSplitter.numberOfSplitsPossible(range);
        }
    }
}
