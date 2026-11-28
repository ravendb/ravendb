using System;
using System.Buffers.Binary;
using System.Collections.Generic;
using Sparrow.Binary;
using Sparrow.Server;
using Tests.Infrastructure;
using Voron;
using Voron.Data.Tables;
using Voron.Impl;
using Xunit;

namespace FastTests.Voron.Tables
{
    public class DynamicKeyIndexSeeks : TableStorageTest
    {
        // rows of (id, group, value): ids 1-6 are in group 1 and 7-12 in group 2, value is id / 2, so ids 2 and 3 (and 4 and 5, ...) share a value
        private static readonly Slice ByGroupAndId;
        private static readonly Slice ByGroupAndValue;

        static DynamicKeyIndexSeeks()
        {
            using (StorageEnvironment.GetStaticContext(out var ctx))
            {
                Slice.From(ctx, "ByGroupAndId", ByteStringType.Immutable, out ByGroupAndId);
                Slice.From(ctx, "ByGroupAndValue", ByteStringType.Immutable, out ByGroupAndValue);
            }
        }

        public DynamicKeyIndexSeeks(ITestOutputHelper output) : base(output)
        {
        }

        [RavenFact(RavenTestCategory.Voron)]
        public void SeekByPrefixOfDuplicateKeysSkipsResultsNotKeys()
        {
            using var tx = Env.WriteTransaction();
            var (table, unique, duplicates) = CreateTable(tx);

            using (Key(tx, 1, out var group1))
            using (Key(tx, 1, 2, out var group1Value2))
            {
                Assert.Equal([1, 2, 3, 4, 5, 6], Ids(table.SeekByPrefix(duplicates, group1, Slices.Empty, 0)));
                Assert.Equal([3, 4, 5, 6], Ids(table.SeekByPrefix(duplicates, group1, Slices.Empty, 2)));
                Assert.Equal([4, 5, 6], Ids(table.SeekByPrefix(duplicates, group1, group1Value2, 0)));
                Assert.Equal([5, 6], Ids(table.SeekByPrefix(duplicates, group1, group1Value2, 1)));
                Assert.Equal(6, table.GetCountOfMatchesForPrefix(duplicates, group1));
            }

            using (Key(tx, 3, out var group3))
            {
                Assert.Empty(Ids(table.SeekByPrefix(duplicates, group3, Slices.Empty, 0)));
                Assert.Equal(0, table.GetCountOfMatchesForPrefix(duplicates, group3));
            }
        }

        [RavenFact(RavenTestCategory.Voron)]
        public void SeekByPrefixOfUniqueKeys()
        {
            using var tx = Env.WriteTransaction();
            var (table, unique, duplicates) = CreateTable(tx);

            using (Key(tx, 2, out var group2))
            using (Key(tx, 2, 9, out var group2Id9))
            {
                Assert.Equal([7, 8, 9, 10, 11, 12], Ids(table.SeekByPrefix(unique, group2, Slices.Empty, 0)));
                Assert.Equal([9, 10, 11, 12], Ids(table.SeekByPrefix(unique, group2, Slices.Empty, 2)));
                Assert.Equal([9, 10, 11, 12], Ids(table.SeekByPrefix(unique, group2, group2Id9, 0)));
                Assert.Equal(6, table.GetCountOfMatchesForPrefix(unique, group2));
            }
        }

        [RavenFact(RavenTestCategory.Voron)]
        public void SeekBackwardByPrimaryKey()
        {
            using var tx = Env.WriteTransaction();
            var (table, _, _) = CreateTable(tx);

            using (PrimaryKey(tx, 5, out var id5))
            {
                Assert.Equal([5, 4, 3, 2, 1], Ids(table.SeekBackwardByPrimaryKey(id5, 0)));
                Assert.Equal([4, 3, 2, 1], Ids(table.SeekBackwardByPrimaryKey(id5, 1)));
            }
        }

        private static (Table Table, TableSchema.DynamicKeyIndexDef Unique, TableSchema.DynamicKeyIndexDef Duplicates) CreateTable(Transaction tx)
        {
            var unique = new TableSchema.DynamicKeyIndexDef { IsGlobal = false, GenerateKey = GroupAndIdKey, Name = ByGroupAndId };
            var duplicates = new TableSchema.DynamicKeyIndexDef { IsGlobal = false, GenerateKey = GroupAndValueKey, Name = ByGroupAndValue, SupportDuplicateKeys = true };

            var schema = new TableSchema()
                .DefineKey(new TableSchema.IndexDef { StartIndex = 0, Count = 1 });
            schema.DefineIndex(unique);
            schema.DefineIndex(duplicates);

            schema.Create(tx, "rows", 16);
            var table = tx.OpenTable(schema, "rows");

            // the rows of one duplicate key are in the order of their storage ids, which is the insertion order here
            for (long id = 1; id <= 12; id++)
            {
                using (table.Allocate(out var builder))
                {
                    builder.Add(Bits.SwapBytes(id));
                    builder.Add(id <= 6 ? 1L : 2L);
                    builder.Add(id / 2);
                    table.Insert(builder);
                }
            }

            return (table, unique, duplicates);
        }

        private static List<long> Ids(IEnumerable<Table.SeekResult> results)
        {
            var ids = new List<long>();
            foreach (var result in results)
                ids.Add(Bits.SwapBytes(result.Result.Reader.ReadLong(0)));
            return ids;
        }

        private static List<long> Ids(IEnumerable<Table.TableValueHolder> results)
        {
            var ids = new List<long>();
            foreach (var result in results)
                ids.Add(Bits.SwapBytes(result.Reader.ReadLong(0)));
            return ids;
        }

        [StorageIndexEntryKeyGenerator]
        internal static ByteStringContext.Scope GroupAndIdKey(Transaction tx, ref TableValueReader tvr, out Slice slice) =>
            Key(tx, (int)tvr.ReadLong(1), Bits.SwapBytes(tvr.ReadLong(0)), out slice);

        [StorageIndexEntryKeyGenerator]
        internal static ByteStringContext.Scope GroupAndValueKey(Transaction tx, ref TableValueReader tvr, out Slice slice) =>
            Key(tx, (int)tvr.ReadLong(1), tvr.ReadLong(2), out slice);

        private static ByteStringContext.Scope Key(Transaction tx, int group, long value, out Slice slice)
        {
            Span<byte> key = stackalloc byte[sizeof(int) + sizeof(long)];
            BinaryPrimitives.WriteInt32BigEndian(key, group);
            BinaryPrimitives.WriteInt64BigEndian(key.Slice(sizeof(int)), value);
            return Slice.From(tx.Allocator, key, out slice);
        }

        private static ByteStringContext.Scope Key(Transaction tx, int group, out Slice slice)
        {
            Span<byte> key = stackalloc byte[sizeof(int)];
            BinaryPrimitives.WriteInt32BigEndian(key, group);
            return Slice.From(tx.Allocator, key, out slice);
        }

        private static ByteStringContext.Scope PrimaryKey(Transaction tx, long id, out Slice slice)
        {
            Span<byte> key = stackalloc byte[sizeof(long)];
            BinaryPrimitives.WriteInt64BigEndian(key, id);
            return Slice.From(tx.Allocator, key, out slice);
        }
    }
}
