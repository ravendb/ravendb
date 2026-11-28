using System;
using System.Buffers.Binary;
using System.Collections.Generic;
using System.Linq;
using Sparrow.Binary;
using Sparrow.Server;
using Tests.Infrastructure;
using Voron;
using Voron.Data.Tables;
using Voron.Impl;
using Xunit;

namespace FastTests.Voron.Tables
{
    public class IndexValueReads : TableStorageTest
    {
        private static readonly Slice ByValue;

        // value 1 has 300 ids (a large fixed size tree), value 2 has 50 (embedded), value 3 has one, value 4 has 200
        private static readonly (long Value, int Ids)[] Values = [(1, 300), (2, 50), (3, 1), (4, 200)];
        private static readonly int Rows = Values.Sum(v => v.Ids);

        static IndexValueReads()
        {
            using (StorageEnvironment.GetStaticContext(out var ctx))
            {
                Slice.From(ctx, "ByValue", ByteStringType.Immutable, out ByValue);
            }
        }

        public IndexValueReads(ITestOutputHelper output) : base(output)
        {
        }

        [RavenTheory(RavenTestCategory.Voron)]
        [InlineData(false)]
        [InlineData(true)]
        public void ReadingOtherValuesOfTheIndexDoesNotMoveAnOpenSeek(bool backward)
        {
            using var tx = Env.WriteTransaction();
            var (table, byValue) = CreateTable(tx);

            using (ValueKey(tx, 1, out var value1))
            using (ValueKey(tx, 2, out var value2))
            using (ValueKey(tx, 4, out var value4))
            {
                var ids = new List<long>();
                var seek = backward
                    ? table.SeekBackwardFrom(byValue, prefix: null, Slices.AfterAllKeys, skip: 0)
                    : table.SeekForwardFrom(byValue, Slices.BeforeAllKeys, skip: 0);

                foreach (var result in seek)
                {
                    ids.Add(Id(ref result.Result.Reader));

                    // the reads of single values share one fixed size tree instance with the seek
                    Assert.Equal(300, table.GetCountOfMatchesFor(byValue, value1));
                    Assert.Equal(50, table.GetCountOfMatchesFor(byValue, value2));
                    Assert.True(table.SeekOneForwardFromPrefix(byValue, value4, out var first));
                    Assert.Equal(352, Id(ref first));
                }

                var expected = Enumerable.Range(1, Rows).Select(id => (long)id);
                Assert.Equal(backward ? expected.Reverse() : expected, ids);
            }
        }

        private static (Table Table, TableSchema.IndexDef ByValue) CreateTable(Transaction tx)
        {
            var byValue = new TableSchema.IndexDef { StartIndex = 1, Count = 1, Name = ByValue };
            var schema = new TableSchema()
                .DefineKey(new TableSchema.IndexDef { StartIndex = 0, Count = 1 });
            schema.DefineIndex(byValue);

            schema.Create(tx, "rows", 16);
            var table = tx.OpenTable(schema, "rows");

            long id = 1;
            foreach (var (value, count) in Values)
            {
                for (var i = 0; i < count; i++, id++)
                {
                    using (table.Allocate(out var builder))
                    {
                        builder.Add(Bits.SwapBytes(id));
                        builder.Add(Bits.SwapBytes(value));
                        table.Insert(builder);
                    }
                }
            }

            return (table, byValue);
        }

        private static long Id(ref TableValueReader reader) => Bits.SwapBytes(reader.ReadLong(0));

        private static ByteStringContext.InternalScope ValueKey(Transaction tx, long value, out Slice slice)
        {
            Span<byte> key = stackalloc byte[sizeof(long)];
            BinaryPrimitives.WriteInt64BigEndian(key, value);
            return Slice.From(tx.Allocator, key, out slice);
        }
    }
}
