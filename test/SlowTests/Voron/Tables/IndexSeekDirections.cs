using System;
using System.Collections.Generic;
using System.Linq;
using FastTests.Voron;
using Sparrow.Binary;
using Sparrow.Server;
using Tests.Infrastructure;
using Voron;
using Voron.Data.Tables;
using Voron.Impl;
using Xunit;

namespace SlowTests.Voron.Tables
{
    // The tree index seeks (forward with a prefix, backward with or without one) and the fixed size index seek from the last
    // entry, against a model of the table: values in key order, the ids of a value in increasing order.
    public class IndexSeekDirections(ITestOutputHelper output) : StorageTest(output)
    {
        private const int NumberOfRows = 6_000;

        [RavenFact(RavenTestCategory.Voron)]
        public void SeeksMatchTheModel()
        {
            Slice docs, byGroup, byEtag;
            using (StorageEnvironment.GetStaticContext(out var ctx))
            {
                Slice.From(ctx, "Docs", ByteStringType.Immutable, out docs);
                Slice.From(ctx, "ByGroup", ByteStringType.Immutable, out byGroup);
                Slice.From(ctx, "ByEtag", ByteStringType.Immutable, out byEtag);
            }

            var schema = new TableSchema()
                .DefineKey(new TableSchema.IndexDef { StartIndex = 0, Count = 1 })
                .DefineIndex(new TableSchema.IndexDef { StartIndex = 1, Count = 1, Name = byGroup })
                .DefineFixedSizeIndex(new TableSchema.FixedSizeKeyIndexDef { StartIndex = 2, Name = byEtag });

            // group of row i: a few prefixes, values with one id and values with many (more than fit in an embedded tree)
            static string GroupOf(int i) => i % 3 == 0 ? $"a/{i % 40:D3}" : i % 3 == 1 ? $"b/{i:D5}" : $"c/{i % 7:D3}";

            using (var tx = Env.WriteTransaction())
            {
                schema.Create(tx, docs, 16);
                var table = tx.OpenTable(schema, docs);
                var builder = new TableValueBuilder();
                for (int i = 0; i < NumberOfRows; i++)
                {
                    using (Slice.From(tx.Allocator, $"rows/{i:D8}", out Slice key))
                    using (Slice.From(tx.Allocator, GroupOf(i), out Slice group))
                    {
                        long etag = Bits.SwapBytes((long)i);
                        builder.Reset();
                        builder.Add(key);
                        builder.Add(group);
                        builder.Add(etag);
                        table.Set(builder);
                    }
                }
                tx.Commit();
            }

            // the model: rows by group in key order, ids (row numbers here) in insertion order, which is the id order
            var byValue = Enumerable.Range(0, NumberOfRows).GroupBy(GroupOf).OrderBy(g => g.Key, StringComparer.Ordinal)
                .Select(g => (Value: g.Key, Rows: g.ToList())).ToList();

            using (var tx = Env.ReadTransaction())
            {
                var table = tx.OpenTable(schema, docs);
                var index = schema.Indexes[byGroup];

                foreach (var (start, prefix, skip) in new[] { ("a/", "a/", 0L), ("a/010", "a/", 5L), ("b/", "b/", 0L), ("b/03000", "b/", 17L), ("c/", "c/", 0L), ("d/", "d/", 0L), ("a/", "a/", 100_000L) })
                {
                    var expected = byValue.Where(v => string.CompareOrdinal(v.Value, start) >= 0 && v.Value.StartsWith(prefix, StringComparison.Ordinal))
                        .SelectMany(v => v.Rows.Select(r => (v.Value, r))).Skip((int)Math.Min(skip, int.MaxValue)).ToList();

                    Assert.Equal(expected, Read(tx, table.SeekForwardFromPrefix(index, From(tx, start), From(tx, prefix), skip)));
                }

                foreach (var (last, prefix, skip) in new (string, string, long)[] { ("a/~", "a/", 0), ("a/020", "a/", 3), ("b/~", "b/", 0), ("b/04000", "b/", 9), ("c/~", null, 0), ("a/", "a/", 0), ("zzz", null, 11) })
                {
                    var expected = ExpectedBackward(byValue, last, prefix).Skip((int)skip).ToList();
                    var seek = prefix == null
                        ? table.SeekBackwardFrom(index, null, From(tx, last), skip)
                        : table.SeekBackwardFrom(index, From(tx, prefix), From(tx, last), skip);
                    Assert.Equal(expected, Read(tx, seek));

                    if (prefix != null && skip == 0)
                        Assert.Equal(expected, Read(tx, table.SeekBackwardFrom(index, From(tx, prefix), From(tx, last))));
                }

                foreach (var skip in new long[] { 0, 1, 2_500, NumberOfRows - 1, NumberOfRows, NumberOfRows + 3 })
                {
                    var expected = Enumerable.Range(0, NumberOfRows).Reverse().Skip((int)skip).ToList();
                    var actual = new List<int>();
                    foreach (var holder in table.SeekBackwardFromLast(schema.FixedSizeIndexes[byEtag], skip))
                        actual.Add(RowOf(holder.Reader));
                    Assert.Equal(expected, actual);
                }
            }
        }

        // SeekBackwardFrom: the value at or before last; if it is not in the prefix, the one before it; then backward while in the prefix
        private static IEnumerable<(string, int)> ExpectedBackward(List<(string Value, List<int> Rows)> byValue, string last, string prefix)
        {
            var position = byValue.FindLastIndex(v => string.CompareOrdinal(v.Value, last) <= 0);
            if (position < 0)
                yield break;

            if (prefix != null && byValue[position].Value.StartsWith(prefix, StringComparison.Ordinal) == false)
                position--;

            for (; position >= 0; position--)
            {
                var (value, rows) = byValue[position];
                if (prefix != null && value.StartsWith(prefix, StringComparison.Ordinal) == false)
                    yield break;

                for (int i = rows.Count - 1; i >= 0; i--)
                    yield return (value, rows[i]);
            }
        }

        private static List<(string, int)> Read(Transaction tx, IEnumerable<Table.SeekResult> seek)
        {
            var results = new List<(string, int)>();
            foreach (var result in seek)
                results.Add((result.Key.ToString(), RowOf(result.Result.Reader)));
            return results;
        }

        private static unsafe int RowOf(TableValueReader reader)
        {
            var key = reader.Read(0, out int size);
            return int.Parse(new string((sbyte*)key, 0, size).AsSpan("rows/".Length));
        }

        private static Slice From(Transaction tx, string value)
        {
            global::Voron.Slice.From(tx.Allocator, value, out var slice);
            return slice;
        }
    }
}
