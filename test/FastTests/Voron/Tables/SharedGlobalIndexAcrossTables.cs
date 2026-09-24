using System.Collections.Generic;
using Sparrow.Server;
using Tests.Infrastructure;
using Voron;
using Voron.Data.Fixed;
using Voron.Data.Tables;
using Xunit;

namespace FastTests.Voron.Tables
{
    // A global variable size index is shared by every table that defines it, and the nested fixed size tree under it is
    // keyed by the index value - so two tables that index the same value write into the very same tree. This is the
    // shape of Collection.Revisions.* sharing DeleteRevisionEtag / ResolvedFlagByEtag, where the index value is a
    // single flag byte and therefore identical for every collection.
    public unsafe class SharedGlobalIndexAcrossTables(ITestOutputHelper output) : StorageTest(output)
    {
        private static TableSchema CreateSchema(ByteStringContext allocator, out Slice indexName)
        {
            Slice.From(allocator, "SharedGlobalIndex", ByteStringType.Immutable, out indexName);

            return new TableSchema()
                .DefineKey(new TableSchema.IndexDef { StartIndex = 0, Count = 1 })
                .DefineIndex(new TableSchema.IndexDef { StartIndex = 1, Count = 1, Name = indexName, IsGlobal = true });
        }

        [RavenFact(RavenTestCategory.Voron)]
        public void TablesSharingAGlobalIndexMustResolveTheSameNestedFixedSizeTree()
        {
            var schema = CreateSchema(Allocator, out Slice indexName);

            using (var tx = Env.WriteTransaction())
            {
                schema.Create(tx, "first", 16);
                schema.Create(tx, "second", 16);
                tx.Commit();
            }

            using (var tx = Env.WriteTransaction())
            {
                var first = tx.OpenTable(schema, "first");
                var second = tx.OpenTable(schema, "second");

                var indexTree = tx.ReadTree(indexName, isIndexTree: true);
                Slice.From(tx.Allocator, "shared", ByteStringType.Immutable, out Slice indexValue);

                FixedSizeTree fromFirst = first.GetFixedSizeTree(indexTree, indexValue, 0, isGlobal: true);
                FixedSizeTree fromSecond = second.GetFixedSizeTree(indexTree, indexValue, 0, isGlobal: true);

                // two instances over one tree each keep their own rightmost-leaf append cache, and the stale one
                // appends left of the separator the other instance's split introduced
                Assert.Same(fromFirst, fromSecond);
            }
        }

        [RavenFact(RavenTestCategory.Voron)]
        public void InterleavedInsertsIntoTablesSharingAGlobalIndexMustAllStayReachable()
        {
            var schema = CreateSchema(Allocator, out Slice indexName);

            using (var tx = Env.WriteTransaction())
            {
                schema.Create(tx, "first", 16);
                schema.Create(tx, "second", 16);
                tx.Commit();
            }

            var ids = new List<long>();

            // one transaction on purpose: the append cache lives for the lifetime of the FixedSizeTree instance, and
            // enough rows to push the nested tree well past a single leaf
            using (var tx = Env.WriteTransaction())
            {
                var first = tx.OpenTable(schema, "first");
                var second = tx.OpenTable(schema, "second");

                for (int i = 0; i < 20_000; i++)
                {
                    var table = (i % 2) == 0 ? first : second;

                    using (table.Allocate(out TableValueBuilder builder))
                    using (Slice.From(tx.Allocator, $"items/{i:D8}", out Slice key))
                    using (Slice.From(tx.Allocator, "shared", out Slice indexValue))
                    {
                        builder.Add(key);
                        builder.Add(indexValue);
                        ids.Add(table.Insert(builder));
                    }
                }

                tx.Commit();
            }

            using (var tx = Env.ReadTransaction())
            {
                var indexTree = tx.ReadTree(indexName, isIndexTree: true);
                Slice.From(tx.Allocator, "shared", ByteStringType.Immutable, out Slice indexValue);

                var fst = new FixedSizeTree(tx.LowLevelTransaction, indexTree, indexValue, 0, isIndexTree: true);

                Assert.Equal(ids.Count, fst.NumberOfEntries);

                foreach (var id in ids)
                    Assert.True(fst.Contains(id), $"row id {id} is in the tree but a search cannot reach it");
            }
        }
    }
}
