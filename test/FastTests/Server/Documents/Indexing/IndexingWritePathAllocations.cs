using System;
using System.Collections.Generic;
using System.Linq;
using Raven.Client.Documents.Indexes;
using Raven.Server.Documents.Indexes;
using Raven.Server.Documents.Indexes.Auto;
using Raven.Server.Documents.Indexes.MapReduce;
using Raven.Server.Documents.Indexes.MapReduce.Auto;
using Raven.Server.ServerWide;
using Raven.Server.ServerWide.Context;
using Sparrow.Server;
using Tests.Infrastructure;
using Voron;
using Voron.Data.BTrees;
using Voron.Data.Fixed;
using Xunit;

namespace FastTests.Server.Documents.Indexing
{
    public unsafe class IndexingWritePathAllocations(ITestOutputHelper output) : RavenLowLevelTestBase(output)
    {
        private const int NumberOfDocuments = 2_000;
        private const string Collection = "Orders";

        private static readonly string CollectionTreeName = IndexStorage.Constants.DocumentReferenceCollectionPrefix + Collection;

        // Before RavenDB-27880 writing the references of a batch allocated two lists per key, a dictionary entry, a sort delegate
        // and a tree iterator per referencing document, ~660 bytes per document with two references each.
        [RavenFact(RavenTestCategory.Indexes)]
        public void Writing_unchanged_references()
        {
            using (var database = CreateDocumentDatabase())
            using (var index = CreateMapIndex(database))
            using (index._contextPool.AllocateOperationContext(out TransactionOperationContext context))
            {
                var references = index._indexStorage.ReferencesForDocuments;

                using (var tx = context.OpenWriteTransaction())
                {
                    CreateReferenceKeys(tx.InnerTransaction.Allocator, out var ids, out var referenced);
                    WriteReferences(references, tx, ids, referenced);
                    tx.Commit();
                }

                using (var tx = context.OpenWriteTransaction())
                {
                    CreateReferenceKeys(tx.InnerTransaction.Allocator, out var ids, out var referenced);

                    // a new container per batch, as CurrentIndexingScope does
                    var bytes = MeasurePerDocument(() => WriteReferences(references, tx, ids, referenced));
                    AssertAtMost(400, "Write unchanged references", bytes);
                }
            }
        }

        // Before RavenDB-27880 removing the references of a deleted document allocated ~130 bytes on top of the tree operations
        // (a list, its array and the name of the collection tree).
        [RavenFact(RavenTestCategory.Indexes)]
        public void Removing_references_of_deleted_documents()
        {
            using (var database = CreateDocumentDatabase())
            using (var index = CreateMapIndex(database))
            using (index._contextPool.AllocateOperationContext(out TransactionOperationContext context))
            {
                var references = index._indexStorage.ReferencesForDocuments;

                using (var tx = context.OpenWriteTransaction())
                {
                    CreateReferenceKeys(tx.InnerTransaction.Allocator, out var ids, out var referenced);
                    WriteReferences(references, tx, ids, referenced);
                    tx.Commit();
                }

                double treeOperations;
                using (var tx = context.OpenWriteTransaction())
                {
                    var allocator = tx.InnerTransaction.Allocator;
                    var referencesTree = tx.InnerTransaction.ReadTree(IndexStorage.IndexSchema.References);
                    var collectionTree = tx.InnerTransaction.ReadTree(CollectionTreeName);
                    var buffer = new Slice[16];
                    CreateReferenceKeys(allocator, out var ids, out _);

                    treeOperations = MeasureRemoval(i => RemoveWithTreeOperations(referencesTree, collectionTree, ids[i], buffer, allocator));
                    Assert.Equal((0, 0), references.GetReferenceTablesCount(Collection, tx));
                }

                using (var tx = context.OpenWriteTransaction())
                {
                    CreateReferenceKeys(tx.InnerTransaction.Allocator, out var ids, out _);

                    var bytes = MeasureRemoval(i => references.RemoveReferences(ids[i], Collection, tx));
                    Assert.Equal((0, 0), references.GetReferenceTablesCount(Collection, tx));

                    Output.WriteLine($"{"Tree operations of a removal",-48} {treeOperations,8:F1} bytes/doc");
                    AssertAtMost(32, "Remove references, over the tree operations", bytes - treeOperations);
                }
            }
        }

        [RavenFact(RavenTestCategory.Indexes)]
        public void Rewriting_references_keeps_only_the_current_ones()
        {
            var many = Enumerable.Range(0, 300).Select(i => $"items/{i:D4}").ToArray();
            var even = many.Where((_, i) => i % 2 == 0).ToArray();

            using (var database = CreateDocumentDatabase())
            using (var index = CreateMapIndex(database))
            using (index._contextPool.AllocateOperationContext(out TransactionOperationContext context))
            {
                var references = index._indexStorage.ReferencesForDocuments;

                using (var tx = context.OpenWriteTransaction())
                {
                    WriteReferences(references, tx, new Dictionary<string, string[]>
                    {
                        ["orders/a"] = ["companies/3", "companies/1", "companies/2"],
                        ["orders/b"] = ["companies/2"],
                        ["orders/c"] = ["companies/1", "companies/4", "companies/1"],
                        ["orders/d"] = many
                    });
                    tx.Commit();
                }

                using (var tx = context.OpenWriteTransaction())
                {
                    AssertReferences(tx, "orders/a", "companies/1", "companies/2", "companies/3");
                    AssertReferences(tx, "orders/b", "companies/2");
                    AssertReferences(tx, "orders/c", "companies/1", "companies/4");
                    AssertReferences(tx, "orders/d", many);
                    AssertReferencedBy(tx, "companies/1", "orders/a", "orders/c");
                    AssertReferencedBy(tx, "companies/2", "orders/a", "orders/b");
                    AssertReferencedBy(tx, "items/0001", "orders/d");

                    // orders/b is not in this batch
                    WriteReferences(references, tx, new Dictionary<string, string[]>
                    {
                        ["orders/a"] = ["companies/5", "companies/2"],
                        ["orders/c"] = ["companies/4"],
                        ["orders/d"] = even
                    });

                    AssertReferences(tx, "orders/a", "companies/2", "companies/5");
                    AssertReferences(tx, "orders/b", "companies/2");
                    AssertReferences(tx, "orders/c", "companies/4");
                    AssertReferences(tx, "orders/d", even);
                    AssertReferencedBy(tx, "companies/1");
                    AssertReferencedBy(tx, "companies/2", "orders/a", "orders/b");
                    AssertReferencedBy(tx, "companies/3");
                    AssertReferencedBy(tx, "companies/4", "orders/c");
                    AssertReferencedBy(tx, "companies/5", "orders/a");
                    AssertReferencedBy(tx, "items/0000", "orders/d");
                    AssertReferencedBy(tx, "items/0001");

                    using (Slice.From(tx.InnerTransaction.Allocator, "orders/a", out Slice a))
                    using (Slice.From(tx.InnerTransaction.Allocator, "orders/d", out Slice d))
                    {
                        references.RemoveReferences(a, Collection, tx);
                        references.RemoveReferences(d, Collection, tx);
                    }

                    AssertReferences(tx, "orders/a");
                    AssertReferences(tx, "orders/d");
                    AssertReferencedBy(tx, "companies/2", "orders/b");
                    AssertReferencedBy(tx, "companies/5");
                    AssertReferencedBy(tx, "items/0000");
                    Assert.Equal((2, 2), references.GetReferenceTablesCount(Collection, tx));
                }
            }
        }

        // Before RavenDB-27880 reading the map entries of a document allocated a queue, its array and an object per entry,
        // ~145 bytes on top of the tree iteration for a document with two entries.
        [RavenFact(RavenTestCategory.Indexes)]
        public void Reading_map_entries_of_a_document()
        {
            const int entriesPerDocument = 2;

            using (var database = CreateDocumentDatabase())
            using (var index = AutoMapReduceIndex.CreateNew(GetCountByLocationDefinition(), database))
            using (index._contextPool.AllocateOperationContext(out TransactionOperationContext context))
            {
                var work = index.MapReduceWorkContext;

                using (var tx = context.OpenWriteTransaction())
                using (index.InitializeIndexingWork(context))
                {
                    var ids = CreateDocumentIds(tx.InnerTransaction.Allocator);
                    for (int i = 0; i < NumberOfDocuments; i++)
                    {
                        work.DocumentMapEntries.RepurposeInstance(ids[i], clone: false);
                        for (int j = 0; j < entriesPerDocument; j++)
                        {
                            ulong reduceKeyHash = ReduceKeyHash(i % 50 + j);
                            using (Slice.External(tx.InnerTransaction.Allocator, (byte*)&reduceKeyHash, sizeof(ulong), out Slice value))
                                work.DocumentMapEntries.Add(entriesPerDocument * i + j, value);
                        }
                    }

                    tx.Commit();
                }

                using (var tx = context.OpenWriteTransaction())
                using (index.InitializeIndexingWork(context))
                {
                    var ids = CreateDocumentIds(tx.InnerTransaction.Allocator);

                    work.DocumentMapEntries.RepurposeInstance(ids[7], clone: false);
                    MapReduceIndexBase<AutoMapReduceIndexDefinition, AutoIndexField>.GetMapEntries(work.DocumentMapEntries, work.MapEntries);
                    Assert.Equal([(14L, ReduceKeyHash(7)), (15L, ReduceKeyHash(8))], work.MapEntries.Select(x => (x.Id, x.ReduceKeyHash)));

                    ulong expected = 0, actual = 0;
                    var treeIteration = MeasurePerDocument(() =>
                    {
                        for (int i = 0; i < NumberOfDocuments; i++)
                        {
                            work.DocumentMapEntries.RepurposeInstance(ids[i], clone: false);
                            expected += IterateWithFixedSizeTree(work.DocumentMapEntries);
                        }
                    });

                    var bytes = MeasurePerDocument(() =>
                    {
                        for (int i = 0; i < NumberOfDocuments; i++)
                        {
                            work.DocumentMapEntries.RepurposeInstance(ids[i], clone: false);
                            MapReduceIndexBase<AutoMapReduceIndexDefinition, AutoIndexField>.GetMapEntries(work.DocumentMapEntries, work.MapEntries);
                            foreach (var entry in work.MapEntries)
                                actual += entry.ReduceKeyHash + (ulong)entry.Id;
                        }
                    });

                    Assert.Equal(expected, actual);
                    Output.WriteLine($"{"Tree iteration of the map entries",-48} {treeIteration,8:F1} bytes/doc");
                    AssertAtMost(8, "Read map entries, over the tree iteration", bytes - treeIteration);
                }
            }
        }

        // Before RavenDB-27880 a results store of the nested type allocated an unused set and the strings of its key,
        // and one of the tree type allocated the set, the strings of the tree name and its page delegates.
        [RavenFact(RavenTestCategory.Indexes)]
        public void Creating_results_stores_of_a_batch()
        {
            using (var database = CreateDocumentDatabase())
            using (var index = AutoMapReduceIndex.CreateNew(GetCountByLocationDefinition(), database))
            using (index._contextPool.AllocateOperationContext(out TransactionOperationContext context))
            {
                var work = index.MapReduceWorkContext;

                using (var tx = context.OpenWriteTransaction())
                using (index.InitializeIndexingWork(context))
                {
                    byte treeType = (byte)MapResultsStorageType.Tree;
                    for (int i = 0; i < NumberOfDocuments; i++)
                    {
                        using (var store = new MapReduceResultsStore(TreeReduceKeyHash(i), MapResultsStorageType.Tree, context, work, create: true))
                        using (Slice.External(tx.InnerTransaction.Allocator, &treeType, sizeof(byte), out Slice value))
                        {
                            Assert.NotNull(store.Tree);
                            work.ResultsStoreTypes.Add((long)TreeReduceKeyHash(i), value);
                        }
                    }

                    tx.Commit();
                }

                using (var tx = context.OpenWriteTransaction())
                using (index.InitializeIndexingWork(context))
                {
                    foreach (var reduceKeyHash in new[] { 0UL, ReduceKeyHash(3), ulong.MaxValue })
                    {
                        using (var store = index.CreateResultsStore(work.ResultsStoreTypes, reduceKeyHash, context, create: true))
                            Assert.Equal(MapReduceResultsStore.NestedValuesPrefix + reduceKeyHash, store.GetNestedResultsSection().Name.ToString());
                    }

                    int nested = 0;
                    var bytes = MeasurePerDocument(() =>
                    {
                        for (int i = 0; i < NumberOfDocuments; i++)
                        {
                            using (var store = index.CreateResultsStore(work.ResultsStoreTypes, ReduceKeyHash(i), context, create: true))
                            {
                                if (store.Type == MapResultsStorageType.Nested)
                                    nested++;
                            }
                        }
                    });

                    Assert.Equal(4 * NumberOfDocuments, nested);
                    AssertAtMost(160, "Nested results store", bytes);
                }

                double treeOpen;
                using (var tx = context.OpenWriteTransaction())
                {
                    var names = new Slice[NumberOfDocuments];
                    for (int i = 0; i < NumberOfDocuments; i++)
                        Slice.From(tx.InnerTransaction.Allocator, MapReduceResultsStore.ReduceTreePrefix + TreeReduceKeyHash(i), ByteStringType.Immutable, out names[i]);

                    int trees = 0;
                    var before = GC.GetAllocatedBytesForCurrentThread();
                    for (int i = 0; i < NumberOfDocuments; i++)
                    {
                        if (tx.InnerTransaction.ReadTree(names[i]) != null)
                            trees++;
                    }
                    treeOpen = (double)(GC.GetAllocatedBytesForCurrentThread() - before) / NumberOfDocuments;

                    Assert.Equal(NumberOfDocuments, trees);
                }

                using (var tx = context.OpenWriteTransaction())
                using (index.InitializeIndexingWork(context))
                {
                    // every batch opens the tree of a store in a new transaction
                    int trees = 0;
                    var before = GC.GetAllocatedBytesForCurrentThread();
                    for (int i = 0; i < NumberOfDocuments; i++)
                    {
                        using (var store = index.CreateResultsStore(work.ResultsStoreTypes, TreeReduceKeyHash(i), context, create: false))
                        {
                            if (store.Type == MapResultsStorageType.Tree && store.Tree != null)
                                trees++;
                        }
                    }
                    var bytes = (double)(GC.GetAllocatedBytesForCurrentThread() - before) / NumberOfDocuments;

                    Assert.Equal(NumberOfDocuments, trees);
                    Output.WriteLine($"{"Open of a reduce tree",-48} {treeOpen,8:F1} bytes/doc");
                    AssertAtMost(320, "Tree results store, over the open of its tree", bytes - treeOpen);
                }
            }
        }

        private static ulong ReduceKeyHash(int i) => (ulong)(i + 1) * 0x9E3779B97F4A7C15UL;

        private static ulong TreeReduceKeyHash(int i) => ReduceKeyHash(NumberOfDocuments + i);

        private static AutoMapReduceIndexDefinition GetCountByLocationDefinition()
        {
            return new AutoMapReduceIndexDefinition("Users",
                [new AutoIndexField { Name = "Count", Id = 1, Aggregation = AggregationOperation.Count, Storage = FieldStorage.Yes }],
                [new AutoIndexField { Name = "Location", Id = 2, Storage = FieldStorage.Yes }]);
        }

        private static ulong IterateWithFixedSizeTree(FixedSizeTree tree)
        {
            ulong sum = 0;
            using (var it = tree.Iterate())
            {
                if (it.Seek(long.MinValue) == false)
                    return 0;

                do
                {
                    ulong reduceKeyHash;
                    it.CreateReaderForCurrent().Read((byte*)&reduceKeyHash, sizeof(ulong));
                    sum += reduceKeyHash + (ulong)it.CurrentKey;
                } while (it.MoveNext());
            }

            return sum;
        }

        private static AutoMapIndex CreateMapIndex(Raven.Server.Documents.DocumentDatabase database)
        {
            return AutoMapIndex.CreateNew(new AutoMapIndexDefinition(Collection, [new AutoIndexField { Name = "Name", Storage = FieldStorage.No }]), database);
        }

        private static void AssertReferences(RavenTransaction tx, string key, params string[] expected) =>
            AssertMultiValues(tx, tx.InnerTransaction.ReadTree(IndexStorage.IndexSchema.References), key, expected);

        private static void AssertReferencedBy(RavenTransaction tx, string key, params string[] expected) =>
            AssertMultiValues(tx, tx.InnerTransaction.ReadTree(CollectionTreeName), key, expected);

        private static void AssertMultiValues(RavenTransaction tx, Tree tree, string key, string[] expected)
        {
            var actual = new List<string>();
            using (Slice.From(tx.InnerTransaction.Allocator, key, out Slice keySlice))
            using (var it = tree.MultiRead(keySlice))
            {
                if (it.Seek(Slices.BeforeAllKeys))
                {
                    do
                    {
                        actual.Add(it.CurrentKey.ToString());
                    } while (it.MoveNext());
                }
            }

            Assert.Equal(expected.OrderBy(x => x, StringComparer.Ordinal), actual);
        }

        private static void WriteReferences(IndexStorage.ReferencesBase references, RavenTransaction tx, Dictionary<string, string[]> referencesByDocument)
        {
            var container = new ReferenceContainer();
            foreach (var (id, referenced) in referencesByDocument)
            {
                foreach (var value in referenced)
                {
                    // a slice per call, as LoadDocument does
                    Slice.From(tx.InnerTransaction.Allocator, id, ByteStringType.Immutable, out Slice idSlice);
                    Slice.From(tx.InnerTransaction.Allocator, value, ByteStringType.Immutable, out Slice valueSlice);
                    container.Add(idSlice, valueSlice);
                }
            }

            references.WriteReferencesForSingleCollection(Collection, container, tx);
        }

        private static void WriteReferences(IndexStorage.ReferencesBase references, RavenTransaction tx, Slice[] ids, Slice[][] referenced)
        {
            var container = new ReferenceContainer();
            for (int i = 0; i < ids.Length; i++)
            {
                foreach (var value in referenced[i])
                    container.Add(ids[i], value);
            }

            references.WriteReferencesForSingleCollection(Collection, container, tx);
        }

        private static void RemoveWithTreeOperations(Tree referencesTree, Tree collectionTree, Slice id, Slice[] buffer, ByteStringContext allocator)
        {
            int count = 0;
            using (var it = referencesTree.MultiRead(id))
            {
                if (it.Seek(Slices.BeforeAllKeys) == false)
                    return;

                do
                {
                    buffer[count++] = it.CurrentKey.Clone(allocator, ByteStringType.Immutable);
                } while (it.MoveNext());
            }

            for (int i = 0; i < count; i++)
            {
                referencesTree.MultiDelete(id, buffer[i]);
                collectionTree.MultiDelete(buffer[i], id);
                buffer[i].Release(allocator);
            }
        }

        private static Slice[] CreateDocumentIds(ByteStringContext allocator)
        {
            var ids = new Slice[NumberOfDocuments];
            for (int i = 0; i < NumberOfDocuments; i++)
                Slice.From(allocator, $"orders/{i:D8}", ByteStringType.Immutable, out ids[i]);
            return ids;
        }

        private static void CreateReferenceKeys(ByteStringContext allocator, out Slice[] ids, out Slice[][] referenced)
        {
            ids = CreateDocumentIds(allocator);
            referenced = new Slice[NumberOfDocuments][];
            for (int i = 0; i < NumberOfDocuments; i++)
            {
                Slice.From(allocator, $"companies/{i % 100:D8}", ByteStringType.Immutable, out var company);
                Slice.From(allocator, $"employees/{i % 30:D8}", ByteStringType.Immutable, out var employee);
                referenced[i] = [company, employee];
            }
        }

        private static double MeasurePerDocument(Action batch)
        {
            batch(); // warm up so JIT and pool filling are excluded from the measurement

            const int rounds = 3;
            var before = GC.GetAllocatedBytesForCurrentThread();
            for (int i = 0; i < rounds; i++)
                batch();

            return (double)(GC.GetAllocatedBytesForCurrentThread() - before) / (rounds * NumberOfDocuments);
        }

        // each document can be removed once, so the first ones warm up and the rest are measured
        private static double MeasureRemoval(Action<int> remove)
        {
            const int warmup = 100;
            for (int i = 0; i < warmup; i++)
                remove(i);

            var before = GC.GetAllocatedBytesForCurrentThread();
            for (int i = warmup; i < NumberOfDocuments; i++)
                remove(i);

            return (double)(GC.GetAllocatedBytesForCurrentThread() - before) / (NumberOfDocuments - warmup);
        }

        private void AssertAtMost(int limit, string name, double bytesPerDocument)
        {
            Output.WriteLine($"{name,-48} {bytesPerDocument,8:F1} bytes/doc");
#if !DEBUG // debug-only checks allocate too
            Assert.True(bytesPerDocument <= limit, $"{name} allocated {bytesPerDocument:F1} bytes/doc, expected at most {limit}");
#endif
        }
    }
}
