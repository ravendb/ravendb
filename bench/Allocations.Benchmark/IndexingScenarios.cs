using System;
using System.Collections.Generic;
using System.Linq;
using FastTests;
using Raven.Client.Documents.Indexes;
using Raven.Server.Documents.Indexes;
using Raven.Server.Documents.Indexes.Auto;
using Raven.Server.Documents.Indexes.MapReduce;
using Raven.Server.Documents.Indexes.MapReduce.Auto;
using Raven.Server.ServerWide;
using Raven.Server.ServerWide.Context;
using Sparrow.Server;
using Voron;
using Voron.Data.BTrees;
using Voron.Data.Fixed;
using Xunit;

namespace Allocations.Benchmark
{
    // The storage of an index: its references, the map entries of documents and the results stores of map-reduce
    public unsafe class IndexingScenarios(ITestOutputHelper output) : RavenLowLevelTestBase(output)
    {
        private const int NumberOfDocuments = 2_000;
        private const string Collection = "Orders";

        private static readonly string CollectionTreeName = IndexStorage.Constants.DocumentReferenceCollectionPrefix + Collection;

        public void WriteUnchangedReferences()
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
                    Measure.Report("Write unchanged references", Measure.PerItem(() => WriteReferences(references, tx, ids, referenced), NumberOfDocuments), "doc");
                }
            }
        }

        public void RemoveReferencesOfDeletedDocuments()
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

                    var next = 0;
                    treeOperations = MeasureRemoval(() => RemoveWithTreeOperations(referencesTree, collectionTree, ids[next++], buffer, allocator));
                    Assert.Equal((0, 0), references.GetReferenceTablesCount(Collection, tx));
                }

                using (var tx = context.OpenWriteTransaction())
                {
                    CreateReferenceKeys(tx.InnerTransaction.Allocator, out var ids, out _);

                    var next = 0;
                    var bytes = MeasureRemoval(() => references.RemoveReferences(ids[next++], Collection, tx));
                    Assert.Equal((0, 0), references.GetReferenceTablesCount(Collection, tx));

                    Measure.Report("Tree operations of a removal", treeOperations, "doc");
                    Measure.Report("Remove references, over the tree operations", bytes - treeOperations, "doc");
                }
            }
        }

        public void ReadMapEntriesOfADocument()
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
                    var treeIteration = Measure.PerItem(() =>
                    {
                        for (int i = 0; i < NumberOfDocuments; i++)
                        {
                            work.DocumentMapEntries.RepurposeInstance(ids[i], clone: false);
                            expected += IterateWithFixedSizeTree(work.DocumentMapEntries);
                        }
                    }, NumberOfDocuments);

                    var bytes = Measure.PerItem(() =>
                    {
                        for (int i = 0; i < NumberOfDocuments; i++)
                        {
                            work.DocumentMapEntries.RepurposeInstance(ids[i], clone: false);
                            MapReduceIndexBase<AutoMapReduceIndexDefinition, AutoIndexField>.GetMapEntries(work.DocumentMapEntries, work.MapEntries);
                            foreach (var entry in work.MapEntries)
                                actual += entry.ReduceKeyHash + (ulong)entry.Id;
                        }
                    }, NumberOfDocuments);

                    Assert.Equal(expected, actual);
                    Measure.Report("Tree iteration of the map entries", treeIteration, "doc");
                    Measure.Report("Read map entries, over the tree iteration", bytes - treeIteration, "doc");
                }
            }
        }

        public void CreateResultsStoresOfABatch()
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
                    var bytes = Measure.PerItem(() =>
                    {
                        for (int i = 0; i < NumberOfDocuments; i++)
                        {
                            using (var store = index.CreateResultsStore(work.ResultsStoreTypes, ReduceKeyHash(i), context, create: true))
                            {
                                if (store.Type == MapResultsStorageType.Nested)
                                    nested++;
                            }
                        }
                    }, NumberOfDocuments);

                    Assert.Equal(4 * NumberOfDocuments, nested);
                    Measure.Report("Nested results store", bytes, "doc");
                }

                double treeOpen;
                using (var tx = context.OpenWriteTransaction())
                {
                    var names = new Slice[NumberOfDocuments];
                    for (int i = 0; i < NumberOfDocuments; i++)
                        Slice.From(tx.InnerTransaction.Allocator, MapReduceResultsStore.ReduceTreePrefix + TreeReduceKeyHash(i), ByteStringType.Immutable, out names[i]);

                    int trees = 0;
                    treeOpen = (double)Measure.Once(() =>
                    {
                        for (int i = 0; i < NumberOfDocuments; i++)
                        {
                            if (tx.InnerTransaction.ReadTree(names[i]) != null)
                                trees++;
                        }
                    }) / NumberOfDocuments;

                    Assert.Equal(NumberOfDocuments, trees);
                }

                using (var tx = context.OpenWriteTransaction())
                using (index.InitializeIndexingWork(context))
                {
                    // every batch opens the tree of a store in a new transaction
                    int trees = 0;
                    var bytes = (double)Measure.Once(() =>
                    {
                        for (int i = 0; i < NumberOfDocuments; i++)
                        {
                            using (var store = index.CreateResultsStore(work.ResultsStoreTypes, TreeReduceKeyHash(i), context, create: false))
                            {
                                if (store.Type == MapResultsStorageType.Tree && store.Tree != null)
                                    trees++;
                            }
                        }
                    }) / NumberOfDocuments;

                    Assert.Equal(NumberOfDocuments, trees);
                    Measure.Report("Open of a reduce tree", treeOpen, "doc");
                    Measure.Report("Tree results store, over the open of its tree", bytes - treeOpen, "doc");
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

        // each document can be removed once, so the first ones warm up and the rest are measured
        private static double MeasureRemoval(Action removeNext) => Measure.PerOperation(removeNext, operations: NumberOfDocuments - 100, warmup: 100);
    }
}
