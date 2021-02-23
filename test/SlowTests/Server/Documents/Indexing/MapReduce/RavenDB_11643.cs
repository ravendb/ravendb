using System;
using System.Threading;
using FastTests;
using Raven.Client.Documents.Indexes;
using Raven.Server.Documents.Indexes;
using Raven.Server.Documents.Indexes.MapReduce;
using Raven.Server.Documents.Indexes.MapReduce.Static;
using Raven.Server.Documents.Indexes.Persistence.Lucene;
using Raven.Server.Documents.Indexes.Static;
using Raven.Server.ServerWide.Context;
using Raven.Server.Utils;
using Sparrow.Binary;
using Sparrow.Json.Parsing;
using Sparrow.Platform;
using Sparrow.Utils;
using Voron;
using Voron.Data.Tables;
using Xunit;
using Xunit.Abstractions;

namespace SlowTests.Server.Documents.Indexing.MapReduce
{
    public class RavenDB_11643 : RavenLowLevelTestBase
    {
        public RavenDB_11643(ITestOutputHelper output) : base(output)
        {
        }

        [Fact]
        public void PageModificationInAnyTreeMustRemoveItFromListOfFreedPagesInAllStores()
        {
            using (var database = CreateDocumentDatabase())
            using (var index = MapReduceIndex.CreateNew<MapReduceIndex>(new IndexDefinition()
            {
                Name = "Users_ByCount_GroupByLocation",
                Maps = { "from user in docs.Users select new { user.Location, Count = 1 }" },
                Reduce =
                    "from result in results group result by result.Location into g select new { Location = g.Key, Count = g.Sum(x => x.Count) }",
                Type = IndexType.MapReduce,
                Fields =
                {
                    {"Location", new IndexFieldOptions {Storage = FieldStorage.Yes}},
                    {"Count", new IndexFieldOptions {Storage = FieldStorage.Yes}}
                }
            }, database))
            {
                index._threadAllocations = NativeMemory.CurrentThreadStats;

                var mapReduceContext = new MapReduceIndexingContext();
                using (var contextPool = new TransactionContextPool(database.DocumentsStorage.Environment))
                {
                    var indexStorage = new IndexStorage(index, contextPool, database);
                    var reducer = new ReduceMapResultsOfStaticIndex(index, index._compiled.Reduce, index.Definition, indexStorage, new MetricCounters(), mapReduceContext);

                    using (index._contextPool.AllocateOperationContext(out TransactionOperationContext indexContext))
                    {
                        using (var tx = indexContext.OpenWriteTransaction())
                        {
                            mapReduceContext.MapPhaseTree = tx.InnerTransaction.CreateTree(MapReduceIndexBase<MapIndexDefinition, IndexField>.MapPhaseTreeName);
                            mapReduceContext.ReducePhaseTree = tx.InnerTransaction.CreateTree(MapReduceIndexBase<MapIndexDefinition, IndexField>.ReducePhaseTreeName);

                            var store1 = new MapReduceResultsStore(1, MapResultsStorageType.Tree, indexContext, mapReduceContext, true);
                            var store2 = new MapReduceResultsStore(2, MapResultsStorageType.Tree, indexContext, mapReduceContext, true);

                            mapReduceContext.StoreByReduceKeyHash.Add(1, store1);
                            mapReduceContext.StoreByReduceKeyHash.Add(2, store2);

                            // we're cheating here a bit as the originally this issue was reproduced on very large amount of data
                            //
                            // we choose page 542 because it's going to be used when calling store1.Add() below
                            // let's pretend this page that was freed as the result of deletion in store2
                            // the important thing is that store1 will be processed before processing store2 and we'll store the aggregation result for page 542 in PageNumberToReduceResult table
                            // the issue was that modification of page 542 in the tree of store1 didn't remove it from FreedPages of store2
                            // in result the processing of store2 removed page 542 from the table

                            long pageNumber = 544;

                            if (tx.InnerTransaction.LowLevelTransaction.Environment.Options.ForceUsing32BitsPager || PlatformDetails.Is32Bits)
                            {
                                // in 32 bits we might allocate different pages, 94 is going to be used during store1.Add() calls
                                pageNumber = 96;
                            }

                            mapReduceContext.FreedPages.Add(pageNumber);

                            for (int i = 0; i < 200; i++)
                            {
                                using (var mappedResult = indexContext.ReadObject(new DynamicJsonValue
                                {
                                    ["Count"] = 1,
                                    ["Location"] = new string('c', 1024)
                                }, $"users/{i}"))
                                {
                                    store1.Add(i, mappedResult);
                                }
                            }
                            
                            var writeOperation = new Lazy<IndexWriteOperation>(() => index.IndexPersistence.OpenIndexWriter(tx.InnerTransaction, null));

                            try
                            {
                                var stats = new IndexingStatsScope(new IndexingRunStats());
                                reducer.Execute(null, indexContext,
                                    writeOperation,
                                    stats, CancellationToken.None);

                                Assert.DoesNotContain(pageNumber, mapReduceContext.FreedPages);

                                var table = indexContext.Transaction.InnerTransaction.OpenTable(ReduceMapResultsBase<MapReduceIndexDefinition>.ReduceResultsSchema,
                                    ReduceMapResultsBase<MapReduceIndexDefinition>.PageNumberToReduceResultTableName);

                                var page = Bits.SwapBytes(pageNumber);

                                unsafe
                                {
                                    using (Slice.External(indexContext.Allocator, (byte*)&page, sizeof(long), out Slice pageSlice))
                                    {
                                        Assert.True(table.ReadByKey(pageSlice, out TableValueReader tvr));
                                    }
                                }
                            }
                            finally
                            {
                                if (writeOperation.IsValueCreated)
                                    writeOperation.Value.Dispose();
                            }
                        }
                    }
                }
            }
        }
    }
}
