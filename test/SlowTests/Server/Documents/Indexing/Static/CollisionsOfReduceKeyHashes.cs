using System;
using System.Threading;
using System.Threading.Tasks;
using FastTests;
using Raven.Client.Documents.Indexes;
using Raven.Server.Documents;
using Raven.Server.Documents.Indexes;
using Raven.Server.Documents.Indexes.MapReduce;
using Raven.Server.Documents.Indexes.MapReduce.Auto;
using Raven.Server.Documents.Indexes.MapReduce.Static;
using Raven.Server.Documents.Indexes.Persistence.Lucene;
using Raven.Server.Documents.Indexes.Static;
using Raven.Server.Documents.Indexes.Workers;
using Raven.Server.Documents.Queries;
using Raven.Server.ServerWide;
using Raven.Server.ServerWide.Context;
using Raven.Server.Utils;
using Sparrow.Json.Parsing;
using Sparrow.Utils;
using Xunit;

namespace SlowTests.Server.Documents.Indexing.Static
{
    public class CollisionsOfReduceKeyHashes : RavenLowLevelTestBase
    {
        [Theory]
        [InlineData(5, new[] { "Israel", "Poland" })]
        [InlineData(100, new[] { "Israel", "Poland", "USA" })]
        public async Task Auto_index_should_produce_multiple_outputs(int numberOfUsers, string[] locations)
        {
            using (var database = CreateDocumentDatabase())
            using (var index = AutoMapReduceIndex.CreateNew(new AutoMapReduceIndexDefinition("Users", new[]
            {
                new AutoIndexField
                {
                    Name = "Count",
                    Aggregation = AggregationOperation.Count,
                    Storage = FieldStorage.Yes
                }
            }, new[]
            {
                new AutoIndexField
                {
                    Name = "Location",
                    Storage = FieldStorage.Yes
                },
            }), database))
            {
                index._threadAllocations = NativeMemory.CurrentThreadStats;

                var mapReduceContext = new MapReduceIndexingContext();
                using (var contextPool = new TransactionContextPool(database.DocumentsStorage.Environment))
                {
                    var indexStorage = new IndexStorage(index, contextPool, database);

                    var reducer = new ReduceMapResultsOfAutoIndex(index, index.Definition, indexStorage,
                        new MetricCounters(), mapReduceContext);

                    await ActualTest(numberOfUsers, locations, index, mapReduceContext, reducer, database);
                }
            }
        }

        [Theory]
        [InlineData(5, new[] { "Israel", "Poland" })]
        [InlineData(100, new[] { "Israel", "Poland", "USA" })]
        public async Task Static_index_should_produce_multiple_outputs(int numberOfUsers, string[] locations)
        {
            using (var database = CreateDocumentDatabase())
            using (var index = MapReduceIndex.CreateNew(new IndexDefinition()
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

                    await ActualTest(numberOfUsers, locations, index, mapReduceContext, reducer, database);
                }
            }
        }

        private static async Task ActualTest(int numberOfUsers, string[] locations, Index index,
            MapReduceIndexingContext mapReduceContext, IIndexingWork reducer, DocumentDatabase database)
        {
            TransactionOperationContext indexContext;
            using (index._contextPool.AllocateOperationContext(out indexContext))
            {
                ulong hashOfReduceKey = 73493;

                using (var tx = indexContext.OpenWriteTransaction())
                {
                    mapReduceContext.MapPhaseTree = tx.InnerTransaction.CreateTree(MapReduceIndexBase<MapIndexDefinition, IndexField>.MapPhaseTreeName);
                    mapReduceContext.ReducePhaseTree = tx.InnerTransaction.CreateTree(MapReduceIndexBase<MapIndexDefinition, IndexField>.ReducePhaseTreeName);

                    var store = new MapReduceResultsStore(hashOfReduceKey, MapResultsStorageType.Tree, indexContext, mapReduceContext, true);

                    for (int i = 0; i < numberOfUsers; i++)
                    {
                        using (var mappedResult = indexContext.ReadObject(new DynamicJsonValue
                        {
                            ["Count"] = 1,
                            ["Location"] = locations[i % locations.Length]
                        }, $"users/{i}"))
                        {
                            store.Add(i, mappedResult);
                        }
                    }

                    mapReduceContext.StoreByReduceKeyHash.Add(hashOfReduceKey, store);

                    var writeOperation =
                        new Lazy<IndexWriteOperation>(() => index.IndexPersistence.OpenIndexWriter(tx.InnerTransaction, null));

                    var stats = new IndexingStatsScope(new IndexingRunStats());
                    reducer.Execute(null, indexContext,
                        writeOperation,
                        stats, CancellationToken.None);

                    using (var indexWriteOperation = writeOperation.Value)
                    {
                        indexWriteOperation.Commit(stats);
                    }

                    index.IndexPersistence.RecreateSearcher(tx.InnerTransaction);

                    mapReduceContext.Dispose();

                    tx.Commit();
                }

                using (var termSingleUse = DocumentsOperationContext.ShortTermSingleUse(database))
                {
                    var queryResult = await
                        index.Query(new IndexQueryServerSide($"FROM INDEX '{index.Name}'"),
                            termSingleUse,
                            OperationCancelToken.None);

                    var results = queryResult.Results;

                    Assert.Equal(locations.Length, results.Count);

                    for (int i = 0; i < locations.Length; i++)
                    {
                        Assert.Equal(locations[i], results[i].Data["Location"].ToString());

                        long expected = numberOfUsers / locations.Length + numberOfUsers % (locations.Length - i);
                        Assert.Equal(expected, results[i].Data["Count"]);
                    }
                }

                // update

                using (var tx = indexContext.OpenWriteTransaction())
                {
                    mapReduceContext.MapPhaseTree = tx.InnerTransaction.CreateTree(MapReduceIndexBase<MapIndexDefinition, IndexField>.MapPhaseTreeName);
                    mapReduceContext.ReducePhaseTree = tx.InnerTransaction.CreateTree(MapReduceIndexBase<MapIndexDefinition, IndexField>.ReducePhaseTreeName);

                    var store = new MapReduceResultsStore(hashOfReduceKey, MapResultsStorageType.Tree, indexContext, mapReduceContext, true);

                    for (int i = 0; i < locations.Length; i++)
                    {
                        using (var mappedResult = indexContext.ReadObject(new DynamicJsonValue
                        {
                            ["Count"] = 2, // increased by 1
                            ["Location"] = locations[i % locations.Length]
                        }, $"users/{i}"))
                        {
                            store.Add(i, mappedResult);
                        }
                    }

                    mapReduceContext.StoreByReduceKeyHash.Add(hashOfReduceKey, store);

                    var writeOperation =
                        new Lazy<IndexWriteOperation>(() => index.IndexPersistence.OpenIndexWriter(tx.InnerTransaction, null));
                    try
                    {

                        var stats = new IndexingStatsScope(new IndexingRunStats());
                        reducer.Execute(null, indexContext,
                            writeOperation,
                            stats, CancellationToken.None);

                        using (var indexWriteOperation = writeOperation.Value)
                        {
                            indexWriteOperation.Commit(stats);
                        }

                        index.IndexPersistence.RecreateSearcher(tx.InnerTransaction);

                        mapReduceContext.Dispose();
                    }
                    finally
                    {
                        if(writeOperation.IsValueCreated)
                            writeOperation.Value.Dispose();
                    }

                    tx.Commit();
                }

                using (var shortTermSingleUse = DocumentsOperationContext.ShortTermSingleUse(database))
                {
                    var queryResult = await index.Query(new IndexQueryServerSide($"FROM INDEX '{index.Name}'"),
                                shortTermSingleUse,
                                OperationCancelToken.None);


                    var results = queryResult.Results;

                    Assert.Equal(locations.Length, results.Count);

                    for (int i = 0; i < locations.Length; i++)
                    {
                        Assert.Equal(locations[i], results[i].Data["Location"].ToString());

                        long expected = numberOfUsers / locations.Length + numberOfUsers % (locations.Length - i);
                        Assert.Equal(expected + 1, results[i].Data["Count"]);
                    }

                }
                // delete

                using (var tx = indexContext.OpenWriteTransaction())
                {
                    mapReduceContext.MapPhaseTree = tx.InnerTransaction.CreateTree(MapReduceIndexBase<MapIndexDefinition, IndexField>.MapPhaseTreeName);
                    mapReduceContext.ReducePhaseTree = tx.InnerTransaction.CreateTree(MapReduceIndexBase<MapIndexDefinition, IndexField>.ReducePhaseTreeName);

                    var store = new MapReduceResultsStore(hashOfReduceKey, MapResultsStorageType.Tree, indexContext, mapReduceContext, true);

                    for (int i = 0; i < locations.Length; i++)
                    {
                        store.Delete(i);
                    }

                    mapReduceContext.StoreByReduceKeyHash.Add(hashOfReduceKey, store);

                    var writeOperation =
                        new Lazy<IndexWriteOperation>(() => index.IndexPersistence.OpenIndexWriter(tx.InnerTransaction, null));
                    try
                    {
                        var stats = new IndexingStatsScope(new IndexingRunStats());
                        reducer.Execute(null, indexContext,
                            writeOperation,
                            stats, CancellationToken.None);

                        using (var indexWriteOperation = writeOperation.Value)
                        {
                            indexWriteOperation.Commit(stats);
                        }

                        index.IndexPersistence.RecreateSearcher(tx.InnerTransaction);

                        tx.Commit();
                    }
                    finally
                    {
                        if(writeOperation.IsValueCreated)
                            writeOperation.Value.Dispose();
                    }
                }

                using (var documentsOperationContext = DocumentsOperationContext.ShortTermSingleUse(database))
                {
                    var queryResult = await index.Query(new IndexQueryServerSide("FROM Users ORDER BY Location"),
                         documentsOperationContext,
                         OperationCancelToken.None);


                    var results = queryResult.Results;

                    Assert.Equal(locations.Length, results.Count);

                    for (int i = 0; i < locations.Length; i++)
                    {
                        Assert.Equal(locations[i], results[i].Data["Location"].ToString());

                        long expected = numberOfUsers / locations.Length + numberOfUsers % (locations.Length - i);
                        Assert.Equal(expected - 1, results[i].Data["Count"]);
                    }
                }
            }
        }
    }
}
