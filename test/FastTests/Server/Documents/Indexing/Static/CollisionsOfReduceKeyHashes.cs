using System;
using System.Threading;
using System.Threading.Tasks;
using Raven.Abstractions.Data;
using Raven.Abstractions.Indexing;
using Raven.Client.Data;
using Raven.Client.Data.Indexes;
using Raven.Client.Indexing;
using Raven.Server.Documents;
using Raven.Server.Documents.Indexes;
using Raven.Server.Documents.Indexes.MapReduce;
using Raven.Server.Documents.Indexes.MapReduce.Auto;
using Raven.Server.Documents.Indexes.MapReduce.Static;
using Raven.Server.Documents.Indexes.Persistence.Lucene;
using Raven.Server.Documents.Indexes.Workers;
using Raven.Server.ServerWide;
using Raven.Server.ServerWide.Context;
using Raven.Server.Utils;
using Raven.Server.Utils.Metrics;
using Sparrow.Json;
using Sparrow.Json.Parsing;
using Xunit;

namespace FastTests.Server.Documents.Indexing.Static
{
    public class CollisionsOfReduceKeyHashes : RavenLowLevelTestBase
    {
        [Theory]
        [InlineData(5, new[] { "Israel", "Poland" })]
        [InlineData(100, new[] { "Israel", "Poland", "USA" })]
        [InlineData(50000, new[] { "Canada", "France" })] // reduce key tree with depth 3
        public async Task Auto_index_should_produce_multiple_outputs(int numberOfUsers, string[] locations)
        {
            using (var database = CreateDocumentDatabase())
            {
                var index = AutoMapReduceIndex.CreateNew(1, new AutoMapReduceIndexDefinition(new[] { "Users" }, new[]
                {
                    new IndexField
                    {
                        Name = "Count",
                        MapReduceOperation = FieldMapReduceOperation.Count,
                        Storage = FieldStorage.Yes
                    }
                }, new[]
                {
                    new IndexField
                    {
                        Name = "Location",
                        Storage = FieldStorage.Yes
                    },
                }), database);

                var mapReduceContext = new MapReduceIndexingContext();

                var reducer = new ReduceMapResultsOfAutoIndex(index.Definition, null, new MetricsCountersManager(new MetricsScheduler()), mapReduceContext);

                await ActualTest(numberOfUsers, locations, index, mapReduceContext, reducer, database);
            }
        }

        [Theory]
        [InlineData(5, new[] { "Israel", "Poland" })]
        [InlineData(100, new[] { "Israel", "Poland", "USA" })]
        [InlineData(50000, new[] { "Canada", "France" })] // reduce key tree with depth 3
        public async Task Static_index_should_produce_multiple_outputs(int numberOfUsers, string[] locations)
        {
            using (var database = CreateDocumentDatabase())
            {
                var index = MapReduceIndex.CreateNew(1, new IndexDefinition()
                {
                    Name = "Users_ByCount_GroupByLocation",
                    Maps = { "from user in docs.Users select new { user.Location, Count = 1 }" },
                    Reduce =
                        "from result in results group result by result.Location into g select new { Location = g.Key, Count = g.Sum(x => (int) x.Count) }",
                    Type = IndexType.MapReduce,
                    Fields =
                    {
                        {"Location", new IndexFieldOptions {Storage = FieldStorage.Yes}},
                        {"Count", new IndexFieldOptions {Storage = FieldStorage.Yes, Sort = SortOptions.NumericDefault}}
                    }
                }, database);

                var mapReduceContext = new MapReduceIndexingContext();

                var reducer = new ReduceMapResultsOfStaticIndex(index._compiled.Reduce, index.Definition, null, new MetricsCountersManager(new MetricsScheduler()), mapReduceContext);

                await ActualTest(numberOfUsers, locations, index, mapReduceContext, reducer, database);
            }
        }

        private static async Task ActualTest(int numberOfUsers, string[] locations, Index index,
            MapReduceIndexingContext mapReduceContext, IIndexingWork reducer, DocumentDatabase database)
        {
            TransactionOperationContext indexContext;
            using (index._contextPool.AllocateOperationContext(out indexContext))
            using (indexContext)
            {
                ulong hashOfReduceKey = 73493;

                using (var tx = indexContext.OpenWriteTransaction())
                {
                    var tree = tx.InnerTransaction.CreateTree(hashOfReduceKey.ToString());

                    var state = new ReduceKeyState(tree);

                    unsafe
                    {
                        for (int i = 0; i < numberOfUsers; i++)
                        {
                            using (var mappedResult = indexContext.ReadObject(new DynamicJsonValue
                            {
                                ["Count"] = 1,
                                ["Location"] = locations[i%locations.Length]
                            }, $"users/{i}"))
                            {
                                mappedResult.CopyTo(tree.DirectAdd(i.ToString(), mappedResult.Size));
                            }
                        }
                    }

                    mapReduceContext.StateByReduceKeyHash.Add(hashOfReduceKey, state);

                    var writeOperation =
                        new Lazy<IndexWriteOperation>(() => index.IndexPersistence.OpenIndexWriter(tx.InnerTransaction));

                    reducer.Execute(null, indexContext,
                        writeOperation,
                        new IndexingStatsScope(new IndexingRunStats()), CancellationToken.None);

                    writeOperation.Value.Dispose();

                    index.IndexPersistence.RecreateSearcher();


                    tx.Commit();
                }

                var queryResult =
                    await
                        index.Query(new IndexQuery(),
                            new DocumentsOperationContext(new UnmanagedBuffersPool(string.Empty), database),
                            OperationCancelToken.None);

                var results = queryResult.Results;

                Assert.Equal(locations.Length, results.Count);

                for (int i = 0; i < locations.Length; i++)
                {
                    Assert.Equal(locations[i], results[i].Data["Location"].ToString());

                    double expected = numberOfUsers/locations.Length + numberOfUsers%(locations.Length - i);
                    Assert.Equal(expected, ((LazyDoubleValue)results[i].Data["Count"]));
                }

                // update

                mapReduceContext.Dispose();

                using (var tx = indexContext.OpenWriteTransaction())
                {
                    var tree = tx.InnerTransaction.CreateTree(hashOfReduceKey.ToString());

                    var state = new ReduceKeyState(tree);

                    unsafe
                    {
                        for (int i = 0; i < locations.Length; i++)
                        {
                            using (var mappedResult = indexContext.ReadObject(new DynamicJsonValue
                            {
                                ["Count"] = 2, // increased by 1
                                ["Location"] = locations[i % locations.Length]
                            }, $"users/{i}"))
                            {
                                mappedResult.CopyTo(tree.DirectAdd(i.ToString(), mappedResult.Size));
                            }
                        }
                    }

                    mapReduceContext.StateByReduceKeyHash.Add(hashOfReduceKey, state);

                    var writeOperation =
                        new Lazy<IndexWriteOperation>(() => index.IndexPersistence.OpenIndexWriter(tx.InnerTransaction));

                    reducer.Execute(null, indexContext,
                        writeOperation,
                        new IndexingStatsScope(new IndexingRunStats()), CancellationToken.None);

                    writeOperation.Value.Dispose();

                    index.IndexPersistence.RecreateSearcher();

                    tx.Commit();
                }

                queryResult = await index.Query(new IndexQuery(),
                            new DocumentsOperationContext(new UnmanagedBuffersPool(string.Empty), database),
                            OperationCancelToken.None);

                results = queryResult.Results;

                Assert.Equal(locations.Length, results.Count);

                for (int i = 0; i < locations.Length; i++)
                {
                    Assert.Equal(locations[i], results[i].Data["Location"].ToString());

                    double expected = numberOfUsers / locations.Length + numberOfUsers % (locations.Length - i);
                    Assert.Equal(expected + 1, ((LazyDoubleValue)results[i].Data["Count"]));
                }

                // delete

                mapReduceContext.Dispose();

                using (var tx = indexContext.OpenWriteTransaction())
                {
                    var tree = tx.InnerTransaction.CreateTree(hashOfReduceKey.ToString());

                    var state = new ReduceKeyState(tree);

                    for (int i = 0; i < locations.Length; i++)
                    {
                        tree.Delete(i.ToString());
                    }

                    mapReduceContext.StateByReduceKeyHash.Add(hashOfReduceKey, state);

                    var writeOperation =
                        new Lazy<IndexWriteOperation>(() => index.IndexPersistence.OpenIndexWriter(tx.InnerTransaction));

                    reducer.Execute(null, indexContext,
                        writeOperation,
                        new IndexingStatsScope(new IndexingRunStats()), CancellationToken.None);

                    writeOperation.Value.Dispose();

                    index.IndexPersistence.RecreateSearcher();

                    tx.Commit();
                }

                queryResult = await index.Query(new IndexQuery()
                    {
                        SortedFields = new[]
                        {
                            new SortedField("Location"),
                        }
                    },
                    new DocumentsOperationContext(new UnmanagedBuffersPool(string.Empty), database),
                    OperationCancelToken.None);

                results = queryResult.Results;

                Assert.Equal(locations.Length, results.Count);

                for (int i = 0; i < locations.Length; i++)
                {
                    Assert.Equal(locations[i], results[i].Data["Location"].ToString());

                    double expected = numberOfUsers / locations.Length + numberOfUsers % (locations.Length - i);
                    Assert.Equal(expected - 1, ((LazyDoubleValue)results[i].Data["Count"]));
                }
            }
        }
    }
}