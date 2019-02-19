using System.Linq;
using System.Threading;
using System.Threading.Tasks;
using Raven.Client;
using Raven.Client.Documents.Indexes;
using Raven.Server.Documents.Indexes;
using Raven.Server.Documents.Indexes.MapReduce.Static;
using Raven.Server.Documents.Queries;
using Raven.Server.ServerWide;
using Raven.Server.ServerWide.Context;
using Sparrow.Json;
using Sparrow.Json.Parsing;
using Xunit;

namespace FastTests.Server.Documents.Indexing.Static
{
    public class BasicStaticMapReduceIndexing : RavenLowLevelTestBase
    {
        [Fact]
        public async Task The_simpliest_static_map_reduce_index()
        {
            using (var database = CreateDocumentDatabase())
            {
                using (var index = MapReduceIndex.CreateNew(new IndexDefinition()
                {
                    Name = "Users_ByCount_GroupByLocation",
                    Maps = { @"from user in docs.Users select new { 
                                user.Location, 
                                CountInteger = 1, 
                                CountDouble = 1.0,
                                CastedInteger = 1
                            }" },
                    Reduce = @"from result in results group result by result.Location into g select new { 
                                Location = g.Key, 
                                CountInteger = g.Sum(x => x.CountInteger), 
                                CountDouble = g.Sum(x => x.CountDouble),
                                CastedInteger = g.Sum(x => (int)x.CastedInteger) 
                            }"
                }, database))
                {
                    DocumentQueryResult queryResult;
                    using (var context = DocumentsOperationContext.ShortTermSingleUse(database))
                    {
                        using (var tx = context.OpenWriteTransaction())
                        {
                            using (var doc = CreateDocument(context, "users/1", new DynamicJsonValue
                            {
                                ["Location"] = "Poland",
                                [Constants.Documents.Metadata.Key] = new DynamicJsonValue
                                {
                                    [Constants.Documents.Metadata.Collection] = "Users"
                                }
                            }))
                            {
                                database.DocumentsStorage.Put(context, "users/1", null, doc);
                            }

                            using (var doc = CreateDocument(context, "users/2", new DynamicJsonValue
                            {
                                ["Location"] = "Poland",
                                [Constants.Documents.Metadata.Key] = new DynamicJsonValue
                                {
                                    [Constants.Documents.Metadata.Collection] = "Users"
                                }
                            }))
                            {
                                database.DocumentsStorage.Put(context, "users/2", null, doc);
                            }

                            tx.Commit();
                        }

                        var batchStats = new IndexingRunStats();
                        var scope = new IndexingStatsScope(batchStats);
                        index.DoIndexingWork(scope, CancellationToken.None);

                        Assert.Equal(2, batchStats.MapAttempts);
                        Assert.Equal(2, batchStats.MapSuccesses);
                        Assert.Equal(0, batchStats.MapErrors);

                        Assert.Equal(2, batchStats.ReduceAttempts);
                        Assert.Equal(2, batchStats.ReduceSuccesses);
                        Assert.Equal(0, batchStats.ReduceErrors);

                        queryResult =
                            await index.Query(new IndexQueryServerSide($"FROM INDEX '{index.Name}'"), context, OperationCancelToken.None);

                        Assert.Equal(1, queryResult.Results.Count);

                    }
                    using (var context = DocumentsOperationContext.ShortTermSingleUse(database))
                    {

                        queryResult = await index.Query(new IndexQueryServerSide($"FROM INDEX '{index.Name}' WHERE Location = 'Poland'"), context, OperationCancelToken.None);

                        var results = queryResult.Results;

                        Assert.Equal(1, results.Count);

                        Assert.Equal(1, queryResult.Results.Count);
                        Assert.Equal("Poland", results[0].Data["Location"].ToString());
                        Assert.Equal(2L, results[0].Data["CountInteger"]);
                        Assert.Equal(2.0, (LazyNumberValue)results[0].Data["CountDouble"]);
                        Assert.Equal(2L, results[0].Data["CastedInteger"]);
                    }
                }
            }
        }

        [Fact]
        public async Task CanPersist()
        {
            IndexDefinition defOne, defTwo;
            string dbName;

            using (CreatePersistentDocumentDatabase(NewDataPath(), out var database))
            {
                dbName = database.Name;

                defOne = new IndexDefinition
                {
                    Name = "Users_ByCount_GroupByLocation",
                    Maps = { "from user in docs.Users select new { user.Location, Count = 1 }" },
                    Reduce = "from result in results group result by result.Location into g select new { Location = g.Key, Count = g.Sum(x => x.Count) }",
                };

                var index = await database.IndexStore.CreateIndex(defOne);

                defTwo = new IndexDefinition()
                {
                    Name = "Orders_ByCount_GroupByProduct",
                    Maps = { @"from order in docs.Orders
from line in order.Lines
select new { Product = line.Product, Count = 1, Total = line.Price }" },
                    Reduce = @"from result in results
group result by result.Product into g
select new
{
    Product = g.Key,
    Count = g.Sum(x=> x.Count),
    Total = g.Sum(x=> x.Total)
}",
                    Fields =
                    {
                        { "Product", new IndexFieldOptions { Indexing = FieldIndexing.Search} }
                    },
                    LockMode = IndexLockMode.LockedError
                };

                await database.IndexStore.CreateIndex(defTwo);

                using (var context = DocumentsOperationContext.ShortTermSingleUse(database))
                {
                    using (var tx = context.OpenWriteTransaction())
                    {
                        using (var doc = CreateDocument(context, "users/1", new DynamicJsonValue
                        {
                            ["Location"] = "Poland",
                            [Constants.Documents.Metadata.Key] = new DynamicJsonValue
                            {
                                [Constants.Documents.Metadata.Collection] = "Users"
                            }
                        }))
                        {
                            database.DocumentsStorage.Put(context, "users/1", null, doc);
                        }
                        tx.Commit();
                    }

                    index.DoIndexingWork(new IndexingStatsScope(new IndexingRunStats()), CancellationToken.None);
                }

                Server.ServerStore.DatabasesLandlord.UnloadDirectly(dbName);

                database = await GetDatabase(dbName);

                var indexes = database
                    .IndexStore
                    .GetIndexes()
                    .OfType<MapReduceIndex>()
                    .OrderByDescending(x => x.Name)
                    .ToList();

                Assert.Equal(IndexType.MapReduce, indexes[0].Type);
                Assert.Equal("Users_ByCount_GroupByLocation", indexes[0].Name);
                Assert.Equal(1, indexes[0].Definition.Collections.Count);
                Assert.Equal("Users", indexes[0].Definition.Collections.Single());
                Assert.Equal(2, indexes[0].Definition.MapFields.Count);
                Assert.Contains("Location", indexes[0].Definition.MapFields.Keys);
                Assert.Contains("Count", indexes[0].Definition.MapFields.Keys);
                Assert.Equal(IndexLockMode.Unlock, indexes[0].Definition.LockMode);
                Assert.Equal(IndexPriority.Normal, indexes[0].Definition.Priority);
                Assert.True(defOne.Equals(indexes[0].GetIndexDefinition()));
                Assert.Equal(1, indexes[0].MapReduceWorkContext.NextMapResultId);

                Assert.Equal(IndexType.MapReduce, indexes[1].Type);
                Assert.Equal("Orders_ByCount_GroupByProduct", indexes[1].Name);
                Assert.Equal(1, indexes[1].Definition.Collections.Count);
                Assert.Equal("Orders", indexes[1].Definition.Collections.Single());
                Assert.Equal(3, indexes[1].Definition.MapFields.Count);
                Assert.Contains("Product", indexes[1].Definition.MapFields.Keys);
                Assert.Equal(FieldIndexing.Search, indexes[1].Definition.MapFields["Product"].As<IndexField>().Indexing);
                Assert.Contains("Count", indexes[1].Definition.MapFields.Keys);
                Assert.Contains("Total", indexes[1].Definition.MapFields.Keys);
                Assert.Equal(IndexLockMode.LockedError, indexes[1].Definition.LockMode);
                Assert.Equal(IndexPriority.Normal, indexes[1].Definition.Priority);
                Assert.True(defTwo.Equals(indexes[1].GetIndexDefinition()));
                Assert.Equal(0, indexes[1].MapReduceWorkContext.NextMapResultId);
            }
        }
    }
}
