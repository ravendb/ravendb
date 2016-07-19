using System.Linq;
using System.Threading;
using System.Threading.Tasks;
using Raven.Abstractions.Data;
using Raven.Abstractions.Indexing;
using Raven.Client.Data;
using Raven.Client.Data.Indexes;
using Raven.Client.Indexing;
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
                using (var index = MapReduceIndex.CreateNew(1, new IndexDefinition()
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
                            }",
                    // TODO arek Reduce = "results.GroupBy(x => x.City).Select(g => new { City = g.Key, Count = g.Sum(x => x.Count) })",
                }, database))
                {
                    using (var context = new DocumentsOperationContext(new UnmanagedBuffersPool(string.Empty), database))
                    {
                        using (var tx = context.OpenWriteTransaction())
                        {
                            using (var doc = CreateDocument(context, "users/1", new DynamicJsonValue
                            {
                                ["Location"] = "Poland",
                                [Constants.Metadata] = new DynamicJsonValue
                                {
                                    [Constants.Headers.RavenEntityName] = "Users"
                                }
                            }))
                            {
                                database.DocumentsStorage.Put(context, "users/1", null, doc);
                            }

                            using (var doc = CreateDocument(context, "users/2", new DynamicJsonValue
                            {
                                ["Location"] = "Poland",
                                [Constants.Metadata] = new DynamicJsonValue
                                {
                                    [Constants.Headers.RavenEntityName] = "Users"
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

                        var queryResult = await index.Query(new IndexQueryServerSide(), context, OperationCancelToken.None);

                        Assert.Equal(1, queryResult.Results.Count);

                        context.Reset();

                        queryResult = await index.Query(new IndexQueryServerSide() { Query = "Location:Poland" }, context, OperationCancelToken.None);

                        var results = queryResult.Results;

                        Assert.Equal(1, results.Count);
                        
                        Assert.Equal(1, queryResult.Results.Count);
                        Assert.Equal("Poland", results[0].Data["Location"].ToString());
                        Assert.Equal(2L, results[0].Data["CountInteger"]);
                        Assert.Equal(2.0, (LazyDoubleValue)results[0].Data["CountDouble"]);
                        Assert.Equal(2L, results[0].Data["CastedInteger"]);
                    }
                }
            }
        }

        [Fact]
        public async Task Static_map_reduce_index_with_multiple_outputs_per_document()
        {
            using (var database = CreateDocumentDatabase())
            {
                using (var index = MapReduceIndex.CreateNew(1, new IndexDefinition()
                {
                    Name = "Users_ByCount_GroupByLocation",
                    //Maps = { @"docs.Orders.SelectMany(x => x.Lines, (order, line) => new { Product = line.Product, Count = 1, Total = line.Price })" },

                    Maps = { @"from order in docs.Orders
from line in ((IEnumerable<dynamic>)order.Lines)
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
                        { "Product", new IndexFieldOptions { Storage = FieldStorage.Yes} }
                    }
                }, database))
                {
                    using (var context = new DocumentsOperationContext(new UnmanagedBuffersPool(string.Empty), database))
                    {
                        using (var tx = context.OpenWriteTransaction())
                        {
                            using (var doc = CreateDocument(context, "orders/1", new DynamicJsonValue
                            {
                                ["Lines"] = new DynamicJsonArray
                                {
                                    new DynamicJsonValue
                                    {
                                        ["Product"] = "Milk",
                                        ["Price"] = 10.5
                                    },
                                    new DynamicJsonValue
                                    {
                                        ["Product"] = "Bread",
                                        ["Price"] = 10.7
                                    }
                                },
                                [Constants.Metadata] = new DynamicJsonValue
                                {
                                    [Constants.Headers.RavenEntityName] = "Orders"
                                }
                            }))
                            {
                                database.DocumentsStorage.Put(context, "orders/1", null, doc);
                            }

                            using (var doc = CreateDocument(context, "orders/2", new DynamicJsonValue
                            {
                                ["Lines"] = new DynamicJsonArray
                                {
                                    new DynamicJsonValue
                                    {
                                        ["Product"] = "Milk",
                                        ["Price"] = 10.5
                                    }
                                },
                                [Constants.Metadata] = new DynamicJsonValue
                                {
                                    [Constants.Headers.RavenEntityName] = "Orders"
                                }
                            }))
                            {
                                database.DocumentsStorage.Put(context, "orders/2", null, doc);
                            }

                            tx.Commit();
                        }

                        var batchStats = new IndexingRunStats();
                        var scope = new IndexingStatsScope(batchStats);
                        index.DoIndexingWork(scope, CancellationToken.None);

                        Assert.Equal(2, batchStats.MapAttempts);
                        Assert.Equal(2, batchStats.MapSuccesses);
                        Assert.Equal(0, batchStats.MapErrors);

                        Assert.Equal(3, batchStats.ReduceAttempts);
                        Assert.Equal(3, batchStats.ReduceSuccesses);
                        Assert.Equal(0, batchStats.ReduceErrors);

                        var queryResult = await index.Query(new IndexQueryServerSide(), context, OperationCancelToken.None);

                        Assert.Equal(2, queryResult.Results.Count);

                        context.Reset();

                        queryResult = await index.Query(new IndexQueryServerSide { Query = "Product:Milk" }, context, OperationCancelToken.None);

                        Assert.Equal(1, queryResult.Results.Count);
                        Assert.Equal("Milk", queryResult.Results[0].Data["Product"].ToString());
                        Assert.Equal(2L, queryResult.Results[0].Data["Count"]);
                        Assert.Equal(21.0, (LazyDoubleValue)queryResult.Results[0].Data["Total"]);
                    }
                }
            }
        }

        [Fact]
        public void CanPersist()
        {
            var path = NewDataPath();
            IndexDefinition defOne, defTwo;

            using (var database = CreateDocumentDatabase(runInMemory: false, dataDirectory: path))
            {
                defOne = new IndexDefinition
                {
                    Name = "Users_ByCount_GroupByLocation",
                    Maps = {"from user in docs.Users select new { user.Location, Count = 1 }"},
                    Reduce = "from result in results group result by result.Location into g select new { Location = g.Key, Count = g.Sum(x => x.Count) }",
                };

                var index = database.IndexStore.GetIndex(database.IndexStore.CreateIndex(defOne));

                Assert.Equal(1, index.IndexId);

                defTwo = new IndexDefinition()
                {
                    Name = "Orders_ByCount_GroupByProduct",
                    Maps = { @"from order in docs.Orders
from line in ((IEnumerable<dynamic>)order.Lines)
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
                        { "Product", new IndexFieldOptions { Indexing = FieldIndexing.Analyzed} }
                    },
                    LockMode = IndexLockMode.SideBySide
                };
                Assert.Equal(2, database.IndexStore.CreateIndex(defTwo));

                using (var context = new DocumentsOperationContext(new UnmanagedBuffersPool(string.Empty), database))
                {
                    using (var tx = context.OpenWriteTransaction())
                    {
                        using (var doc = CreateDocument(context, "users/1", new DynamicJsonValue
                        {
                            ["Location"] = "Poland",
                            [Constants.Metadata] = new DynamicJsonValue
                            {
                                [Constants.Headers.RavenEntityName] = "Users"
                            }
                        }))
                        {
                            database.DocumentsStorage.Put(context, "users/1", null, doc);
                        }
                        tx.Commit();
                    }

                    index.DoIndexingWork(new IndexingStatsScope(new IndexingRunStats()), CancellationToken.None);
                }
            }

            using (var database = CreateDocumentDatabase(runInMemory: false, dataDirectory: path, modifyConfiguration: configuration => configuration.Core.ThrowIfAnyIndexOrTransformerCouldNotBeOpened = true))
            {
                var indexes = database
                    .IndexStore
                    .GetIndexes()
                    .OrderBy(x => x.IndexId)
                    .OfType<MapReduceIndex>()
                    .ToList();

                Assert.Equal(1, indexes[0].IndexId);
                Assert.Equal(IndexType.MapReduce, indexes[0].Type);
                Assert.Equal("Users_ByCount_GroupByLocation", indexes[0].Name);
                Assert.Equal(1, indexes[0].Definition.Collections.Length);
                Assert.Equal("Users", indexes[0].Definition.Collections[0]);
                Assert.Equal(2, indexes[0].Definition.MapFields.Count);
                Assert.Contains("Location", indexes[0].Definition.MapFields.Keys);
                Assert.Contains("Count", indexes[0].Definition.MapFields.Keys);
                Assert.Equal(IndexLockMode.Unlock, indexes[0].Definition.LockMode);
                Assert.Equal(IndexingPriority.Normal, indexes[0].Priority);
                Assert.True(indexes[0].Definition.Equals(defOne, ignoreFormatting: true, ignoreMaxIndexOutputs: false));
                Assert.True(defOne.Equals(indexes[0].GetIndexDefinition(), compareIndexIds: false, ignoreFormatting: false, ignoreMaxIndexOutput: false));
                Assert.Equal(0, indexes[0]._mapReduceWorkContext.LastMapResultId);

                Assert.Equal(2, indexes[1].IndexId);
                Assert.Equal(IndexType.MapReduce, indexes[1].Type);
                Assert.Equal("Orders_ByCount_GroupByProduct", indexes[1].Name);
                Assert.Equal(1, indexes[1].Definition.Collections.Length);
                Assert.Equal("Orders", indexes[1].Definition.Collections[0]);
                Assert.Equal(3, indexes[1].Definition.MapFields.Count);
                Assert.Contains("Product", indexes[1].Definition.MapFields.Keys);
                Assert.Equal(FieldIndexing.Analyzed, indexes[1].Definition.MapFields["Product"].Indexing);
                Assert.Contains("Count", indexes[1].Definition.MapFields.Keys);
                Assert.Contains("Total", indexes[1].Definition.MapFields.Keys);
                Assert.Equal(IndexLockMode.SideBySide, indexes[1].Definition.LockMode);
                Assert.Equal(IndexingPriority.Normal, indexes[1].Priority);
                Assert.True(indexes[1].Definition.Equals(defTwo, ignoreFormatting: true, ignoreMaxIndexOutputs: false));
                Assert.True(defTwo.Equals(indexes[1].GetIndexDefinition(), compareIndexIds: false, ignoreFormatting: false, ignoreMaxIndexOutput: false));
                Assert.Equal(-1, indexes[1]._mapReduceWorkContext.LastMapResultId);
            }
        }
    }
}