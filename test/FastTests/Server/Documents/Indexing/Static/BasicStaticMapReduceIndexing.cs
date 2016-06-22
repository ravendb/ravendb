using System.Threading;
using System.Threading.Tasks;
using Raven.Abstractions.Data;
using Raven.Client.Data;
using Raven.Client.Data.Indexes;
using Raven.Client.Indexing;
using Raven.Server.Documents.Indexes;
using Raven.Server.Documents.Indexes.MapReduce.Static;
using Raven.Server.ServerWide;
using Raven.Server.ServerWide.Context;
using Sparrow.Json;
using Sparrow.Json.Parsing;
using Xunit;

namespace FastTests.Server.Documents.Indexing.Static
{
    public class BasicStaticMapReduceIndexing : RavenLowLevelTestBase
    {
        [Fact(Skip = "TODO arek")]
        public async Task The_simpliest_static_map_reduce_index()
        {
            using (var database = CreateDocumentDatabase())
            {
                using (var index = MapReduceIndex.CreateNew(1, new IndexDefinition()
                {
                    Name = "Users_ByCount_GroupByLocation",
                    Maps = { "from user in docs.Users select new { user.Location, Count = 1 }" },
                    //Reduce = "from result in results group result by result.Location into g select new { Location = g.Key, Count = g.Sum(x => x.Count) }",
                    Reduce = "results.GroupBy(x => x.City).Select(g => new { City = g.Key, Count = g.Sum(x => x.Count) })",
                    Type = IndexType.MapReduce
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

                        var queryResult = await index.Query(new IndexQuery(), context, OperationCancelToken.None);

                        Assert.Equal(2, queryResult.Results.Count);

                        context.Reset();

                        queryResult = await index.Query(new IndexQuery() { Query = "Location:Poland" }, context, OperationCancelToken.None);

                        Assert.Equal(1, queryResult.Results.Count);
                        Assert.Equal("users/1", queryResult.Results[0].Key);
                    }
                }
            }
        }

        // TODO arek - index definition persistance test

        [Fact(Skip = "TODO arek")]
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
    Count = g.Sum(x=>x.Count),
    Total = g.Sum(x=>x.Total)
}",
                    Type = IndexType.MapReduce
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
                                        ["Product"] = "a",
                                        ["Price"] = 10.5
                                    },
                                    new DynamicJsonValue
                                    {
                                        ["Product"] = "b",
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
                                        ["Product"] = "a",
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

                        Assert.Equal(2, batchStats.ReduceAttempts);
                        Assert.Equal(2, batchStats.ReduceSuccesses);
                        Assert.Equal(0, batchStats.ReduceErrors);

                        var queryResult = await index.Query(new IndexQuery(), context, OperationCancelToken.None);

                        Assert.Equal(2, queryResult.Results.Count);

                        context.Reset();

                        queryResult = await index.Query(new IndexQuery() { Query = "Location:Poland" }, context, OperationCancelToken.None);

                        Assert.Equal(1, queryResult.Results.Count);
                        Assert.Equal("users/1", queryResult.Results[0].Key);
                    }
                }
            }
        }
    }
}