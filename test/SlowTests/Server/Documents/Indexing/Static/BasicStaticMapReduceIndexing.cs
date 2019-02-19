using System.Threading;
using System.Threading.Tasks;
using FastTests;
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

namespace SlowTests.Server.Documents.Indexing.Static
{
    public class BasicStaticMapReduceIndexing : RavenLowLevelTestBase
    {
        [Fact]
        public async Task Static_map_reduce_index_with_multiple_outputs_per_document()
        {
            using (var database = CreateDocumentDatabase())
            {
                using (var index = MapReduceIndex.CreateNew(new IndexDefinition()
                {
                    Name = "Users_ByCount_GroupByLocation",
                    Maps = { @"from order in docs.Orders
from line in order.Lines
select new { Product = line.Product, Count = 1, Total = line.Price }" },
                    Reduce = @"from result in mapResults
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
                    DocumentQueryResult queryResult;
                    using (var context = DocumentsOperationContext.ShortTermSingleUse(database))
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
                                [Constants.Documents.Metadata.Key] = new DynamicJsonValue
                                {
                                    [Constants.Documents.Metadata.Collection] = "Orders"
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
                                [Constants.Documents.Metadata.Key] = new DynamicJsonValue
                                {
                                    [Constants.Documents.Metadata.Collection] = "Orders"
                                }
                            }))
                            {
                                database.DocumentsStorage.Put(context, "orders/2", null, doc);
                            }

                            tx.Commit();
                        }

                        var batchStats = new IndexingRunStats();
                        var scope = new IndexingStatsScope(batchStats);
                        while (index.DoIndexingWork(scope, CancellationToken.None))
                        {

                        }

                        queryResult = await index.Query(new IndexQueryServerSide($"FROM INDEX '{index.Name}'"), context, OperationCancelToken.None);

                        Assert.Equal(2, queryResult.Results.Count);

                    }
                    using (var context = DocumentsOperationContext.ShortTermSingleUse(database))
                    {
                        queryResult = await index.Query(new IndexQueryServerSide($"FROM INDEX '{index.Name}' WHERE Product = 'Milk'"), context, OperationCancelToken.None);

                        Assert.Equal(1, queryResult.Results.Count);
                        Assert.Equal("Milk", queryResult.Results[0].Data["Product"].ToString());
                        Assert.Equal(2L, queryResult.Results[0].Data["Count"]);
                        Assert.Equal(21.0, (LazyNumberValue)queryResult.Results[0].Data["Total"]);
                    }
                }
            }
        }
    }
}
