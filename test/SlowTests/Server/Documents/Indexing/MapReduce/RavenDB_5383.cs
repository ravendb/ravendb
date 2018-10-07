using System;
using System.Threading;
using FastTests;
using Raven.Client;
using Raven.Client.Documents.Indexes;
using Raven.Server.Documents;
using Raven.Server.Documents.Indexes;
using Raven.Server.Documents.Indexes.MapReduce.Static;
using Raven.Server.ServerWide.Context;
using Sparrow.Json.Parsing;
using Xunit;

namespace SlowTests.Server.Documents.Indexing.MapReduce
{
    public class RavenDB_5383 : RavenLowLevelTestBase
    {
        [Theory]
        [InlineData(1)]
        [InlineData(1000)]
        public void When_map_results_do_not_change_then_we_skip_the_reduce_phase(int numberOfDocs)
        {
            using (var database = CreateDocumentDatabase())
            {
                using (var index = MapReduceIndex.CreateNew(new IndexDefinition()
                {
                    Name = "Users_ByCount_GroupByProduct",
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
                }, database))
                {
                    using (var context = DocumentsOperationContext.ShortTermSingleUse(database))
                    {
                        for (int i = 0; i < numberOfDocs; i++)
                        {
                            var order = CreateOrder();
                            PutOrder(database, order, context, i);
                        }
                        
                        var firstRunStats = new IndexingRunStats();
                        var scope = new IndexingStatsScope(firstRunStats);


                        var cts = new CancellationTokenSource(TimeSpan.FromSeconds(60));
                        while (index.DoIndexingWork(scope, cts.Token));

                        Assert.Equal(numberOfDocs, firstRunStats.MapAttempts);
                        Assert.Equal(numberOfDocs, firstRunStats.MapSuccesses);
                        Assert.Equal(0, firstRunStats.MapErrors);

                        Assert.True(firstRunStats.ReduceAttempts > 0);
                        Assert.True(firstRunStats.ReduceSuccesses > 0);
                        Assert.Equal(0, firstRunStats.ReduceErrors);

                        for (int i = 0; i < numberOfDocs; i++)
                        {
                            var order = CreateOrder();
                            order["RefNumber"] = "456";
                            PutOrder(database, order, context, i);
                        }

                        var secondRunStats = new IndexingRunStats();
                        scope = new IndexingStatsScope(secondRunStats);
                        cts = new CancellationTokenSource(TimeSpan.FromSeconds(60));
                        while (index.DoIndexingWork(scope, cts.Token));
                        Assert.Equal(firstRunStats.MapAttempts, secondRunStats.MapAttempts);
                        Assert.Equal(firstRunStats.MapSuccesses, secondRunStats.MapSuccesses);
                        Assert.Equal(0, secondRunStats.MapErrors);

                        Assert.Equal(0, secondRunStats.ReduceAttempts);
                        Assert.Equal(0, secondRunStats.ReduceSuccesses);
                        Assert.Equal(0, secondRunStats.ReduceErrors);
                    }
                }
            }
        }

        private static DynamicJsonValue CreateOrder()
        {
            return new DynamicJsonValue
            {
                ["RefNumber"] = "123",
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
            };
        }

        private static void PutOrder(DocumentDatabase database, DynamicJsonValue dynamicOrder, DocumentsOperationContext context, int number)
        {
            using (var tx = context.OpenWriteTransaction())
            {
                using (var doc = CreateDocument(context, $"orders/{number}", dynamicOrder))
                {
                    database.DocumentsStorage.Put(context, $"orders/{number}", null, doc);
                }

                tx.Commit();
            }
        }
    }
}
