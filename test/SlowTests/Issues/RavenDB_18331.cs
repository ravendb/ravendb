using System;
using System.Collections.Generic;
using System.Linq;
using System.Threading;
using FastTests;
using Raven.Client;
using Raven.Client.Documents.Indexes;
using Raven.Server.Documents;
using Raven.Server.Documents.Indexes;
using Raven.Server.Documents.Indexes.MapReduce.Exceptions;
using Raven.Server.Documents.Indexes.MapReduce.Static;
using Raven.Server.ServerWide.Context;
using Sparrow.Json.Parsing;
using Xunit;
using Xunit.Abstractions;

namespace SlowTests.Issues;

public class RavenDB_18331 : RavenLowLevelTestBase
{
    public RavenDB_18331(ITestOutputHelper output) : base(output)
    {
    }

    [Fact]
    public void ErrorShouldIncludeTheActualItemAndReduceKey()
    {
        var numberOfDocs = 100;

        using (var database = CreateDocumentDatabase())
        {
            using (var index = MapReduceIndex.CreateNew<MapReduceIndex>(new IndexDefinition()
            {
                Name = "Users_DivideByZero",
                Maps = { @"from order in docs.Orders
from line in order.Lines
select new { Product = line.Product, FakeValue = 0 }" },
                Reduce = @"from result in mapResults
group result by result.Product into g
select new
{
    Product = g.Key,
    FakeValue = (long) (128 / g.Sum(x=> x.Total) - g.Sum(x=> x.Count))
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

                    var stats = new IndexingRunStats();
                    var scope = new IndexingStatsScope(stats);

                    var cts = new CancellationTokenSource(TimeSpan.FromSeconds(60));

                    try
                    {
                        index.DoIndexingWork(scope, cts.Token);
                    }
                    catch (ExcessiveNumberOfReduceErrorsException)
                    {
                        // expected
                    }

                    List<IndexingError> indexingErrors = stats.Errors;

                    Assert.Equal(1, indexingErrors.Count);
                    Assert.Contains(@"current item to reduce: {""Product"":""Milk"",""FakeValue"":0}", indexingErrors.First().Error);
                    Assert.Equal(@"Reduce key: { 'Product' : Milk }", indexingErrors.First().Document);
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
                    ["Price"] = 1
                },
                new DynamicJsonValue
                {
                    ["Product"] = "Bread",
                    ["Price"] = 1
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
