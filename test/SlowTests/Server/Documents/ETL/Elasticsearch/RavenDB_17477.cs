using System;
using System.Collections.Generic;
using System.Threading.Tasks;
using Orders;
using Raven.Client.Documents.Operations.ETL.ElasticSearch;
using Tests.Infrastructure;
using Xunit;
using Xunit.Abstractions;

namespace SlowTests.Server.Documents.ETL.ElasticSearch
{
    public class RavenDB_17477 : ElasticSearchEtlTestBase
    {
        public RavenDB_17477(ITestOutputHelper output) : base(output)
        {
        }

        [RequiresElasticSearchRetryFact]
        public async Task ShouldErrorAndAlertOnInvalidIndexSetupInElastic()
        {
            using (var store = GetDocumentStore())
            using (GetElasticClient(out var client))
            {
                await client.Indices.CreateAsync(OrdersIndexName, c => c
                    .Mappings(m => m
                        .Properties<object>(p => p
                            .MatchOnlyText("Id"))));

                var config = SetupElasticEtl(store, @"
var orderData = {
    Id: id(this),
    OrderLinesCount: this.Lines.length,
    TotalCost: 0
};

loadTo" + OrdersIndexName + @"(orderData);", 
                    new []{ new ElasticSearchIndex { IndexName = OrdersIndexName, DocumentIdProperty = "Id" } },
                    new List<string> { "orders" }, configurationName: "my-etl", transformationName: "my-transformation");

                using (var session = store.OpenAsyncSession())
                {
                    await session.StoreAsync(new Order { Lines = new List<OrderLine>() });
                    await session.SaveChangesAsync();
                }

                var alert = await AssertWaitForNotNullAsync(async () =>
                {
                    var error = await Etl.TryGetLoadErrorAsync(store.Database, config);
                    return error;
                }, timeout: (int)TimeSpan.FromMinutes(1).TotalMilliseconds);

                Assert.Contains($"The index '{OrdersIndexName}' has invalid mapping for 'Id' property.", alert.Error);
            }
        }
    }
}
