using System;
using System.Collections.Generic;
using System.Linq;
using System.Threading.Tasks;
using Orders;
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

        [RequiresElasticSearchFact]
        public async Task ShouldErrorAndAlertOnInvalidIndexSetupInElastic()
        {
            using (var store = GetDocumentStore())
            using (GetElasticClient(out var client))
            {
                client.Indices.Create("orders", c => c
                    .Map(m => m
                        .Properties(p => p
                            .MatchOnlyText(t => t
                                .Name("Id")))));

                var config = SetupElasticEtl(store, @"
var orderData = {
    Id: id(this),
    OrderLinesCount: this.Lines.length,
    TotalCost: 0
};

loadToOrders(orderData);
", new List<string> { "orders" }, configurationName: "my-etl", transformationName: "my-transformation");
                var etlDone = WaitForEtl(store, (n, statistics) => statistics.LoadErrors != 0);

                using (var session = store.OpenSession())
                {
                    session.Store(new Order { Lines = new List<OrderLine>() });
                    session.SaveChanges();
                }

                var alert = await AssertWaitForNotNullAsync(() =>
                {
                    TryGetLoadError(store.Database, config, out var error);

                    return Task.FromResult(error);
                }, timeout: (int)TimeSpan.FromMinutes(1).TotalMilliseconds);

                Assert.Contains("The index 'orders' has invalid mapping for 'Id' property.", alert.Error);
            }
        }
    }
}
