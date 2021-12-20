using System;
using System.Collections.Generic;
using System.Linq;
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
        public void ShouldErrorAndAlertOnInvalidIndexSetupInElastic()
        {
            using (var store = GetDocumentStore())
            using (GetElasticClient(out var client))
            {
                client.Indices.Create("orders", c => c
                    .Map(m => m
                        .Properties(p => p
                            .MatchOnlyText(t => t
                                .Name("Id")))));

                SetupElasticEtl(store, @"
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

                etlDone.Wait(TimeSpan.FromSeconds(15));

                using (GetDatabase(store.Database).Result.NotificationCenter.GetStored(out var alerts, false))
                {
                    var alert = alerts.Where(x => x.Json.ToString().Contains("The index 'orders' has invalid mapping for 'Id' property."));

                    Assert.NotNull(alert);
                }

            }
        }
    }
}
