using System;
using System.Collections.Generic;
using System.Linq;
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

        [RequiresElasticSearchFact]
        public async Task ShouldErrorAndAlertOnInvalidIndexSetupInElastic()
        {
            using (var store = GetDocumentStore())
            using (GetElasticClient(out var client))
            {
                client.Indices.Create(OrdersIndexName, c => c
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

loadTo" + OrdersIndexName + @"(orderData);", 
                    new []{ new ElasticSearchIndex { IndexName = OrdersIndexName, DocumentIdProperty = "Id" } },
                    new List<string> { "orders" }, configurationName: "my-etl", transformationName: "my-transformation");

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

                Assert.Contains($"The index '{OrdersIndexName}' has invalid mapping for 'Id' property.", alert.Error);
            }
        }
    }
}
