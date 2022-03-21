using System;
using System.Collections.Generic;
using System.Linq;
using System.Net.Http;
using FastTests;
using Orders;
using Raven.Client.Documents.Conventions;
using Raven.Client.Documents.Indexes;
using Raven.Client.Documents.Operations;
using Raven.Client.Documents.Operations.Indexes;
using Raven.Client.Http;
using Sparrow.Json;
using Tests.Infrastructure;
using Tests.Infrastructure.Operations;
using Xunit;
using Xunit.Abstractions;

namespace SlowTests.Issues
{
    public class RavenDB_5824 : RavenTestBase
    {
        public RavenDB_5824(ITestOutputHelper output) : base(output)
        {
        }

        [Fact]
        public void ShouldBeAbleToReturnIndexStalenessReasons()
        {
            using (var store = GetDocumentStore())
            {
                store.Maintenance.Send(new CreateSampleDataOperation());

                new Index().Execute(store);

                Indexes.WaitForIndexing(store);

                var staleness = store.Maintenance.Send(new GetIndexStalenessOperation(new Index().IndexName));
                Assert.False(staleness.IsStale);
                Assert.Empty(staleness.StalenessReasons);

                store.Maintenance.Send(new StopIndexingOperation());

                using (var session = store.OpenSession())
                {
                    var order = session.Load<Order>("orders/1-A");
                    var company = session.Load<Company>("companies/1-A");
                    var product = session.Load<Product>("products/1-A");
                    var supplier = session.Load<Supplier>("suppliers/1-A");

                    order.Freight = 10;
                    company.ExternalId = "1234";
                    product.PricePerUnit = 10;
                    supplier.Fax = "1234";

                    session.Delete("orders/2-A");

                    session.SaveChanges();
                }

                staleness = store.Maintenance.Send(new GetIndexStalenessOperation(new Index().IndexName));
                Assert.True(staleness.IsStale);
                Assert.Equal(5, staleness.StalenessReasons.Count);

                store.Maintenance.Send(new StartIndexingOperation());

                Indexes.WaitForIndexing(store);

                staleness = store.Maintenance.Send(new GetIndexStalenessOperation(new Index().IndexName));
                Assert.False(staleness.IsStale);
                Assert.Empty(staleness.StalenessReasons);
            }
        }

        private class Index : AbstractMultiMapIndexCreationTask<Index.Result>
        {
            public class Result
            {
                public string Name { get; set; }
            }

            public Index()
            {
                AddMap<Order>(orders =>
                    from o in orders
                    let c = LoadDocument<Company>(o.Company)
                    select new
                    {
                        Name = c.Name
                    });

                AddMap<Product>(products =>
                    from p in products
                    let s = LoadDocument<Supplier>(p.Supplier)
                    select new
                    {
                        Name = s.Name
                    });
            }
        }
    }
}
