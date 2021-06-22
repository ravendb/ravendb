using System;
using System.Linq;
using FastTests;
using MongoDB.Driver;
using Newtonsoft.Json;
using Raven.Client.Documents;
using Raven.Client.Documents.Indexes;
using Tests.Infrastructure;
using Tests.Infrastructure.Entities;
using Xunit;
using Xunit.Abstractions;

namespace SlowTests.SlowTests
{
    public class RavenDB_14600 : RavenTestBase
    {
        public RavenDB_14600(ITestOutputHelper output) : base(output)
        {
        }

        [Fact]
        public void CanIncludeFacetResult()
        {
            using var store = GetDocumentStore();
            store.Maintenance.Send(new CreateSampleDataOperation(Raven.Client.Documents.Smuggler.DatabaseItemType.Documents | Raven.Client.Documents.Smuggler.DatabaseItemType.Indexes));
            WaitForIndexing(store);
            using (var s = store.OpenSession())
            {
                var facets = s.Query<Order>("Orders/Totals")
                    .Include(x => x.Employee)
                    .Where(x => x.Company == "companies/1-A")
                    .AggregateBy(x => x.ByField(o => o.Employee))
                    .Execute();

                Assert.NotEmpty(facets["Employee"].Values);
                foreach (var f in facets["Employee"].Values)
                {
                    s.Load<object>(f.Range);
                }
                Assert.Equal(1, s.Advanced.NumberOfRequests);
            }
        }

        private class MyIndex : AbstractIndexCreationTask<Order>
        {
            public MyIndex()
            {
                Map = orders =>
                    from o in orders
                    select new
                    {
                        o.Employee,
                        o.Company,
                        Total = o.Lines.Sum(x => x.Quantity * x.PricePerUnit),
                        EmployeeByDay = new { o.Employee, o.OrderedAt.Date }
                    };
                Index("EmployeeByDay", FieldIndexing.Exact);
            }
        }

        [Fact]
        public void CanIncludeComplexFacetResult()
        {
            using var store = GetDocumentStore();
            store.Maintenance.Send(new CreateSampleDataOperation(Raven.Client.Documents.Smuggler.DatabaseItemType.Documents));
            new MyIndex().Execute(store);
            WaitForIndexing(store);

            using (var s = store.OpenSession())
            {
                var facets = s.Query<Order, MyIndex>()
                    .Include("EmployeeByDay.Employee")
                    .Where(x => x.Company == "companies/1-A")
                    .AggregateBy(x => x.ByField("EmployeeByDay"))
                    .Execute();

                Assert.NotEmpty(facets["EmployeeByDay"].Values);
                foreach (var f in facets["EmployeeByDay"].Values)
                {
                    var item = JsonConvert.DeserializeAnonymousType(f.Range, new { Employee = default(string), Date = default(DateTime) });
                    Assert.NotNull(s.Load<object>(item.Employee));
                }
                Assert.Equal(1, s.Advanced.NumberOfRequests);
            }
        }
    }
}
