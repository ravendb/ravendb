using System.Collections.Generic;
using System.Linq;
using FastTests;
using FastTests.Client;
using Raven.Client.Documents.Indexes;
using Tests.Infrastructure;
using Tests.Infrastructure.Entities;
using Xunit;
using Xunit.Abstractions;

namespace SlowTests.Issues;

public class RavenDB_19083 : RavenTestBase
{
    public RavenDB_19083(ITestOutputHelper output) : base(output)
    {
    }

    [RavenFact(RavenTestCategory.Querying | RavenTestCategory.Sharding)]
    public void No_results_after_reduce_on_orchestrator()
    {
        using (var store = Sharding.GetDocumentStore())
        {
            var index = new Orders_ByCompany();

            store.ExecuteIndex(index);

            using (var session = store.OpenSession())
            {
                for (int i = 0; i < 10; i++)
                {
                    session.Store(new Query.Order()
                    {
                        Company = "companies/1",
                        Lines = new List<OrderLine>
                        {
                            new OrderLine(){ Quantity = 1, PricePerUnit = 10}
                        }
                    });
                }

                session.SaveChanges();

                var results = session.Query<Orders_ByCompany.Result, Orders_ByCompany>().Customize(x => x.WaitForNonStaleResults()).ToList();

                Assert.Equal(0, results.Count);
            }
        }
    }

    private class Orders_ByCompany : AbstractIndexCreationTask<Query.Order, Orders_ByCompany.Result>
    {
        public class Result
        {
            public string Company { get; set; }
            public int Count { get; set; }
            public decimal Total { get; set; }
        }

        public Orders_ByCompany()
        {
            Map = orders => from order in orders
                            select new Result { Company = order.Company, Count = 1, Total = order.Lines.Sum(l => (l.Quantity * l.PricePerUnit) * (1 - l.Discount)) };

            Reduce = results => from result in results
                                group result by result.Company
                                into g
                                let total = g.Sum(x => x.Total)
                                where total < 100
                                select new { Company = g.Key, Count = g.Sum(x => x.Count), Total =  total };
        }
    }
}
