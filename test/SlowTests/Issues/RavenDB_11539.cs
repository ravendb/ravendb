
using System.Linq;
using FastTests;
using FastTests.Server.Basic.Entities;
using Raven.Client.Documents.Indexes;
using Raven.Client.Documents.Queries;
using SlowTests.Core.Utils.Entities;
using Tests.Infrastructure.Entities;
using Xunit;
using Xunit.Abstractions;

namespace SlowTests.Issues
{
    public class RavenDB_11539 : RavenTestBase
    {
        public RavenDB_11539(ITestOutputHelper output) : base(output)
        {
        }

        private class Index1 : AbstractIndexCreationTask<Order>
        {
            public Index1()
            {
                Map = orders =>
                    from order in orders
                    select new
                    {
                        Company = order.Company
                    };
            }
        }

        [Fact]
        public void IndexQueryWithLoadAndSimpleMemberProjectionShouldGenerateCorrecetSelectPath()
        {
            using (var store = GetDocumentStore())
            {
                new Index1().Execute(store);

                using (var session = store.OpenSession())
                {
                    session.Store(new Company
                    {
                        Name = "HR"
                    }, "companies/1");
                    session.Store(new Order
                    {
                        Company = "companies/1"
                    });

                    session.SaveChanges();
                }

                WaitForIndexing(store);

                using (var session = store.OpenSession())
                {
                    var query = from o in session.Query<Order, Index1>()
                                let c = RavenQuery.Load<Company>(o.Company)
                                select c.Name;

                    Assert.Equal("from index 'Index1' as o load o.Company as c select c.Name"
                        , query.ToString());

                    var result = query.ToList();

                    Assert.Equal(1, result.Count);
                    Assert.Equal("HR", result[0]);

                }
            }
        }
    }
}
