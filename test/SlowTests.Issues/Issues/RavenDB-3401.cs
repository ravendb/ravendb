using System.Linq;
using FastTests;
using Raven.Client.Documents.Indexes;
using Tests.Infrastructure;
using Xunit;
using Xunit.Abstractions;

namespace SlowTests.Issues
{
    public class RavenDB_3401 : RavenTestBase
    {
        public RavenDB_3401(ITestOutputHelper output) : base(output)
        {
        }

        [RavenTheory(RavenTestCategory.Querying | RavenTestCategory.Indexes)]
        [RavenData(DatabaseMode = RavenDatabaseMode.All, SearchEngineMode = RavenSearchEngineMode.All)]
        public void projections_with_property_rename(Options options)
        {
            using (var store = GetDocumentStore(options))
            {
                var index = new Customers_ByName();
                index.Execute(store);

                using (var session = store.OpenSession())
                {
                    session.Store(new Customer { Name = "John", Address = "Tel Aviv" });
                    session.SaveChanges();

                    Indexes.WaitForIndexing(store);

                    var customer = session.Query<Customer>(index.IndexName)
                        .Select(r => new
                        {
                            Name = r.Name,
                            OtherThanName = r.Address,
                            OtherThanName2 = r.Address,
                            AnotherOtherThanName = r.Name
                        }).Single();
                    {
                        Assert.Equal("John", customer.Name);
                        Assert.Equal("Tel Aviv", customer.OtherThanName);
                        Assert.Equal("Tel Aviv", customer.OtherThanName2);
                        Assert.Equal("John", customer.AnotherOtherThanName);
                    }
                }
            }
        }

        private class Customer
        {
            public string Name { get; set; }
            public string Address { get; set; }
        }

        private class Customers_ByName : AbstractIndexCreationTask<Customer>
        {
            public Customers_ByName()
            {
                Map = customers => from customer in customers
                                   select new
                                   {
                                       customer.Name
                                   };
            }
        }
    }
}


