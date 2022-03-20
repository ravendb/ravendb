using System.Linq;
using FastTests;
using Orders;
using Raven.Client.Documents.Indexes;
using Raven.Client.Documents.Session;
using Xunit;
using Xunit.Abstractions;

namespace SlowTests.Issues
{
    public class RavenDB_15493 : RavenTestBase
    {
        public RavenDB_15493(ITestOutputHelper output) : base(output)
        {
        }

        [Fact]
        public void Should_Be_Able_To_Index_Compare_Exchange()
        {
            const int numberOfCompanies = 256;
            const int numberOfAddresses = 16;

            DoNotReuseServer();

            using (var store = GetDocumentStore())
            {
                new Companies_ByCity().Execute(store);

                using (var bulk = store.BulkInsert())
                {
                    for (var i = 0; i < numberOfCompanies; i++)
                    {
                        bulk.Store(new Company
                        {
                            Name = $"Company_{i}",
                            ExternalId = $"addresses/{i % numberOfAddresses}"
                        });
                    }
                }

                Indexes.WaitForIndexing(store);

                using (var session = store.OpenSession(new SessionOptions { TransactionMode = TransactionMode.ClusterWide }))
                {
                    for (var i = 0; i < numberOfAddresses; i++)
                    {
                        session.Advanced.ClusterTransaction.CreateCompareExchangeValue($"addresses/{i}", new Address { City = $"Address_{i}" });
                    }

                    session.SaveChanges();
                }

                Indexes.WaitForIndexing(store);

                using (var session = store.OpenSession())
                {
                    var results = session.Query<Companies_ByCity.Result, Companies_ByCity>()
                        .Select(x => new
                        {
                            Name = x.Name,
                            City = x.City
                        })
                        .ToList();

                    Assert.Equal(numberOfCompanies, results.Count);

                    foreach (var result in results)
                    {
                        Assert.NotNull(result.Name);
                        Assert.NotNull(result.City); // should not be null
                    }
                }
            }
        }

        private class Companies_ByCity : AbstractIndexCreationTask<Company>
        {
            public class Result
            {
                public string Name { get; set; }

                public string City { get; set; }
            }

            public Companies_ByCity()
            {
                Map = companies => from company in companies
                                   let address = LoadCompareExchangeValue<Address>(company.ExternalId)
                                   select new
                                   {
                                       Name = company.Name,
                                       City = address.City
                                   };

                StoreAllFields(FieldStorage.Yes);
            }
        }
    }
}
