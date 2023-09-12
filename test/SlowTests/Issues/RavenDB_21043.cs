using System.Linq;
using FastTests;
using Orders;
using Raven.Client.Documents.Indexes;
using Raven.Client.Documents.Session;
using Raven.Client.ServerWide;
using Raven.Client.ServerWide.Operations.DocumentsCompression;
using Tests.Infrastructure;
using Xunit;
using Xunit.Abstractions;

namespace SlowTests.Issues;

public class RavenDB_21043 : RavenTestBase
{
    public RavenDB_21043(ITestOutputHelper output) : base(output)
    {
    }

    [RavenTheory(RavenTestCategory.Indexes)]
    [RavenData(SearchEngineMode = RavenSearchEngineMode.Corax)]
    public void Should_Be_Able_To_Train_Corax_Dictionary_on_Compare_Exchange_Values_and_compressed_docs(Options options)
    {
        const int numberOfCompanies = 256;
        const int numberOfAddresses = 16;

        DoNotReuseServer();

        using (var store = GetDocumentStore(options))
        {
            // turn on compression
            store.Maintenance.Send(new UpdateDocumentsCompressionConfigurationOperation(new DocumentsCompressionConfiguration(false, true)));

            using (var session = store.OpenSession(new SessionOptions { TransactionMode = TransactionMode.ClusterWide }))
            {
                for (var i = 0; i < numberOfAddresses; i++)
                {
                    session.Advanced.ClusterTransaction.CreateCompareExchangeValue($"addresses/{i}", new Address { City = $"Address_{i}" });
                }

                session.SaveChanges();
            }

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

            new Companies_ByCity().Execute(store);

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
