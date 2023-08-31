using FastTests;
using Orders;
using Tests.Infrastructure;
using Xunit;
using Xunit.Abstractions;

namespace SlowTests.Sharding.Issues;

public class RavenDB_20035 : RavenTestBase
{
    public RavenDB_20035(ITestOutputHelper output) : base(output)
    {
    }

    [RavenFact(RavenTestCategory.Sharding)]
    public void Should_Not_Throw_NRE_When_Value_Of_Included_Property_Is_Null()
    {
        using (var store = Sharding.GetDocumentStore())
        {
            using (var session = store.OpenSession())
            {
                session.Store(new Address { City = "Torun" }, "addresses/1");
                session.Store(new Company
                {
                    ExternalId = null,
                    Fax = "addresses/1"
                }, "companies/1");

                session.SaveChanges();
            }

            using (var session = store.OpenSession())
            {
                var company = session.Load<Company>("companies/1", builder => builder
                    .IncludeDocuments<Address>(x => x.ExternalId)
                    .IncludeDocuments<Address>(x => x.Fax));

                Assert.NotNull(company);

                var numberOfRequests = session.Advanced.NumberOfRequests;

                var address = session.Load<Address>(company.Fax);

                Assert.NotNull(address);
                Assert.Equal(numberOfRequests, session.Advanced.NumberOfRequests);
            }
        }
    }


    [RavenFact(RavenTestCategory.Sharding)]
    public void Should_Not_Throw_NRE_For_Non_Existing_Documents_When_More_Than_One_Document_Is_Requested()
    {
        using (var store = Sharding.GetDocumentStore())
        {
            using (var session = store.OpenSession())
            {
                session.Store(new Address { City = "Torun" }, "addresses/1");
                session.Store(new Company
                {
                    ExternalId = null,
                    Fax = "addresses/1"
                }, "companies/1");

                session.SaveChanges();
            }

            using (var session = store.OpenSession())
            {
                var results = session.Load<Company>(new[] { "companies/1", "companies/2", "companies/3" }, builder => builder
                    .IncludeDocuments<Address>(x => x.ExternalId)
                    .IncludeDocuments<Address>(x => x.Fax));

                Assert.NotNull(results);
                Assert.Equal(3, results.Count);

                Assert.NotNull(results["companies/1"]);
                Assert.Null(results["companies/2"]);
                Assert.Null(results["companies/3"]);
            }
        }
    }
}
