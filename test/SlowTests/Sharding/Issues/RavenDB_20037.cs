using System.Linq;
using FastTests;
using Orders;
using Tests.Infrastructure;
using Xunit;
using Xunit.Abstractions;

namespace SlowTests.Sharding.Issues;

public class RavenDB_20037 : RavenTestBase
{
    public RavenDB_20037(ITestOutputHelper output) : base(output)
    {
    }

    [RavenFact(RavenTestCategory.Sharding | RavenTestCategory.Querying)]
    public void Query_With_Offset_That_Exceeds_Number_of_Results_Should_Return_0()
    {
        using (var store = Sharding.GetDocumentStore())
        {
            using (var session = store.OpenSession())
            {
                session.Store(new Company { Name = "CF" });

                session.SaveChanges();
            }

            using (var session = store.OpenSession())
            {
                var companies = session.Query<Company>()
                    .ToList();

                Assert.Equal(1, companies.Count);

                companies = session.Query<Company>()
                    .Skip(0)
                    .ToList();

                Assert.Equal(1, companies.Count);

                companies = session.Query<Company>()
                    .Skip(1)
                    .ToList();

                Assert.Equal(0, companies.Count);

                companies = session.Query<Company>()
                    .Skip(2)
                    .ToList();

                Assert.Equal(0, companies.Count);
            }
        }
    }
}
