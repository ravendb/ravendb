using System.Linq;
using FastTests;
using Raven.Client.Documents.Linq;
using Raven.Tests.Core.Utils.Entities;
using Tests.Infrastructure;
using Xunit;
using Xunit.Abstractions;

namespace SlowTests.Sharding.Issues;

public class RavenDB_19683 : RavenTestBase
{
    public RavenDB_19683(ITestOutputHelper output) : base(output)
    {
    }

    [RavenFact(RavenTestCategory.Querying | RavenTestCategory.Sharding)]
    public void Simple_query_with_distinct()
    {
        using (var store = Sharding.GetDocumentStore())
        {
            using (var session = store.OpenSession())
            {
                session.Store(new User { Name = "Grisha", Age = 1 }, "users/1");
                session.Store(new User { Name = "Grisha", Age = 1 }, "users/2");
                session.Store(new User { Name = "Grisha", Age = 1 }, "users/3");
                session.Store(new User { Name = "Grisha", Age = 1 }, "users/4");
                session.Store(new User { Name = "Grisha", Age = 1 }, "users/5");
                session.Store(new User { Name = "Grisha", Age = 1 }, "users/6");
                session.SaveChanges();

                var queryResult = session.Query<User>()
                    .OrderBy(x => x.Name)
                    .Select(x => x.Name)
                    .Customize(x => x.WaitForNonStaleResults())
                    .Distinct()
                    .ToList();

                Assert.Equal(1, queryResult.Count);
                Assert.Equal("Grisha", queryResult[0]);
            }
        }
    }
}
