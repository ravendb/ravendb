using FastTests;
using Raven.Client.Documents.Session;
using Tests.Infrastructure;
using Xunit;
using Xunit.Abstractions;

namespace SlowTests.Issues;

public class RavenDB_21379 : RavenTestBase
{
    public RavenDB_21379(ITestOutputHelper output) : base(output)
    {
    }

    [RavenTheory(RavenTestCategory.Querying | RavenTestCategory.Sharding)]
    [RavenData(DatabaseMode = RavenDatabaseMode.All)]
    public void QueryMapReduceWithCount(Options options)
    {
        using (var store = GetDocumentStore(options))
        {
            using (var session = store.OpenSession())
            {
                User user1 = new() {name = "John"};

                User user2 = new() {name = "John"};

                User user3 = new() { name = "Tarzan" };

                session.Store(user1, "users/1");
                session.Store(user2, "users/2");
                session.Store(user3, "users/3");
                session.SaveChanges();
            }

            using (var session = store.OpenSession())
            {
                var documentQuery = session.Advanced.DocumentQuery<User>().GroupBy("name")
                    .SelectKey()
                    .SelectCount("count")
                    .OrderByDescending("count")
                    .OfType<ReduceResult>();

                var res = documentQuery.ToList();

                Assert.Equal(2, res[0].count);
                Assert.Equal("John", res[0].name);

                Assert.Equal(1, res[1].count);
                Assert.Equal("Tarzan", res[1].name);
            }
        }
    }

    private class User
    {
        public string name { get; set; }
    }

    private class ReduceResult
    {
#pragma warning disable CS0649 // Field is never assigned to, and will always have its default value
        public int count;
        public string name;
#pragma warning restore CS0649 // Field is never assigned to, and will always have its default value
    }
}
