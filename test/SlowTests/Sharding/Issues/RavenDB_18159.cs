using System.Linq;
using FastTests;
using SlowTests.Core.Utils.Entities;
using Tests.Infrastructure;
using Xunit;
using Xunit.Abstractions;

namespace SlowTests.Sharding.Issues;

public class RavenDB_18159 : RavenTestBase
{
    public RavenDB_18159(ITestOutputHelper output) : base(output)
    {
    }

    [RavenTheory(RavenTestCategory.Sharding)]
    [RavenData(DatabaseMode = RavenDatabaseMode.Sharded)]
    public void CanSpecifyShardContext(Options options)
    {
        using (var store = GetDocumentStore(options))
        {
            using (var session = store.OpenSession())
            {
                session.Store(new User { Name = "Joe" }, "users/1");
                session.Store(new User {Name = "Doe"}, "users/2");
                session.Store(new User {Name = "James"}, "users/3");

                session.SaveChanges();
            }

            using (var session = store.OpenSession())
            {
                var results = session.Query<User>()
                    .Customize(x => x.ShardContext(s => s.ByDocumentId("users/1")))
                    .Where(x => x.Name == "Joe").ToList();

                Assert.Equal(1, results.Count);
                Assert.Equal("Joe", results[0].Name);

                var results2 = session.Query<User>()
                    .Customize(x => x.ShardContext(s => s.ByDocumentIds(new []{ "users/2", "users/3" })))
                    .Where(x => x.Name == "Doe" || x.Name == "James").Select(x => x.Name).ToList();

                Assert.Equal(2, results2.Count);
                Assert.Contains("Doe", results2);
                Assert.Contains("James", results2);

                var emptyResults = session.Query<User>()
                    .Customize(x => x.ShardContext(s => s.ByDocumentId("users/2"))) // we specify wrong id - this shard doesn't contain "Joe"
                    .Where(x => x.Name == "Joe").ToList();

                Assert.Equal(0, emptyResults.Count);
            }

            using (var session = store.OpenSession())
            {
                var results = session.Advanced.DocumentQuery<User>()
                    .ShardContext(s => s.ByDocumentId("users/1"))
                    .WhereEquals(x => x.Name, "Joe").ToList();

                Assert.Equal(1, results.Count);
                Assert.Equal("Joe", results[0].Name);

                var results2 = session.Advanced.DocumentQuery<User>()
                    .ShardContext(s => s.ByDocumentIds(new [] { "users/2", "users/3" }))
                    .WhereEquals(x => x.Name, "Doe")
                    .OrElse()
                    .WhereEquals(x => x.Name, "James")
                    .SelectFields<string>("Name").ToList();

                Assert.Equal(2, results2.Count);
                Assert.Contains("Doe", results2);
                Assert.Contains("James", results2);

                var emptyResults = session.Advanced.DocumentQuery<User>()
                    .ShardContext(s => s.ByDocumentId("users/2")) // we specify wrong id - this shard doesn't contain "Joe"
                    .WhereEquals(x => x.Name, "Joe").ToList();

                Assert.Equal(0, emptyResults.Count);
            }
        }
    }
}
