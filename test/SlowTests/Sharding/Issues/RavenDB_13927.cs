using System.Linq;
using FastTests;
using Raven.Client.Documents;
using Raven.Client.Exceptions.Sharding;
using Raven.Tests.Core.Utils.Entities;
using Tests.Infrastructure;
using Xunit;
using Xunit.Abstractions;

namespace SlowTests.Sharding.Issues;

public class RavenDB_13927 : RavenTestBase
{
    public RavenDB_13927(ITestOutputHelper output) : base(output)
    {
    }

    [RavenFact(RavenTestCategory.Sharding | RavenTestCategory.Querying)]
    public void ShouldThrowOnAttemptToCreateIndexWithOutputReduce()
    {
        using (var store = Sharding.GetDocumentStore())
        {
            var e = Assert.Throws<NotSupportedInShardingException>(() =>
            {
                using (var session = store.OpenSession())
                {
                    session.Store(new User() { Name = "Foo"});
                    session.Store(new User() { Name = "Bar"});

                    session.SaveChanges();

                    var list = session.Query<User>().Where(x => x.Name == "foo").OrderByScore()
                        .ToList();
                }
            });
            Assert.Contains("Ordering by score is not supported in sharding", e.Message);
        }
    }
}
