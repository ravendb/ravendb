using System.Linq;
using System.Threading.Tasks;
using FastTests;
using Raven.Client.Documents;
using SlowTests.Core.Utils.Entities;
using Tests.Infrastructure;
using Xunit;
using Xunit.Abstractions;

namespace SlowTests.Issues;

public class RavenDB_20831 : RavenTestBase
{
    public RavenDB_20831(ITestOutputHelper output) : base(output)
    {
    }

    [RavenFact(RavenTestCategory.Querying | RavenTestCategory.ClientApi)]
    public async Task Can_Use_LongCountLazily()
    {
        using var store = GetDocumentStore();

        using (var s = store.OpenSession())
        {
            s.Store(new User { Name = "John" });
            s.SaveChanges();
        }

        using (var s = store.OpenSession())
        {
            var count = s.Query<User>()
                .Where(x => x.Name != null)
                .CountLazily();

            Assert.Equal(1, count.Value);

            var longCount = s.Query<User>()
                .Where(x => x.Name != null)
                .LongCountLazily();

            Assert.Equal(1, longCount.Value);
        }

        using (var s = store.OpenAsyncSession())
        {
            var count = s.Query<User>()
                .Where(x => x.Name != null)
                .CountLazilyAsync();

            Assert.Equal(1, await count.Value);

            var longCount = s.Query<User>()
                .Where(x => x.Name != null)
                .LongCountLazilyAsync();

            Assert.Equal(1, await longCount.Value);
        }

        using (var s = store.OpenSession())
        {
            var count = s.Advanced.DocumentQuery<User>()
                .WhereNotEquals(x => x.Name, (string)null)
                .CountLazily();

            Assert.Equal(1, count.Value);

            var longCount = s.Advanced.DocumentQuery<User>()
                .WhereNotEquals(x => x.Name, (string)null)
                .LongCountLazily();

            Assert.Equal(1, longCount.Value);
        }

        using (var s = store.OpenAsyncSession())
        {
            var count = s.Advanced.AsyncDocumentQuery<User>()
                .WhereNotEquals(x => x.Name, (string)null)
                .CountLazilyAsync();

            Assert.Equal(1, await count.Value);

            var longCount = s.Advanced.AsyncDocumentQuery<User>()
                .WhereNotEquals(x => x.Name, (string)null)
                .LongCountLazilyAsync();

            Assert.Equal(1, await longCount.Value);
        }
    }
}
