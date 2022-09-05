using System.Linq;
using System.Threading.Tasks;
using FastTests;
using Raven.Client.Documents;
using Xunit;
using Xunit.Abstractions;

namespace SlowTests.Issues;

public class RavenDB_19286 : RavenTestBase
{
    public RavenDB_19286(ITestOutputHelper output) : base(output)
    {
    }

    class User
    {
        public string Name;
    }

    [Fact]
    public async Task CanDoStringRangeQuery()
    {
        using var store = GetDocumentStore();
        using (var session = store.OpenAsyncSession())
        {
            await session.StoreAsync(new User { Name = "Zoof" });
            await session.SaveChangesAsync();
        }

        using (var session = store.OpenAsyncSession())
        {
            await session.Query<User>()
                .Where(x => x.Name.CompareTo( "Zoof") == 0)
                .SingleAsync();

            await session.Query<User>()
                .Where(x => string.Compare(x.Name, "Zoof") == 0)
                .SingleAsync();
        }
    }
}
