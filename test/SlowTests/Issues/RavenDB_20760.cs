
using System.Linq;
using System.Threading.Tasks;
using FastTests;
using SlowTests.Core.Utils.Entities;
using Tests.Infrastructure;
using Xunit;
using Xunit.Abstractions;

namespace SlowTests.Issues;

public class RavenDB_20760 : RavenTestBase
{
    public RavenDB_20760(ITestOutputHelper output) : base(output)
    {
    }

    [RavenTheory(RavenTestCategory.ClientApi | RavenTestCategory.Querying)]
    [RavenData(DatabaseMode = RavenDatabaseMode.All)]
    public async Task StreamingQueryMoveNextBeyondDocAmount(Options options)
    {
        using (var documentStore = GetDocumentStore(options))
        {
            using (var session = documentStore.OpenSession())
            {
                for (int i = 0; i < 5; i++)
                    session.Store(new User());

                session.SaveChanges();
            }

            using (var session = documentStore.OpenSession())
            {
                var q = session.Query<User>().OrderBy(x => x.Name);
                var enumerator = session.Advanced.Stream(q);

                while (enumerator.MoveNext())
                {
                }

                Assert.False(enumerator.MoveNext());
            }

            using (var session = documentStore.OpenAsyncSession())
            {
                var q = session.Query<User>().OrderBy(x => x.Name);
                var enumerator = await session.Advanced.StreamAsync(q);

                while (await enumerator.MoveNextAsync())
                {
                }

                Assert.False(await enumerator.MoveNextAsync());
            }
        }
    }
}
