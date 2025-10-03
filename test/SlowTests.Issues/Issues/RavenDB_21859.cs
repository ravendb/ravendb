using System.Threading.Tasks;
using FastTests;
using Tests.Infrastructure;
using Xunit;
using Xunit.Abstractions;

namespace SlowTests.Issues;

public class RavenDB_21859 : RavenTestBase
{
    public RavenDB_21859(ITestOutputHelper output) : base(output)
    {
    }

    [RavenFact(RavenTestCategory.ClientApi)]
    public async Task Load_And_Lazy_Load_Should_Return_Null_When_Id_Is_Null()
    {
        using (var store = GetDocumentStore())
        {
            using (var session = store.OpenSession())
            {
                Assert.Null(session.Load<object>((string)null));

                Assert.Null(session.Advanced.Lazily.Load<object>((string)null).Value);
            }

            using (var session = store.OpenAsyncSession())
            {
                Assert.Null(await session.LoadAsync<object>((string)null));

                Assert.Null(await session.Advanced.Lazily.LoadAsync<object>((string)null).Value);
            }
        }
    }
}
