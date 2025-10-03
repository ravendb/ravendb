using FastTests;
using Raven.Client.Documents.Operations;
using Raven.Client.Exceptions.Sharding;
using Tests.Infrastructure;
using Xunit;
using Xunit.Abstractions;

namespace SlowTests.Issues;

public class RavenDB_18663 : RavenTestBase
{
    public RavenDB_18663(ITestOutputHelper output) : base(output)
    {
    }

    [RavenTheory(RavenTestCategory.Patching)]
    [RavenData(DatabaseMode = RavenDatabaseMode.Sharded)]
    public void CanUsePagingWhilePatchingOrDeleting(Options options)
    {
        using (var store = GetDocumentStore(options))
        {
            Assert.Throws<NotSupportedInShardingException>(() => store.Operations.Send(new PatchByQueryOperation("from Companies as c update { c.Name = 'patch1' } limit 0,2")));
        }
    }
}
