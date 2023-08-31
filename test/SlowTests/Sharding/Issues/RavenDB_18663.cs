using FastTests;
using Raven.Client.Documents.Operations;
using Raven.Client.Exceptions.Sharding;
using Tests.Infrastructure;
using Xunit;
using Xunit.Abstractions;

namespace SlowTests.Sharding.Issues;

public class RavenDB_18663 : RavenTestBase
{
    public RavenDB_18663(ITestOutputHelper output) : base(output)
    {
    }

    [RavenFact(RavenTestCategory.Sharding | RavenTestCategory.Querying)]
    public void ShouldThrowOnAttemptToCreateIndexWithOutputReduce()
    {
        using (var store = Sharding.GetDocumentStore())
        {
            var e = Assert.Throws<NotSupportedInShardingException>(() =>
            {
                var operation = store.Operations.Send(new PatchByQueryOperation("from Companies update { this.Name = this.Name + '_Patched'; } limit 3"));

            });
            Assert.Contains("Query with limit is not supported in patch / delete by query operation", e.Message);
        }
    }
}
