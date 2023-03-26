using System;
using System.Threading.Tasks;
using FastTests;
using Raven.Client.Documents.Operations;
using Tests.Infrastructure;
using Xunit;
using Xunit.Abstractions;

namespace SlowTests.Sharding.Issues
{
    public class RavenDB_19744 : RavenTestBase
    {
        public RavenDB_19744(ITestOutputHelper output) : base(output)
        {
        }

        [RavenFact(RavenTestCategory.Patching | RavenTestCategory.Sharding)]
        public async Task PatchByQueryOnShardedDbShouldGenerateIdsInTheCorrectShard()
        {
            using (var store = Sharding.GetDocumentStore())
            {
                await store.Maintenance.SendAsync(new CreateSampleDataOperation());

                var operation = await store.Operations.SendAsync(new PatchByQueryOperation("from Orders update { put(\"orders/\", this) }"));
                var result = await operation.WaitForCompletionAsync<BulkOperationResult>(TimeSpan.FromMinutes(1));
                Assert.Equal(830, result.Total);
            }
        }
    }
}
