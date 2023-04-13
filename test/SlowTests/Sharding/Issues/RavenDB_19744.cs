using System;
using System.Threading.Tasks;
using FastTests;
using Orders;
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
                using (var session = store.OpenSession())
                {
                    for (var i = 0; i < 10; i++)
                        session.Store(new Order(), $"orders/{i}");

                    session.SaveChanges();
                }

                // all document Ids in 'Orders' collection don't contain a '$suffix'
                var operation = await store.Operations.SendAsync(new PatchByQueryOperation("from Orders update { put(\"orders/\", this) }"));
                var result = await operation.WaitForCompletionAsync<BulkOperationResult>(TimeSpan.FromMinutes(1));
                Assert.Equal(10, result.Total);

                // some document Ids in 'Orders' collection contains a '$suffix'
                operation = await store.Operations.SendAsync(new PatchByQueryOperation("from Orders update { put(\"orders/\", this) }"));
                result = await operation.WaitForCompletionAsync<BulkOperationResult>(TimeSpan.FromMinutes(1));
                Assert.Equal(20, result.Total);
            }
        }
    }
}
