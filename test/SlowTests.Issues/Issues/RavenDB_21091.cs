using System;
using System.Collections.Generic;
using System.Linq;
using System.Threading.Tasks;
using FastTests;
using Orders;
using Raven.Client.Documents;
using Raven.Client.Documents.Commands.Batches;
using Raven.Client.Documents.Operations;
using Raven.Client.Documents.Queries;
using Tests.Infrastructure;
using Xunit;

namespace SlowTests.Issues;

public class RavenDB_21091 : RavenTestBase
{
    public RavenDB_21091(ITestOutputHelper output) : base(output)
    {
    }

    [RavenTheory(RavenTestCategory.Patching)]
    [InlineData(true)]
    [InlineData(false)]
    public async Task CanPutDocumentWithIdIdentityPartsSeparatorInPatch(bool useBatchPatch)
    {
        using (var store = GetDocumentStore())
        {
            using (var session = store.OpenAsyncSession())
            {
                await session.StoreAsync(new Order(), "orders/0000000000000000002-A");
                await session.SaveChangesAsync();
            }

            if (useBatchPatch)
            {
                using (var session = store.OpenAsyncSession())
                {
                    session.Advanced.Defer(new BatchPatchCommandData(new List<string>()
                    {
                        "orders/0000000000000000002-A"
                    }, new PatchRequest
                    {
                        Script = "put('orders/', this);"
                    }, null));
                    
                    await session.SaveChangesAsync();
                }
            }
            else
            {
                var operation = await store.Operations.SendAsync(new PatchByQueryOperation(new IndexQuery { Query = "from Orders update { put('orders/', this); }" }));
                await operation.WaitForCompletionAsync();
            }
            
            using (var session = store.OpenAsyncSession())
            {
                var orders = await session.Query<Order>().ToListAsync();
                
                Assert.Equal(2, orders.Count);
            }
        }
    }
}
