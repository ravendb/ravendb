using System;
using System.Diagnostics;
using System.Threading.Tasks;
using Raven.Server.Documents;
using Raven.Server.ServerWide.Context;
using Voron;
using Xunit;
using Xunit.Abstractions;

namespace FastTests.Server.Documents
{
    public class DocumentIdWorkerTests : RavenTestBase
    {
        [Fact]
        public async Task GetSliceFromId_WhenStringAscii_ShouldNotModifyTheValueAcceptToLower()
        {
            using (var store = GetDocumentStore())
            {
                var database = await GetDocumentDatabaseInstanceFor(store);
                using (database.DocumentsStorage.ContextPool.AllocateOperationContext(out DocumentsOperationContext ctx))
                {
                    const string str = "Person@1";

                    using (DocumentIdWorker.GetSliceFromId(ctx, str, out var lowerId))
                    {
                        Assert.Equal(str.ToLower(), lowerId.ToString());
                    }
                }
            }
        }

        [Fact]
        public async Task GetSliceFromId_WhenStringIsUnicode_ShouldNotModifyTheValueAcceptToLower()
        {
            using (var store = GetDocumentStore())
            {
                var database = await GetDocumentDatabaseInstanceFor(store);
                using (database.DocumentsStorage.ContextPool.AllocateOperationContext(out DocumentsOperationContext ctx))
                {
                    const string str = "Person@יפתח";

                    using (DocumentIdWorker.GetSliceFromId(ctx, str, out var lowerId))
                    {
                        Assert.Equal(str.ToLower(), lowerId.ToString());
                    }
                }
            }
        }

        [Fact]
        public async Task GetSliceFromId_WhenDisposing_ShouldFreeMemory()
        {
            using (var store = GetDocumentStore())
            {
                var database = await GetDocumentDatabaseInstanceFor(store);
                using (database.DocumentsStorage.ContextPool.AllocateOperationContext(out DocumentsOperationContext ctx))
                {
                    var before = ctx.AllocatedMemory;
                    using (DocumentIdWorker.GetSliceFromId(ctx, "Person@יפתח", out var lowerId))
                    {
                    }
                    var after = ctx.AllocatedMemory;

                    Assert.Equal(before, after);
                }
            }
        }
    }
}
