using System;
using System.Diagnostics;
using System.Runtime.InteropServices;
using System.Text;
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
        public DocumentIdWorkerTests(ITestOutputHelper output) : base(output)
        {
        }

        [Fact]
        public async Task GetLower_WhenStringAscii_ShouldNotModifyTheValueAcceptToLower()
        {
            using (var store = GetDocumentStore())
            {
                var database = await GetDocumentDatabaseInstanceFor(store);
                using (database.DocumentsStorage.ContextPool.AllocateOperationContext(out DocumentsOperationContext ctx))
                {
                    for (var i = 32; i <= 127; i++)
                    {
                        using (var str = ctx.GetLazyString("Person@1" + (char)i))
                        {
                            using (DocumentIdWorker.GetLower(ctx.Allocator, str, out var lowerId))
                            {
                                Assert.Equal(str.ToLower(), lowerId.ToString());
                            }
                        }
                    }
                }
            }
        }

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
        public void GetSliceFromId_WhenEmptyLazyString_ShouldNotThrow()
        {
            using var ctx = DocumentsOperationContext.ShortTermSingleUse(null);
            const string str = "";
            var lazyString = ctx.GetLazyString(str);
            using (DocumentIdWorker.GetSliceFromId(ctx, lazyString, out var lowerId))
            {
                Assert.Equal(str.ToLower(), lowerId.ToString());
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
