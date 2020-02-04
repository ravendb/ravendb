using System;
using System.Threading.Tasks;
using FastTests;
using Raven.Server.ServerWide.Context;
using Sparrow.Json;
using Sparrow.Json.Parsing;
using Sparrow.Server;
using Xunit;
using Xunit.Abstractions;

namespace SlowTests.Issues
{
    public class RavenDB_13029 : RavenLowLevelTestBase
    {
        public RavenDB_13029(ITestOutputHelper output) : base(output)
        {
        }

        [Fact]
        public async Task ReadTransactionShouldNotSeeCollectionsThatWereGeneratedAfterItsCreation()
        {
            AsyncManualResetEvent readEvent = new AsyncManualResetEvent();
            AsyncManualResetEvent writeEvent = new AsyncManualResetEvent();
            using (var database = CreateDocumentDatabase())
            {
                var readTask = Task.Run(async () =>
                {
                    using (database.DocumentsStorage.ContextPool.AllocateOperationContext(out DocumentsOperationContext readCtx))
                    using (readCtx.OpenReadTransaction())
                    {
                        readEvent.Set();
                        Assert.True(await writeEvent.WaitAsync(TimeSpan.FromSeconds(60)));
                        var collection = database.DocumentsStorage.GetCollections(readCtx);
                        Assert.Empty(collection);
                    }
                });
                var writeTask = Task.Run(async () =>
                {
                    Assert.True(await readEvent.WaitAsync(TimeSpan.FromSeconds(60)));
                    using (database.DocumentsStorage.ContextPool.AllocateOperationContext(out DocumentsOperationContext writeCtx))
                    using (var tx = writeCtx.OpenWriteTransaction())
                    using (var doc = writeCtx.ReadObject(new DynamicJsonValue
                    {
                        ["Name"] = "Oren",
                        ["@metadata"] = new DynamicJsonValue
                        {
                            ["@collection"] = "Users"
                        }
                    }, "users/1", BlittableJsonDocumentBuilder.UsageMode.ToDisk))
                    {
                        database.DocumentsStorage.Put(writeCtx, "users/1", null, doc);
                        tx.Commit();
                    }

                    writeEvent.Set();
                });
                await readTask;
            }
        }
    }
}
