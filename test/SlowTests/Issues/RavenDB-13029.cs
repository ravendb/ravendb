using System.Threading.Tasks;
using FastTests;
using Raven.Server.ServerWide.Context;
using Sparrow;
using Sparrow.Json;
using Sparrow.Json.Parsing;
using Xunit;

namespace SlowTests.Issues
{
    public class RavenDB_13029 : RavenLowLevelTestBase
    {
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
                        await writeEvent.WaitAsync();
                        var collection = database.DocumentsStorage.GetCollections(readCtx);
                        Assert.Empty(collection);
                    }
                });
                var writeTask = Task.Run(async () =>
                {
                    await readEvent.WaitAsync();
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
