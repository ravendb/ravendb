using System;
using System.Collections.Generic;
using System.Linq;
using System.Threading;
using System.Threading.Tasks;
using FastTests;
using Nito.AsyncEx;
using Raven.Client.Documents;
using Raven.Client.Documents.Operations;
using Raven.Client.Documents.Session;
using Raven.Client.Exceptions;
using Raven.Server.Config;
using Xunit;
using Xunit.Abstractions;

namespace SlowTests.Server
{
    public class TransactionMergerTests : RavenTestBase
    {
        public TransactionMergerTests(ITestOutputHelper output) : base(output)
        {
        }

        protected override DocumentStore GetDocumentStore(Options options = null, string caller = null)
        {
            options ??= new Options();
            var modifyDatabaseRecord = options.ModifyDatabaseRecord;
            options.ModifyDatabaseRecord = r =>
            {
                modifyDatabaseRecord?.Invoke(r);
                r.Settings[RavenConfiguration.GetKey(x => x.TransactionMergerConfiguration.MaxTimeToWaitForPreviousTx)] = int.MaxValue.ToString();
            };
            return base.GetDocumentStore(options, caller);
        }

        class TestObj
        {
            public string Id { get; set; }
            public string Prop { get; set; }
        }

        [Fact]
        public async Task RerunMergedTransactionCommand_WhenPatchByQuery_ShouldPatchAllRelevantDocs()
        {
            const int docCount = 10;

            using var store = GetDocumentStore(new Options
            {
                ModifyDatabaseRecord = record => record.Settings[RavenConfiguration.GetKey(x => x.Patching.MaxStepsForScript)] = int.MaxValue.ToString()
            });

            using (var session = store.OpenAsyncSession())
            {
                for (int i = 0; i < docCount; i++)
                {
                    await session.StoreAsync(new TestObj(), $"testObjs/{i}");
                }
                await session.SaveChangesAsync();
            }

            var tokenSource = new CancellationTokenSource();
            var amre = new AsyncManualResetEvent();
            var failingTasks = RunFailingTasks(store, amre, tokenSource.Token);

            WaitForIndexing(store);
            await amre.WaitAsync();
            var operation = await store.Operations.SendAsync(new PatchByQueryOperation(@"
from TestObjs as o where o.Prop = null update 
{{ 
    this.Prop = 'Changed';
    var b = 1;
    for(var i = 1; i < 100000; i++){{
        b *= i;
    }} 
}}"));
            await operation.WaitForCompletionAsync();
            tokenSource.Cancel();
            await Task.WhenAll(failingTasks);

            using (var session = store.OpenAsyncSession())
            {
                WaitForIndexing(store);
                var patchCount = await session.Query<TestObj>().Where(o => o.Prop == "Changed").CountAsync();
                Assert.Equal(docCount, patchCount);
            }
        }

        private static Task RunFailingTasks(IDocumentStore store, AsyncManualResetEvent amre, CancellationToken token)
        {
            const int taskCount = 10;
            var count = 0;
            return Task.WhenAll(Enumerable.Range(0, taskCount).Select(async _ =>
            {
                var first = true;
                while (token.IsCancellationRequested == false)
                {
                    try
                    {
                        using var session = store.OpenAsyncSession();
                        await session.StoreAsync(new TestObj(), "FailedChangeVector", "testObjs/something", token);
                        var task = session.SaveChangesAsync(token);
                        if (first)
                        {
                            first = false;
                            if (Interlocked.Increment(ref count) == taskCount / 2)
                                amre.Set();
                        }

                        await task;
                    }
                    catch (Exception e) when (e is ConcurrencyException or TaskCanceledException)
                    {
                        //ignore
                    }
                }
            }));
        }
    }
}
