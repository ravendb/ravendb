using System;
using System.Linq;
using System.Threading;
using System.Threading.Tasks;
using FastTests;
using Nito.AsyncEx;
using Raven.Client.Documents;
using Raven.Client.Documents.Indexes;
using Raven.Client.Documents.Operations;
using Raven.Server.Config;
using Raven.Server.Documents;
using Raven.Server.Documents.TransactionMerger;
using Raven.Server.Documents.TransactionMerger.Commands;
using Raven.Server.ServerWide.Context;
using Sparrow.Json;
using Xunit;
using Xunit.Abstractions;

namespace SlowTests.Server
{
    public class TransactionMergerTests : RavenTestBase
    {
        public TransactionMergerTests(ITestOutputHelper output) : base(output)
        {
        }

        protected internal override DocumentStore GetDocumentStore(Options options = null, string caller = null)
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

        private class TestObj
        {
            public string Id { get; set; }
            public string Prop { get; set; }
        }

        private class TestIndex : AbstractIndexCreationTask<TestObj, TestIndex.Result>
        {
            public class Result
            {
                public string KeyProp { get; set; }
            }
            
            public TestIndex()
            {
                Map = testObjs =>
                    from testObj in testObjs
                    select new Result
                    {
                        KeyProp = testObj.Prop,
                    };

                Reduce = results =>
                    from r in results
                    group r by r.KeyProp
                    into g
                    select new Result
                    {
                        KeyProp = g.Key,
                    };

                OutputReduceToCollection = "OutputReduceCollection";
                PatternForOutputReduceToCollectionReferences = x => $"someprefix/{x.KeyProp}";
            }
        }
        
        [Fact]
        public async Task RerunMergedTransactionCommand_WhenReduceOutputToCollectionResultDidntChange_ShouldKeepOutputCollctionDocuments()
        {
            using var store = GetDocumentStore();
            const string prop = "0";
            
            await new TestIndex().ExecuteAsync(store);
            using (var session = store.OpenAsyncSession())
            {
                await session.StoreAsync(new TestObj { Prop = prop}, $"testObjs/0");
                await session.SaveChangesAsync();
            }

            Indexes.WaitForIndexing(store);
            
            using var tokenSource = new AutoCancellationTokenSource();
            
            var amre = new AsyncManualResetEvent();
            var failingTasks = RunFailingTasks(store, amre, tokenSource.Token);

            using (var session = store.OpenAsyncSession())
            {
                Assert.NotNull(await session.LoadAsync<object>("someprefix/0"));
                Assert.NotEmpty(await session.Advanced.AsyncRawQuery<object>("from OutputReduceCollection").ToArrayAsync());
            }
            using (var session = store.OpenAsyncSession())
            {
                await session.StoreAsync(new TestObj { Prop = prop}, $"testObjs/1");
                await session.SaveChangesAsync();
            }
            Indexes.WaitForIndexing(store);

            using (var session = store.OpenAsyncSession())
            {
                Assert.NotNull(await session.LoadAsync<object>("someprefix/0"));
                Assert.NotEmpty(await session.Advanced.AsyncRawQuery<object>("from OutputReduceCollection").ToArrayAsync());
            }
            
            tokenSource.Cancel();
            await Task.WhenAll(failingTasks);
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

            using var tokenSource = new AutoCancellationTokenSource();
            var amre = new AsyncManualResetEvent();
            var failingTasks = RunFailingTasks(store, amre, tokenSource.Token);

            Indexes.WaitForIndexing(store);
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
            await operation.WaitForCompletionAsync(TimeSpan.FromMinutes(5));
            tokenSource.Cancel();
            await Task.WhenAll(failingTasks);

            using (var session = store.OpenAsyncSession())
            {
                Indexes.WaitForIndexing(store);
                var patchCount = await session.Query<TestObj>().Where(o => o.Prop == "Changed").CountAsync();
                Assert.Equal(docCount, patchCount);
            }
        }

        private async Task RunFailingTasks(IDocumentStore store, AsyncManualResetEvent amre, CancellationToken token)
        {
            var database = await GetDatabase(store.Database);
            while (token.IsCancellationRequested == false)
            {
                await database.TxMerger.Enqueue(new FailedCommand()).ContinueWith(_ => string.Empty);
                amre.Set();
            }
        }
        
        private class TestException : Exception
        {
        
        }
    
        private class FailedCommand : MergedTransactionCommand<DocumentsOperationContext, DocumentsTransaction>
        {
            protected override long ExecuteCmd(DocumentsOperationContext context) => throw new TestException();

            public override IReplayableCommandDto<DocumentsOperationContext, DocumentsTransaction, MergedTransactionCommand<DocumentsOperationContext, DocumentsTransaction>> ToDto(DocumentsOperationContext context) =>
                throw new NotImplementedException();
        }

        private class AutoCancellationTokenSource : CancellationTokenSource
        {
            protected override void Dispose(bool disposing)
            {
                Cancel();
                base.Dispose(disposing);
            }
        }
    }
}
