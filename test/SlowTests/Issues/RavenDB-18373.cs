using System;
using System.Linq;
using System.Threading.Tasks;
using FastTests;
using Raven.Client.Documents.Indexes;
using Raven.Client.Documents.Operations.Indexes;
using Raven.Client.Documents.Session;
using Raven.Client.Exceptions;
using Xunit;
using Xunit.Abstractions;

namespace SlowTests.Issues
{
    public class RavenDB_18373 : RavenTestBase
    {
        public RavenDB_18373(ITestOutputHelper output) : base(output)
        {
        }

        class TestObj
        {
            public string Id { get; set; }
            public string Prop { get; set; }
        }

        private class Simple_Map_Index : AbstractIndexCreationTask<TestObj>
        {
            public Simple_Map_Index()
            {
                Map = companies => from c in companies
                    select new
                    {
                        Name = ""
                    };
            }
        }

        [Theory]
        [InlineData(TransactionMode.ClusterWide)]
        [InlineData(TransactionMode.SingleNode)]
        public async Task Waiting_For_Index_Doesnt_Work_For_ClusterWideTransaction(TransactionMode transactionMode)
        {
            using var store = GetDocumentStore();

            await new Simple_Map_Index().ExecuteAsync(store);
            await store.Maintenance.SendAsync(new StopIndexingOperation());

            using (var session = store.OpenAsyncSession(new SessionOptions {
                       TransactionMode = transactionMode
            }))
            {
                session.Advanced.WaitForIndexesAfterSaveChanges(TimeSpan.FromSeconds(3));

                await session.StoreAsync(new TestObj(), "testObjs/0");
                await Assert.ThrowsAsync<RavenTimeoutException>(async () => await session.SaveChangesAsync());
            }
        }

        [Theory]
        [InlineData(TransactionMode.ClusterWide)]
        [InlineData(TransactionMode.SingleNode)]
        public async Task Waiting_For_Index_Doesnt_Work_For_ClusterWideTransaction_Delete(TransactionMode transactionMode)
        {
            using var store = GetDocumentStore();

            await new Simple_Map_Index().ExecuteAsync(store);

            using (var session = store.OpenAsyncSession(new SessionOptions { TransactionMode = transactionMode }))
            {
                session.Advanced.WaitForIndexesAfterSaveChanges(TimeSpan.FromSeconds(3));
                await session.StoreAsync(new TestObj(), "testObjs/0");
                await session.StoreAsync(new TestObj(), "testObjs/1");
                await session.SaveChangesAsync();
            }

            Indexes.WaitForIndexing(store, timeout: TimeSpan.FromSeconds(15));

            await store.Maintenance.SendAsync(new StopIndexingOperation());

            using (var session = store.OpenAsyncSession(new SessionOptions
                   {
                       TransactionMode = transactionMode
                   }))
            {
                session.Advanced.WaitForIndexesAfterSaveChanges(TimeSpan.FromSeconds(3));

                session.Delete("testObjs/0");
                await Assert.ThrowsAsync<RavenTimeoutException>(async () => await session.SaveChangesAsync());
            }
        }
    }
}
