using System.Threading;
using System.Threading.Tasks;
using FastTests;
using Raven.Client.Documents.Session;
using Raven.Client.Exceptions.Database;
using Raven.Tests.Core.Utils.Entities;
using Xunit;
using Xunit.Abstractions;

namespace SlowTests.Issues
{
    public class RavenDB_14798 : RavenTestBase
    {
        public RavenDB_14798(ITestOutputHelper output) : base(output)
        {
        }

        [Fact]
        public async Task Should_throw_correct_exception_when_database_disposed_during_cluster_transaction()
        {
            using (var store = GetDocumentStore())
            {
                using (var session = store.OpenAsyncSession(new SessionOptions
                       {
                           TransactionMode = TransactionMode.ClusterWide
                       }))
                {
                    await session.StoreAsync(new User());

                    Task unloadTask = null;
                    var database = await GetDatabase(store.Database);
                    var testingStuff = database.ForTestingPurposesOnly();

                    var afterTxMergerDispose = new ManualResetEvent(false);

                    testingStuff.AfterTxMergerDispose = () => afterTxMergerDispose.Set();
                    testingStuff.BeforeExecutingClusterTransactions = () =>
                    {
                        unloadTask = Task.Run(() => Server.ServerStore.DatabasesLandlord.UnloadDirectly(database.Name));
                        afterTxMergerDispose.WaitOne();
                    };

                    var exception = await Assert.ThrowsAsync<DatabaseDisabledException>(async () => await session.SaveChangesAsync());
                    Assert.Contains("is shutting down", exception.Message);
                    await unloadTask;
                }
            }
        }
    }
}
