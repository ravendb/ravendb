using System;
using System.Collections.Generic;
using System.Linq;
using System.Net;
using System.Text;
using System.Threading;
using System.Threading.Tasks;
using FastTests.Graph;
using Jint;
using Raven.Client.Documents;
using Raven.Client.Documents.Commands.Batches;
using Raven.Client.Documents.Conventions;
using Raven.Client.Documents.Session;
using Raven.Client.Exceptions;
using Raven.Server;
using Raven.Server.Config;
using Tests.Infrastructure;
using Xunit;
using Xunit.Abstractions;

namespace SlowTests.Issues
{
    public class RavenDB_20628 : ClusterTestBase
    {
        public RavenDB_20628(ITestOutputHelper output) : base(output)
        {
        }

        [Fact]
        public async Task RequestExecutor_With_CanellationToken_Should_Throw_In_Timeout_When_ClusterWideTransaction_Is_Slow()
        {
            using var store = GetDocumentStore();

            var user1 = new User()
            {
                Id = "Users/1-A", 
                Name = "Alice"
            };


            using (var session = store.OpenAsyncSession(new SessionOptions { TransactionMode = TransactionMode.ClusterWide }))
            {
                await session.StoreAsync(user1);
                await session.SaveChangesAsync();
            }

            var db = await Server.ServerStore.DatabasesLandlord.TryGetOrCreateResourceStore(store.Database);
            db.ForTestingPurposesOnly().AfterCommitInClusterTransaction = () =>
            {
                return Task.Delay(15_000);
            };

            var e = await Assert.ThrowsAsync<TaskCanceledException>( async () =>
            {
                using (var cts = new CancellationTokenSource(TimeSpan.FromSeconds(1)))
                using (var session = store.OpenAsyncSession(new SessionOptions { TransactionMode = TransactionMode.ClusterWide }))
                {
                    var user2 = await session.LoadAsync<User>(user1.Id);
                    user2.Name = "Bob";
                    await session.SaveChangesAsync(cts.Token);
                }
            });

            Assert.NotNull(e);
            Assert.Contains("RequestExecutor", e.StackTrace);
            Assert.Contains("HttpClient", e.StackTrace);
        }
    }
}
