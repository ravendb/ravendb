using System;
using System.Collections.Generic;
using System.Linq;
using System.Text;
using System.Threading.Tasks;
using FastTests.Graph;
using Jint;
using Raven.Client.Documents.Commands.Batches;
using Raven.Client.Documents.Session;
using Raven.Client.Exceptions;
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
        public async Task ClusterWideTransactionShouldThrowTimeoutException()
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
            db.ForTestingPurposesOnly().WaitForDatabaseResultsTimeoutInClusterTransaction = (command) =>
            {
                if (command.ParsedCommands.Any(cmd => cmd.Type == CommandType.PUT))
                    return TimeSpan.Zero;
                else
                    return null;
            };

            var e = await Assert.ThrowsAsync<RavenException>( async () =>
            {
                using (var session = store.OpenAsyncSession(new SessionOptions { TransactionMode = TransactionMode.ClusterWide }))
                {
                    var user2 = await session.LoadAsync<User>(user1.Id);
                    user2.Name = "Bob";
                    await session.SaveChangesAsync();
                }
            });

            Assert.NotNull(e);
            Assert.True(e.InnerException is TimeoutException);
        }
    }
}
