using System.IO;
using System.Threading;
using System.Threading.Tasks;
using Tests.Infrastructure;
using Xunit.Abstractions;

namespace SlowTests.Issues
{
    public class RavenDB_18916 : ReplicationTestBase
    {
        public RavenDB_18916(ITestOutputHelper output) : base(output)
        {
        }

        [RavenTheory(RavenTestCategory.Replication)]
        [RavenData(DatabaseMode = RavenDatabaseMode.All)]
        public async Task DeadLockTest(Options options)
        {
            var server = GetNewServer();

            using var store1 = GetDocumentStore(new Options(options) { Server = server, ReplicationFactor = 1 });
            using var store2 = GetDocumentStore(new Options(options) { Server = server, ReplicationFactor = 1 });

            var database = await GetDocumentDatabaseInstanceForAsync(store2, options.DatabaseMode, "Users/2-A", server);
            var replicationLoader = database.ReplicationLoader;

            var handlersMre = new ManualResetEvent(false);
            replicationLoader.ForTestingPurposesOnly().OnIncomingReplicationHandlerStart = () =>
            {
                throw new EndOfStreamException("Shahar");
            };
            replicationLoader.ForTestingPurposesOnly().OnIncomingReplicationHandlerFailure = (e) =>
            {
                handlersMre.WaitOne();
            };
            replicationLoader.ForTestingPurposesOnly().BeforeDisposingIncomingReplicationHandlers = () =>
            {
                handlersMre.Set();
            };

            await SetupReplicationAsync(store1, store2);

            using (var session = store1.OpenAsyncSession())
            {
                await session.StoreAsync(new User { Id = "Users/2-A", Name = "Shahar" });
                await session.SaveChangesAsync();
            }

            await DisposeServerAsync(server, 20_000); // Shouldnt throw "System.InvalidOperationException: Could not dispose server with URL.."
        }

        private class User
        {
            public string Id { get; set; }
            public string Name { get; set; }
        }
    }
}
