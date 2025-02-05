using System;
using System.Collections.Generic;
using System.Linq;
using System.Text;
using System.Threading.Tasks;
using Raven.Client.Documents.Session;
using Raven.Server.Utils;
using Tests.Infrastructure;
using Xunit.Abstractions;

namespace SlowTests.Issues
{
    public class RavenDB_22419 : ClusterTestBase
    {
        public RavenDB_22419(ITestOutputHelper output) : base(output)
        {
        }

        [RavenTheory(RavenTestCategory.ClusterTransactions)]
        [RavenData(DatabaseMode = RavenDatabaseMode.All)]
        public async Task Dispose_Leader_Without_Changing_State_Shuoldnt_Cause_The_Leader_To_Stuck(Options options)
        {
            DebuggerAttachedTimeout.DisableLongTimespan = true;
            var (_, leader) = await CreateRaftCluster(2);
            options.Server = leader;
            options.ReplicationFactor = 2;

            using (var leaderStore = GetDocumentStore(options))
            {
                using (var session = leaderStore.OpenAsyncSession(new SessionOptions
                       {
                           TransactionMode = TransactionMode.ClusterWide
                       }))
                {
                    session.Advanced.ClusterTransaction.CreateCompareExchangeValue("usernames/1", new User());
                    await session.SaveChangesAsync();
                }

                await ActionWithLeader((l) =>
                {
                    l.ServerStore.Engine.CurrentLeader?.Dispose();
                    return Task.CompletedTask;
                });

                using (var session = leaderStore.OpenAsyncSession(new SessionOptions
                       {
                           TransactionMode = TransactionMode.ClusterWide
                       }))
                {
                    session.Advanced.ClusterTransaction.CreateCompareExchangeValue("usernames/2", new User());
                    await session.SaveChangesAsync();
                }
            }
        }

        public class User
        {
            public string Id { get; set; }
            public string Name { get; set; }
        }
    }
}
