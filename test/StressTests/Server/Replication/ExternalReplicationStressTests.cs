using System;
using System.Collections.Generic;
using System.Linq;
using System.Threading;
using System.Threading.Tasks;
using FastTests;
using FastTests.Server.Replication;
using Raven.Client.Documents.Operations;
using Raven.Client.Documents.Operations.Replication;
using Raven.Server.Config;
using SlowTests.Server.Replication;
using Tests.Infrastructure;
using Xunit;
using Xunit.Abstractions;

namespace StressTests.Server.Replication
{
    public class ExternalReplicationStressTests : ReplicationTestBase
    {
        public ExternalReplicationStressTests(ITestOutputHelper output) : base(output)
        {
        }

        [Fact]
        public async Task TwoWayExternalReplicationShouldNotLoadIdleDatabase()
        {
            using (var server = GetNewServer(new ServerCreationOptions
            {
                CustomSettings = new Dictionary<string, string>
                {
                    [RavenConfiguration.GetKey(x => x.Databases.MaxIdleTime)] = "10",
                    [RavenConfiguration.GetKey(x => x.Databases.FrequencyToCheckForIdle)] = "3",
                    [RavenConfiguration.GetKey(x => x.Core.RunInMemory)] = "false"

                }
            }))
            using (var store1 = GetDocumentStore(new Options
            {
                Server = server,
                RunInMemory = false

            }))
            using (var store2 = GetDocumentStore(new Options
            {
                Server = server,
                RunInMemory = false
            }))
            {
                var externalTask1 = new ExternalReplication(store2.Database, "MyConnectionString1")
                {
                    Name = "MyExternalReplication1"
                };

                var externalTask2 = new ExternalReplication(store1.Database, "MyConnectionString2")
                {
                    Name = "MyExternalReplication2"
                };
                await AddWatcherToReplicationTopology(store1, externalTask1);
                await AddWatcherToReplicationTopology(store2, externalTask2);

                Assert.True(server.ServerStore.DatabasesLandlord.LastRecentlyUsed.TryGetValue(store1.Database, out _));
                Assert.True(server.ServerStore.DatabasesLandlord.LastRecentlyUsed.TryGetValue(store2.Database, out _));

                var now = DateTime.Now;
                var nextNow = now + TimeSpan.FromSeconds(60);
                while (now < nextNow && server.ServerStore.IdleDatabases.Count < 2)
                {
                    Thread.Sleep(3000);
                    now = DateTime.Now;
                }
                Assert.Equal(2, server.ServerStore.IdleDatabases.Count);

                await store1.Maintenance.SendAsync(new CreateSampleDataOperation());
                WaitForIndexing(store1);

                var count = 0;
                var docs = store1.Maintenance.Send(new GetStatisticsOperation()).CountOfDocuments;
                var replicatedDocs = store2.Maintenance.Send(new GetStatisticsOperation()).CountOfDocuments;
                while (docs != replicatedDocs && count < 20)
                {
                    Thread.Sleep(3000);
                    replicatedDocs = store2.Maintenance.Send(new GetStatisticsOperation()).CountOfDocuments;
                    count++;
                }
                Assert.Equal(docs, replicatedDocs);

                count = 0;
                nextNow = DateTime.Now + TimeSpan.FromMinutes(5);
                while (server.ServerStore.IdleDatabases.Count == 0 && now < nextNow)
                {
                    Thread.Sleep(500);
                    if (count % 10 == 0)
                        store1.Maintenance.Send(new GetStatisticsOperation());

                    now = DateTime.Now;
                    count++;
                }
                Assert.Equal(1, server.ServerStore.IdleDatabases.Count);

                nextNow = DateTime.Now + TimeSpan.FromSeconds(15);
                while (now < nextNow)
                {
                    Thread.Sleep(2000);
                    store1.Maintenance.Send(new GetStatisticsOperation());
                    Assert.Equal(1, server.ServerStore.IdleDatabases.Count);
                    now = DateTime.Now;
                }

                nextNow = DateTime.Now + TimeSpan.FromMinutes(10);
                while (now < nextNow && server.ServerStore.IdleDatabases.Count < 2)
                {
                    Thread.Sleep(3000);
                    now = DateTime.Now;
                }
                Assert.Equal(2, server.ServerStore.IdleDatabases.Count);

                using (var s = store2.OpenSession())
                {
                    s.Advanced.RawQuery<dynamic>("from @all_docs")
                        .ToList();
                }
                Assert.Equal(1, server.ServerStore.IdleDatabases.Count);

                var operation = await store2
                    .Operations
                    .ForDatabase(store2.Database)
                    .SendAsync(new PatchByQueryOperation("from Companies update { this.Name = this.Name + '_patched'; }"));

                await operation.WaitForCompletionAsync();

                nextNow = DateTime.Now + TimeSpan.FromMinutes(2);
                while (now < nextNow && server.ServerStore.IdleDatabases.Count > 0)
                {
                    Thread.Sleep(5000);
                    now = DateTime.Now;
                }
                Assert.Equal(0, server.ServerStore.IdleDatabases.Count);

                nextNow = DateTime.Now + TimeSpan.FromMinutes(10);
                while (server.ServerStore.IdleDatabases.Count == 0 && now < nextNow)
                {
                    Thread.Sleep(500);
                    if (count % 10 == 0)
                        store2.Maintenance.Send(new GetStatisticsOperation());

                    now = DateTime.Now;
                    count++;
                }
                Assert.Equal(1, server.ServerStore.IdleDatabases.Count);

                nextNow = DateTime.Now + TimeSpan.FromSeconds(15);
                while (now < nextNow)
                {
                    Thread.Sleep(2000);
                    store2.Maintenance.Send(new GetStatisticsOperation());
                    Assert.Equal(1, server.ServerStore.IdleDatabases.Count);
                    now = DateTime.Now;
                }
            }
        }

    }
}
