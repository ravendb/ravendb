using System;
using System.Collections.Generic;
using System.Linq;
using System.Text;
using System.Threading.Tasks;
using FastTests;
using Raven.Client.ServerWide.Operations;
using Raven.Server.Config;
using Tests.Infrastructure;
using Xunit;
using Xunit.Abstractions;

namespace SlowTests.Issues
{
    public class RavenDB_17903 : ClusterTestBase
    {
        public RavenDB_17903(ITestOutputHelper output) : base(output)
        {
        }

        [Fact]
        public async Task BulkInsertFailover_TestCase1()
        {
            using var server = GetNewServer(new ServerCreationOptions { RunInMemory = false, });
            using var store = GetDocumentStore(new Options { RunInMemory = false, Server = server });

            await using (var bulk = store.BulkInsert())
            {
                await bulk.StoreAsync(new TestObj(), $"testObjs/0");
            }
            
            var result = await DisposeServerAndWaitForFinishOfDisposalAsync(server);

            await Assert.ThrowsAnyAsync<Exception>(async () =>
            {
                await using (var bulk = store.BulkInsert())
                {
                    await bulk.StoreAsync(new TestObj(), $"testObjs/0");
                }
            });

            using var newServer = GetNewServer(new ServerCreationOptions
            {
                DeletePrevious = false,
                RunInMemory = false,
                DataDirectory = result.DataDirectory,
                CustomSettings = new Dictionary<string, string> { [RavenConfiguration.GetKey(x => x.Core.ServerUrls)] = result.Url }
            });

            await using (var bulk = store.BulkInsert())
            {
                await bulk.StoreAsync(new TestObj(), $"testObjs/0");
            }
        }

        [Fact]
        public async Task BulkInsertFailover_TestCase2()
        {
            (var nodes, var leader) = await CreateRaftCluster(2, shouldRunInMemory: false);

            using var store = GetDocumentStore(new Options { RunInMemory = false, Server = leader, ReplicationFactor = 1 });

            var record = await store.Maintenance.Server.SendAsync(new GetDatabaseRecordOperation(store.Database));
            var databaseNodeTag = record.Topology.AllNodes.First();
            var nodeToRestart = nodes.First(n => n.ServerStore.NodeTag == databaseNodeTag);

            await using (var bulk = store.BulkInsert())
            {
                await bulk.StoreAsync(new TestObj(), $"testObjs/0");
            }
            var result = await DisposeServerAndWaitForFinishOfDisposalAsync(nodeToRestart);

            await Assert.ThrowsAnyAsync<Exception>(async () =>
            {
                await using (var bulk = store.BulkInsert())
                {
                    await bulk.StoreAsync(new TestObj(), $"testObjs/0");
                }
            });

            using var newServer = GetNewServer(new ServerCreationOptions
            {
                DeletePrevious = false,
                RunInMemory = false,
                DataDirectory = result.DataDirectory,
                CustomSettings = new Dictionary<string, string> { [RavenConfiguration.GetKey(x => x.Core.ServerUrls)] = result.Url }
            });

            await using (var bulk = store.BulkInsert())
            {
                await bulk.StoreAsync(new TestObj(), $"testObjs/0");
            }
        }

        class TestObj
        {
            public string Id { get; set; }
        }
    }
}
