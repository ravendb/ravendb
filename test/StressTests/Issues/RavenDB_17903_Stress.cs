using System;
using System.Collections.Generic;
using System.Linq;
using System.Threading.Tasks;
using Raven.Client.ServerWide.Operations;
using Raven.Server.Config;
using Raven.Tests.Core.Utils.Entities;
using Tests.Infrastructure;
using Xunit;
using Xunit.Abstractions;

namespace StressTests.Issues;

public class RavenDB_17903_Stress : ClusterTestBase
{
    public RavenDB_17903_Stress(ITestOutputHelper output) : base(output)
    {
    }

    [Fact]
    public async Task Bulk_Insert_1NodeRestart_TestCase2()
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

    [Fact]
    public async Task Should_Create_New_Bulk_Insert_If_Previously_Failed_On_Unavailable_Server_2()
    {
        (var nodes, var leader) = await CreateRaftCluster(2, shouldRunInMemory: false);

        using var store = GetDocumentStore(new Options { RunInMemory = false, Server = leader, ReplicationFactor = 1 });

        var record = await store.Maintenance.Server.SendAsync(new GetDatabaseRecordOperation(store.Database));
        var databaseNodeTag = record.Topology.AllNodes.First();
        var nodeToRestart = nodes.First(n => n.ServerStore.NodeTag == databaseNodeTag);

        var result = await DisposeServerAndWaitForFinishOfDisposalAsync(nodeToRestart);

        await Assert.ThrowsAnyAsync<Exception>(async () =>
        {
            await using (var bulk = store.BulkInsert())
            {
                await bulk.StoreAsync(new User(), "user/1");
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
            await bulk.StoreAsync(new User(), "user/1");
        }
    }


    private class TestObj
    {
        public string Id { get; set; }
    }
}
