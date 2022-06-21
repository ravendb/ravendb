using System;
using System.Collections.Generic;
using System.Linq;
using System.Threading.Tasks;
using Raven.Client.Documents.Operations;
using Raven.Client.ServerWide.Operations;
using Raven.Server.Config;
using Raven.Tests.Core.Utils.Entities;
using Tests.Infrastructure;
using Xunit;
using Xunit.Abstractions;

namespace FastTests.Issues;

public class RDBC_576 : ClusterTestBase
{
    public RDBC_576(ITestOutputHelper output) : base(output)
    {
    }

    [Fact]
    public async Task Should_Create_New_Bulk_Insert_If_Previously_Failed_On_Unavailable_Server_1()
    {
        using var server = GetNewServer();
        using var store = GetDocumentStore(new Options {Server = server});

        var result = await DisposeServerAndWaitForFinishOfDisposalAsync(server);

        await Assert.ThrowsAnyAsync<Exception>(async () =>
        {
            await using (var bulk = store.BulkInsert())
            {
                await bulk.StoreAsync(new User(), "user/1");
            }
        });

        using var newServer = GetNewServer(new ServerCreationOptions
        {
            CustomSettings = new Dictionary<string, string>
            {
                [RavenConfiguration.GetKey(x => x.Core.ServerUrls)] = result.Url
            }
        });

        await using (var bulk = store.BulkInsert())
        {
            await bulk.StoreAsync(new User(), "user/1");
        }
    }

    [Fact]
    public async Task Should_Create_New_Bulk_Insert_If_Previously_Failed_On_Unavailable_Server_2()
    {
        (var nodes, var leader) = await CreateRaftCluster(2, shouldRunInMemory: false);

        using var store = GetDocumentStore(new Options {RunInMemory = false, Server = leader, ReplicationFactor = 1});

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
            CustomSettings = new Dictionary<string, string> {[RavenConfiguration.GetKey(x => x.Core.ServerUrls)] = result.Url}
        });

        await using (var bulk = store.BulkInsert())
        {
            await bulk.StoreAsync(new User(), "user/1");
        }
    }

    [Fact]
    public async Task Should_Not_Throw_Node_Unavailable()
    {
        using var server = GetNewServer(new ServerCreationOptions {RunInMemory = false,});
        using var store = GetDocumentStore(new Options {RunInMemory = false, Server = server});

        using (var session = store.OpenSession())
        {
            session.Store(new User {Name = "Omer"});
        }

        var result = await DisposeServerAndWaitForFinishOfDisposalAsync(server);

        await Assert.ThrowsAnyAsync<Exception>(async () =>
        {
            Operation deleteOperation = await store
                .Operations
                .SendAsync(new DeleteByQueryOperation("from Users"))
                .ConfigureAwait(false);

            await deleteOperation.WaitForCompletionAsync().ConfigureAwait(false);
        });

        using var newServer = GetNewServer(new ServerCreationOptions
        {
            CustomSettings = new Dictionary<string, string> {[RavenConfiguration.GetKey(x => x.Core.ServerUrls)] = result.Url}
        });

        Operation deleteOperation = await store
            .Operations
            .SendAsync(new DeleteByQueryOperation("from Users"))
            .ConfigureAwait(false);

        await deleteOperation.WaitForCompletionAsync().ConfigureAwait(false);
    }
}

