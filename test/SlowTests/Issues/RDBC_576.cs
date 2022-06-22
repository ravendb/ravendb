using System;
using System.Collections.Generic;
using System.Linq;
using System.Threading.Tasks;
using Raven.Client.Documents;
using Raven.Client.Documents.Operations;
using Raven.Client.ServerWide;
using Raven.Client.ServerWide.Operations;
using Raven.Server.Config;
using Raven.Tests.Core.Utils.Entities;
using SlowTests.Bugs.Caching;
using Tests.Infrastructure;
using Xunit;
using Xunit.Abstractions;

namespace SlowTests.Issues;

public class RDBC_576 : ClusterTestBase
{
    public RDBC_576(ITestOutputHelper output) : base(output)
    {
    }
    
    [Fact]
    public async Task Should_Create_New_Bulk_Insert_If_Previously_Failed_On_Unavailable_Server_1()
    {
        using var server = GetNewServer(new ServerCreationOptions {RunInMemory = false,});
        using var store = GetDocumentStore(new Options {RunInMemory = false, Server = server});

        await using (var bulk = store.BulkInsert())
        {
            await bulk.StoreAsync(new User(), "omer/1");
        }
        var result = await DisposeServerAndWaitForFinishOfDisposalAsync(server);

        await Assert.ThrowsAnyAsync<Exception>(async () =>
        {
            await using (var bulk = store.BulkInsert())
            {
                await bulk.StoreAsync(new User(), "omer/2");
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
            await bulk.StoreAsync(new User(), "omer/2");
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

        await using (var bulk = store.BulkInsert())
        {
            await bulk.StoreAsync(new User(), "omer/1");
        }
        
        var result = await DisposeServerAndWaitForFinishOfDisposalAsync(nodeToRestart);

        await Assert.ThrowsAnyAsync<Exception>(async () =>
        {
            await using (var bulk = store.BulkInsert())
            {
                await bulk.StoreAsync(new User(), "omer/2");
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
            await bulk.StoreAsync(new User(), "omer/2");
        }
    }
    
    [Fact]
    public async Task Should_Not_Throw_Node_Unavailable()
    {
        var path = NewDataPath();
        var dbName = GetDatabaseName();
        using var server = GetNewServer(new ServerCreationOptions
        {
            RunInMemory = false,
            CustomSettings = new Dictionary<string, string>
            {
                [RavenConfiguration.GetKey(x => x.Core.DataDirectory)] = path 
            },
        });

        using var store = new DocumentStore {Urls = new[] {server.WebUrl}, Database = dbName}.Initialize();
        var dbRecord = new DatabaseRecord(dbName);
        store.Maintenance.Server.Send(new CreateDatabaseOperation(dbRecord));
        
        using (var session = store.OpenSession())
        {
            session.Store(new User {Name = "Omer"});
            session.SaveChanges();
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
            DeletePrevious = false,
            RunInMemory = false,
            CustomSettings = new Dictionary<string, string>
            {
                [RavenConfiguration.GetKey(x => x.Core.ServerUrls)] = result.Url, 
                [RavenConfiguration.GetKey(x => x.Core.DataDirectory)] = path
            },
        });
        
        Operation deleteOperation = await store
            .Operations
            .SendAsync(new DeleteByQueryOperation("from Users"))
            .ConfigureAwait(false);

        await deleteOperation.WaitForCompletionAsync().ConfigureAwait(false);
    }
}

