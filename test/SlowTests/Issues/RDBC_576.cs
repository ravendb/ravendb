using System;
using System.Collections.Generic;
using System.Linq;
using System.Net.Http;
using System.Text;
using System.Threading;
using System.Threading.Tasks;
using NuGet.ContentModel;
using Raven.Client.Documents;
using Raven.Client.Documents.Conventions;
using Raven.Client.Documents.Operations;
using Raven.Client.Documents.Queries;
using Raven.Client.Extensions;
using Raven.Client.Http;
using Raven.Client.Json;
using Raven.Client.Json.Serialization;
using Raven.Client.ServerWide;
using Raven.Client.ServerWide.Operations;
using Raven.Server.Config;
using Raven.Tests.Core.Utils.Entities;
using SlowTests.Bugs.Caching;
using Sparrow.Json;
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
    
    [Fact]
    public async Task Should_Clear_Sate_Failures_Of_Given_Node()
    {
        var (nodes, leader) = await CreateRaftCluster(3, leaderIndex: 0);
        var dbName = GetDatabaseName();
        var db= await CreateDatabaseInCluster(dbName, 3, leader.WebUrl);
        using (var store = new DocumentStore
               {
                   Urls = new[]
                   {
                       leader.WebUrl,
                       nodes[1].WebUrl,
                       nodes[2].WebUrl
                   },
                   Database = dbName
               }.Initialize())
        {
            (string dataDirectory, string disposedServerUrl, _) = await DisposeServerAndWaitForFinishOfDisposalAsync(nodes[1]);

            Operation deleteOperation = await store
                .Operations
                .SendAsync(new DeleteByQueryOperation("from Users"))
                .ConfigureAwait(false);

            await deleteOperation.WaitForCompletionAsync().ConfigureAwait(false);
            
            
            using var newServer = GetNewServer(new ServerCreationOptions
            {
                DeletePrevious = false,
                RunInMemory = false,
                CustomSettings = new Dictionary<string, string>
                {
                    [RavenConfiguration.GetKey(x => x.Core.ServerUrls)] = disposedServerUrl, 
                    [RavenConfiguration.GetKey(x => x.Core.DataDirectory)] = dataDirectory
                },
            });
            
            using (var session = store.OpenAsyncSession())
            {
                await session.StoreAsync(new User
                    {
                        Name = "Omer"
                    },
                    "Users/1");
                await session.SaveChangesAsync();
            }
            var val = await WaitForValueAsync(async () => await GetMembersCount(store, dbName), 3, 60_000);
            Assert.Equal(3, val);
            
            using (var store2 = new DocumentStore
                   {
                       Urls = new []{nodes[1].WebUrl},
                       Database = dbName,
                       Conventions = new DocumentConventions
                       {
                           DisableTopologyUpdates = true
                       }
                   }.Initialize())
            {
                await WaitForDocumentToReplicateAsync<User>(store2, "Users/1", 1000);
            }
       
            var executor = store.GetRequestExecutor(dbName);
            var command = new DeleteByQueryCommand(DocumentConventions.Default, new IndexQuery {Query = "from Users"});
            using (executor.ContextPool.AllocateOperationContext(out JsonOperationContext ctx))
            {
                await executor.ExecuteAsync(new ServerNode
                {
                    Url = nodes[1].WebUrl,
                    Database = dbName,
                    ClusterTag = nodes[1].ServerStore.NodeTag,
                },null, ctx, command);
                var nodeTag = nodes[1].ServerStore.NodeTag;
                var operation =  new Operation(executor, () => store.Changes(dbName, nodeTag), executor.Conventions, command.Result.OperationId, nodeTag);
                var operationResult= await operation.WaitForCompletionAsync(TimeSpan.FromSeconds(60)).ConfigureAwait(false);
                Assert.Equal(1, ((BulkOperationResult)operationResult).Total);
            }
        }
    }
    
    private static async Task<int> GetMembersCount(IDocumentStore store, string databaseName)
    {
        var res = await store.Maintenance.Server.SendAsync(new GetDatabaseRecordOperation(databaseName));
        if (res == null)
        {
            return -1;
        }
        return res.Topology.Members.Count;
    }
    
    private class DeleteByQueryCommand : RavenCommand<OperationIdResult>
    {
        private readonly DocumentConventions _conventions;
        private readonly IndexQuery _queryToDelete;
        private readonly QueryOperationOptions _options;

        public DeleteByQueryCommand(DocumentConventions conventions, IndexQuery queryToDelete, QueryOperationOptions options = null)
        {
            _conventions = conventions ?? throw new ArgumentNullException(nameof(conventions));
            _queryToDelete = queryToDelete ?? throw new ArgumentNullException(nameof(queryToDelete));
            _options = options ?? new QueryOperationOptions();
        }

        public override bool IsReadRequest { get; }

        public override HttpRequestMessage CreateRequest(JsonOperationContext ctx, ServerNode node, out string url)
        {
            var path = new StringBuilder(node.Url)
                .Append("/databases/")
                .Append(node.Database)
                .Append("/queries")
                .Append("?allowStale=")
                .Append(_options.AllowStale)
                .Append("&maxOpsPerSec=")
                .Append(_options.MaxOpsPerSecond)
                .Append("&details=")
                .Append(_options.RetrieveDetails);

            if (_options.StaleTimeout != null)
            {
                path
                    .Append("&staleTimeout=")
                    .Append(_options.StaleTimeout.Value);
            }

            var request = new HttpRequestMessage
            {
                Method = HttpMethod.Delete,
                Content = new BlittableJsonContent(async stream =>
                {
                    await using (var writer = new AsyncBlittableJsonTextWriter(ctx, stream))
                    {
                        writer.WriteIndexQuery(_conventions, ctx, _queryToDelete);
                    }
                })
            };

            url = path.ToString();
            return request;
        }
        public override void SetResponse(JsonOperationContext context, BlittableJsonReaderObject response, bool fromCache)
        {
            if (response == null)
                ThrowInvalidResponse();

            Result = JsonDeserializationClient.OperationIdResult(response);
        }

    }
    
}

