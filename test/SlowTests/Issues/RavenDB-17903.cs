using System;
using System.Collections.Generic;
using System.Diagnostics;
using System.Linq;
using System.Text;
using System.Threading;
using System.Threading.Tasks;
using FastTests;
using FastTests.Graph;
using Raven.Client.Documents;
using Raven.Client.Documents.Operations;
using Raven.Client.Exceptions;
using Raven.Client.ServerWide;
using Raven.Client.ServerWide.Operations;
using Raven.Server;
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
        public async Task Bulk_Insert_1NodeRestart_TestCase1()
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
        public async Task Bulk_Insert_2NodesDown_1NodeRestart()
        {
            (var nodes, var leader) = await CreateRaftCluster(2, shouldRunInMemory: false);

            using var store = GetDocumentStore(new Options { RunInMemory = false, Server = leader, ReplicationFactor = 2 });

            var record = await store.Maintenance.Server.SendAsync(new GetDatabaseRecordOperation(store.Database));
            var databaseNodeTag = record.Topology.AllNodes.First();
            var nodeToRestart = nodes.First(n => n.ServerStore.NodeTag == databaseNodeTag);
            var nodeToKill = nodes.First(n => n.ServerStore.NodeTag != databaseNodeTag);

            await using (var bulk = store.BulkInsert())
            {
                await bulk.StoreAsync(new TestObj(), $"testObjs/0");
            }

            var result = await DisposeServerAndWaitForFinishOfDisposalAsync(nodeToRestart);
            await DisposeServerAndWaitForFinishOfDisposalAsync(nodeToKill);

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
        public async Task Bulk_Insert_Failover()
        {
            (var nodes, var leader) = await CreateRaftCluster(2, shouldRunInMemory: false);

            using var store = GetDocumentStore(new Options
            {
                RunInMemory = false,
                Server = leader, 
                ReplicationFactor = 2,
            });

            await using (var bulk = store.BulkInsert())
            {
                await bulk.StoreAsync(new TestObj(), $"testObjs/0");
            }

            var storeNodes = store.GetRequestExecutor(store.Database).Topology.Nodes;
            var responsibleNodeUrl = storeNodes[0].Url;
            var responsibleNode = nodes.Single(n => n.ServerStore.GetNodeHttpServerUrl() == responsibleNodeUrl);

            await DisposeServerAndWaitForFinishOfDisposalAsync(responsibleNode);

            // Check topology didn't changed
            var responsibleNodeUrlAfterDispose = store.GetRequestExecutor(store.Database).Topology.Nodes[0].Url;
            Assert.Equal(responsibleNodeUrl, responsibleNodeUrlAfterDispose);

            // If passes - failover succeeded
            await using (var bulk = store.BulkInsert())
            {
                await bulk.StoreAsync(new TestObj(), $"testObjs/1");
            }
        }

        class TestObj
        {
            public string Id { get; set; }
        }

        [Fact]
        public async Task Should_Create_New_Bulk_Insert_If_Previously_Failed_On_Unavailable_Server_1()
        {
            using var server = GetNewServer(new ServerCreationOptions { RunInMemory = false, });
            using var store = GetDocumentStore(new Options { Server = server, RunInMemory = false });

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
                DeletePrevious = false,
                RunInMemory = false,
                DataDirectory = result.DataDirectory,
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

        [Fact]
        public async Task Should_Not_Throw_NodeUnavailable()
        {
            using var server = GetNewServer(new ServerCreationOptions { RunInMemory = false, });
            using var store = GetDocumentStore(new Options { RunInMemory = false, Server = server });

            using (var session = store.OpenSession())
            {
                session.Store(new User { Name = "Omer" });
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
                DataDirectory = result.DataDirectory,
                CustomSettings = new Dictionary<string, string> { [RavenConfiguration.GetKey(x => x.Core.ServerUrls)] = result.Url }
            });

            Operation deleteOperation = await store
                .Operations
                .SendAsync(new DeleteByQueryOperation("from Users"))
                .ConfigureAwait(false);

            await deleteOperation.WaitForCompletionAsync().ConfigureAwait(false);
        }

        [Fact]
        public async Task Should_Throw_AllTopologyNodesDownException()
        {
            (var nodes, var leader) = await CreateRaftCluster(2, shouldRunInMemory: false);

            using var store = GetDocumentStore(new Options
            {
                RunInMemory = false,
                Server = leader,
                ReplicationFactor = 2,
            });

            using (var session = store.OpenSession())
            {
                session.Store(new User { Name = "Omer" });
            }

            var responsibleNodeUrl = store.GetRequestExecutor(store.Database).Topology.Nodes[0].Url;
            var responsibleNode = nodes.Single(n => n.ServerStore.GetNodeHttpServerUrl() == responsibleNodeUrl);
            var otherNode = nodes.Single(n => n.ServerStore.GetNodeHttpServerUrl() != responsibleNodeUrl);
            var result = await DisposeServerAndWaitForFinishOfDisposalAsync(responsibleNode);
            await DisposeServerAndWaitForFinishOfDisposalAsync(otherNode);

            Exception exception = null;
            try
            {
                Operation deleteOperation1 = await store
                    .Operations
                    .SendAsync(new DeleteByQueryOperation("from Users"))
                    .ConfigureAwait(false);

                await deleteOperation1.WaitForCompletionAsync().ConfigureAwait(false);
            }
            catch (Exception e)
            {
                exception = e;
            }
            Assert.NotNull(exception);
            Assert.True(exception is AllTopologyNodesDownException);

            using var newServer = GetNewServer(new ServerCreationOptions
            {
                DeletePrevious = false,
                RunInMemory = false,
                DataDirectory = result.DataDirectory,
                CustomSettings = new Dictionary<string, string> { [RavenConfiguration.GetKey(x => x.Core.ServerUrls)] = result.Url }
            });

            Operation deleteOperation = await store
                .Operations
                .SendAsync(new DeleteByQueryOperation("from Users"))
                .ConfigureAwait(false);

            await deleteOperation.WaitForCompletionAsync().ConfigureAwait(false);
        }
    }
}
