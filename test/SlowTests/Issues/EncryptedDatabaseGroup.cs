using System;
using System.Collections.Generic;
using System.Linq;
using System.Threading.Tasks;
using Raven.Client.Documents;
using Raven.Client.Exceptions;
using Raven.Client.Exceptions.Database;
using Raven.Client.Exceptions.Security;
using Raven.Client.ServerWide;
using Raven.Client.ServerWide.Operations;
using Raven.Client.ServerWide.Operations.Certificates;
using Raven.Server;
using Raven.Server.ServerWide.Context;
using Raven.Server.Utils;
using Raven.Tests.Core.Utils.Entities;
using Tests.Infrastructure;
using Xunit;
using Xunit.Abstractions;

namespace SlowTests.Issues
{
    public class EncryptedDatabaseGroup : ClusterTestBase
    {
        public EncryptedDatabaseGroup(ITestOutputHelper output) : base(output)
        {
        }

        [Fact]
        public async Task CanRemoveNodeWithNoKey()
        {
            DebuggerAttachedTimeout.DisableLongTimespan = true;
            var (nodes, leader, certificates) = await CreateRaftClusterWithSsl(3, watcherCluster: true);

            Encryption.EncryptedCluster(nodes, certificates, out var databaseName);

            var options = new Options
            {
                Server = leader,
                ReplicationFactor = 2,
                ClientCertificate = certificates.ClientCertificate1.Value,
                AdminCertificate = certificates.ServerCertificate.Value,
                ModifyDatabaseName = _ => databaseName,
                Encrypted = true,
                RunInMemory = false
            };
            using (var store = GetDocumentStore(options))
            {
                await TrySavingDocument(store, 1);

                var record = await store.Maintenance.Server.SendAsync(new GetDatabaseRecordOperation(store.Database));
                var notInDbGroupServer = nodes.Single(s => record.Topology.AllNodes.Contains(s.ServerStore.NodeTag) == false);
                Encryption.DeleteSecretKeyForDatabaseFromServerStore(databaseName, notInDbGroupServer);
                await Assert.ThrowsAsync<RavenException>(async () => await store.Maintenance.Server.SendAsync(new AddDatabaseNodeOperation(store.Database)));

                await WaitForValueAsync(async () => (await store.Maintenance.Server.SendAsync(new GetDatabaseRecordOperation(store.Database))).Topology.Members.Count, 2);

                record = await store.Maintenance.Server.SendAsync(new GetDatabaseRecordOperation(store.Database));
                Assert.Empty(record.DeletionInProgress);
                Assert.Equal(2, record.Topology.Members.Count);
                Assert.Equal(1, record.Topology.Promotables.Count);

                await DeleteNodeFromGroup(store, notInDbGroupServer);

                var originalKey = Encryption.CreateMasterKey(out _);
                notInDbGroupServer.ServerStore.PutSecretKey(originalKey, databaseName, overwrite: false);
                await AddNodeToGroup(store);

                await TrySavingDocument(store, 2);
            }
        }

        [Fact]
        public async Task CanRemoveNodeWithWrongKey()
        {
            DebuggerAttachedTimeout.DisableLongTimespan = true;
            var (nodes, leader, certificates) = await CreateRaftClusterWithSsl(3, watcherCluster: true);

            Encryption.EncryptedCluster(nodes, certificates, out var databaseName);

            var options = new Options
            {
                Server = leader,
                ReplicationFactor = 2,
                ClientCertificate = certificates.ClientCertificate1.Value,
                AdminCertificate = certificates.ServerCertificate.Value,
                ModifyDatabaseName = _ => databaseName,
                Encrypted = true,
                RunInMemory = false
            };
            using (var store = GetDocumentStore(options))
            {
                await TrySavingDocument(store, 1);

                var record = await store.Maintenance.Server.SendAsync(new GetDatabaseRecordOperation(store.Database));
                var notInDbGroupServer = nodes.Single(s => record.Topology.AllNodes.Contains(s.ServerStore.NodeTag) == false);
                Encryption.DeleteSecretKeyForDatabaseFromServerStore(databaseName, notInDbGroupServer);

                var key = Encryption.CreateMasterKey(out _);
                var copy = new string(key); // need to copy because we nullify the underlying mem after putting the key

                notInDbGroupServer.ServerStore.PutSecretKey(key, databaseName, overwrite: false);
                await AddNodeToGroup(store);

                await TrySavingDocument(store, 2);

                await DeleteNodeFromGroup(store, notInDbGroupServer);

                notInDbGroupServer.ServerStore.PutSecretKey(Encryption.CreateMasterKey(out _), databaseName, overwrite: true);

                await Assert.ThrowsAsync<RavenException>(() => AddNodeToGroup(store));

                await DeleteNodeFromGroup(store, notInDbGroupServer);

                notInDbGroupServer.ServerStore.PutSecretKey(copy, databaseName, overwrite: true);
                await AddNodeToGroup(store);
                await TrySavingDocument(store, 2);
            }
        }

        private async Task DeleteNodeFromGroup(DocumentStore store, RavenServer notInDbGroupServer)
        {
            var deleteResult = await store.Maintenance.Server.SendAsync(
                new DeleteDatabasesOperation(store.Database, hardDelete: false, fromNode: notInDbGroupServer.ServerStore.NodeTag,
                    timeToWaitForConfirmation: TimeSpan.FromSeconds(30)));
            await Cluster.WaitForRaftIndexToBeAppliedInClusterAsync(deleteResult.RaftCommandIndex, TimeSpan.FromSeconds(30));

            var record = await store.Maintenance.Server.SendAsync(new GetDatabaseRecordOperation(store.Database));
            Assert.Equal(2, record.Topology.Count);
            Assert.Empty(record.DeletionInProgress);
        }

        private async Task AddNodeToGroup(DocumentStore store)
        {
            var addResult = await store.Maintenance.Server.SendAsync(new AddDatabaseNodeOperation(store.Database));
            await Cluster.WaitForRaftIndexToBeAppliedInClusterAsync(addResult.RaftCommandIndex, TimeSpan.FromSeconds(30));
            var record = await store.Maintenance.Server.SendAsync(new GetDatabaseRecordOperation(store.Database));
            Assert.Equal(3, record.Topology.Count);
        }

        [Fact]
        public async Task AddingNodeToEncryptedDatabaseGroupShouldThrow()
        {
            var (nodes, leader, certificates) = await CreateRaftClusterWithSsl(3);

            Encryption.EncryptedCluster(nodes, certificates, out var databaseName);

            var options = new Options
            {
                Server = leader,
                ReplicationFactor = 2,
                ClientCertificate = certificates.ClientCertificate1.Value,
                AdminCertificate = certificates.ServerCertificate.Value,
                ModifyDatabaseName = _ => databaseName,
                Encrypted = true
            };

            using (var store = GetDocumentStore(options))
            {
                var record = await store.Maintenance.Server.SendAsync(new GetDatabaseRecordOperation(store.Database));
                var notInDbGroupServer = nodes.Single(s => record.Topology.AllNodes.Contains(s.ServerStore.NodeTag) == false);
                Encryption.DeleteSecretKeyForDatabaseFromServerStore(databaseName, notInDbGroupServer);

                await Assert.ThrowsAsync<RavenException>(async () => await store.Maintenance.Server.SendAsync(new AddDatabaseNodeOperation(store.Database)));

                using (var notInDbGroupStore = GetDocumentStore(new Options
                {
                    Server = notInDbGroupServer,
                    CreateDatabase = false,
                    ModifyDocumentStore = ds => ds.Conventions.DisableTopologyUpdates = true,
                    ClientCertificate = options.ClientCertificate,
                    ModifyDatabaseName = _ => databaseName
                }))
                {
                    await Assert.ThrowsAsync<DatabaseLoadFailureException>(async () => await TrySavingDocument(notInDbGroupStore));
                    Encryption.PutSecretKeyForDatabaseInServerStore(databaseName, notInDbGroupServer);
                    await notInDbGroupServer.ServerStore.DatabasesLandlord.TryGetOrCreateResourceStore(databaseName, ignoreDisabledDatabase: true);
                    await TrySavingDocument(notInDbGroupStore);
                }
            }
        }

        private static async Task TrySavingDocument(DocumentStore store, int? replicas = null)
        {
            using (var session = store.OpenAsyncSession())
            {
                if (replicas.HasValue)
                    session.Advanced.WaitForReplicationAfterSaveChanges(replicas: replicas.Value);

                await session.StoreAsync(new User { Name = "Foo" });
                await session.SaveChangesAsync();
            }
        }

        [Fact]
        public async Task DeletingMasterKeyForExistedEncryptedDatabaseShouldFail()
        {
            Encryption.EncryptedServer(out var certificates, out var databaseName);

            var options = new Options
            {
                ModifyDatabaseName = _ => databaseName,
                ClientCertificate = certificates.ServerCertificate.Value,
                AdminCertificate = certificates.ServerCertificate.Value,
                Encrypted = true
            };

            using (var store = GetDocumentStore(options))
            {
                await TrySavingDocument(store);
                using (Server.ServerStore.ContextPool.AllocateOperationContext(out TransactionOperationContext ctx))
                {
                    using (ctx.OpenWriteTransaction())
                    {
                        Assert.Throws<InvalidOperationException>(() => Server.ServerStore.DeleteSecretKey(ctx, store.Database));
                    }

                    store.Maintenance.Server.Send(new DeleteDatabasesOperation(store.Database, true));
                    using (ctx.OpenWriteTransaction())
                    {
                        Server.ServerStore.DeleteSecretKey(ctx, store.Database);
                    }
                }
            }
        }

        [Fact]
        public async Task DeletingEncryptedDatabaseFromDatabaseGroup()
        {
            var (nodes, server, certificates) = await CreateRaftClusterWithSsl(3);

            Encryption.EncryptedCluster(nodes, certificates, out var databaseName);

            var options = new Options
            {
                Server = server,
                ReplicationFactor = 3,
                Encrypted = true,
                AdminCertificate = certificates.ServerCertificate.Value,
                ClientCertificate = certificates.ClientCertificate1.Value,
                ModifyDatabaseName = _ => databaseName
            };

            using (var store = GetDocumentStore(options))
            {
                await TrySavingDocument(store);
                store.Maintenance.Server.Send(new DeleteDatabasesOperation(store.Database, true, fromNode: nodes[0].ServerStore.NodeTag));
                var value = WaitForValue(() => GetTopology().AllNodes.Count(), 2);
                Assert.Equal(2, value);

                store.Maintenance.Server.Send(new DeleteDatabasesOperation(store.Database, true, fromNode: nodes[1].ServerStore.NodeTag));
                value = WaitForValue(() => GetTopology().AllNodes.Count(), 1);
                Assert.Equal(1, value);

                DatabaseTopology GetTopology()
                {
                    var serverStore = nodes[2].ServerStore;
                    using (serverStore.ContextPool.AllocateOperationContext(out TransactionOperationContext ctx))
                    using (ctx.OpenReadTransaction())
                    using (var databaseRecord = serverStore.Cluster.ReadRawDatabaseRecord(ctx, store.Database))
                    {
                        return databaseRecord.Topology;
                    }
                }
            }
        }

        [Fact]
        public async Task EnsureDatabaseDeletedFromCertificate()
        {
            var (nodes, leader, certificates) = await CreateRaftClusterWithSsl(3);
            var adminClusterCert = certificates.ClientCertificate1.Value;
            var userCert = certificates.ClientCertificate2.Value;

            var databaseName = GetDatabaseName();
            foreach (var node in nodes)
            {
                Certificates.RegisterClientCertificate(certificates.ServerCertificate.Value, userCert, new Dictionary<string, DatabaseAccess>() { [databaseName] = DatabaseAccess.Admin }, SecurityClearance.ValidUser, node);
                Certificates.RegisterClientCertificate(certificates.ServerCertificate.Value, adminClusterCert, new Dictionary<string, DatabaseAccess>(), SecurityClearance.ClusterAdmin, node);
            }

            using (var store = GetDocumentStore(new Options
            {
                Server = leader,
                ReplicationFactor = 3,
                AdminCertificate = certificates.ServerCertificate.Value,
                ClientCertificate = userCert,
                ModifyDatabaseName = _ => databaseName,
                DeleteDatabaseOnDispose = false,
                ModifyDocumentStore = s => s.Conventions.DisposeCertificate = false
            }))
            {
                await TrySavingDocument(store);
            }

            using (var store = new DocumentStore()
            {
                Database = databaseName,
                Urls = new string[] { leader.WebUrl },
                Certificate = adminClusterCert,
                Conventions =
                {
                    DisposeCertificate = false
                }
            }.Initialize())
            {
                //check cert is saved with this db as expected
                var cert = await store.Maintenance.Server.SendAsync(new GetCertificateOperation(userCert.Thumbprint));
                Assert.True(cert.Permissions.ContainsKey(databaseName));

                //delete the database
                var res = await store.Maintenance.Server.SendAsync(new DeleteDatabasesOperation(databaseName, true));
                await Cluster.WaitForRaftIndexToBeAppliedOnClusterNodesAsync(res.RaftCommandIndex, nodes);

                var actual = WaitForValue(GetTopologyCount, 0);
                Assert.Equal(0, actual);

                int GetTopologyCount()
                {
                    var count = 0;
                    foreach (var node in nodes)
                    {
                        using (node.ServerStore.ContextPool.AllocateOperationContext(out TransactionOperationContext ctx))
                        using (ctx.OpenReadTransaction())
                        using (var databaseRecord = node.ServerStore.Cluster.ReadRawDatabaseRecord(ctx, databaseName))
                        {
                            if (databaseRecord != null)
                                count++;
                        }
                    }

                    return count;
                }

                //create database with the same name as deleted one
                var (index, servers) = await CreateDatabaseInCluster(databaseName, 3, leader.WebUrl, adminClusterCert);
                await Cluster.WaitForRaftIndexToBeAppliedOnClusterNodesAsync(index, servers);

                //check db has been deleted from the permissions of cert
                var changedCert = await store.Maintenance.Server.SendAsync(new GetCertificateOperation(userCert.Thumbprint));
                Assert.False(changedCert.Permissions.ContainsKey(databaseName));
            }

            //try accessing the new database with the old database certificate
            using (var store = new DocumentStore()
            {
                Urls = new[] { leader.WebUrl },
                Certificate = userCert,
                Database = databaseName,
                Conventions =
                {
                    DisposeCertificate = false
                }
            }.Initialize())
            {
                var requestExecutor = store.GetRequestExecutor(databaseName);
                requestExecutor.TryRemoveHttpClient(force: true); // reset to forget the previous connection

                var ex = await Assert.ThrowsAsync<AuthorizationException>(async () => await TrySavingDocument((DocumentStore)store));
                Assert.Contains($"Could not authorize access to {databaseName} using provided client certificate", ex.Message);
            }
        }
    }
}
