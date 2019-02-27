// -----------------------------------------------------------------------
//  <copyright file="CanAuthenticate.cs" company="Hibernating Rhinos LTD">
//      Copyright (c) Hibernating Rhinos LTD. All rights reserved.
//  </copyright>
// -----------------------------------------------------------------------

using System;
using System.Collections.Generic;
using System.Diagnostics;
using System.IO;
using System.Linq;
using System.Security.Cryptography.X509Certificates;
using System.Threading;
using System.Threading.Tasks;
using FastTests.Server.Replication;
using Raven.Client.Documents;
using Raven.Client.Documents.Operations;
using Raven.Client.Documents.Operations.ConnectionStrings;
using Raven.Client.Documents.Operations.ETL;
using Raven.Client.Documents.Operations.OngoingTasks;
using Raven.Client.Documents.Operations.Replication;
using Raven.Client.ServerWide;
using Raven.Client.ServerWide.Operations;
using Raven.Client.ServerWide.Operations.Certificates;
using Raven.Server.Utils;
using Raven.Tests.Core.Utils.Entities;
using Sparrow.Json;
using Tests.Infrastructure;
using Xunit;

namespace SlowTests.Authentication
{
    public class AuthenticationClusterTests : ReplicationTestBase
    {
        [Fact]
        public async Task CanReplaceClusterCert()
        {
            var clusterSize = 3;
            var databaseName = GetDatabaseName();
            var leader = await CreateRaftClusterAndGetLeader(clusterSize, false, useSsl: true);

            X509Certificate2 adminCertificate = null;
            
            adminCertificate = AskServerForClientCertificate(_selfSignedCertFileName, new Dictionary<string, DatabaseAccess>(), SecurityClearance.ClusterAdmin, server: leader);

            DatabasePutResult databaseResult;
            using (var store = new DocumentStore
            {
                Urls = new[] {leader.WebUrl},
                Database = databaseName,
                Certificate = adminCertificate,
                Conventions =
                {
                    DisableTopologyUpdates = true
                }
            }.Initialize())
            {
                var doc = new DatabaseRecord(databaseName);
                databaseResult = await store.Maintenance.Server.SendAsync(new CreateDatabaseOperation(doc, clusterSize));
            }

            Assert.Equal(clusterSize, databaseResult.Topology.AllNodes.Count());
            foreach (var server in Servers)
            {
                await server.ServerStore.Cluster.WaitForIndexNotification(databaseResult.RaftCommandIndex);
            }

            foreach (var server in Servers.Where(s => databaseResult.NodesAddedTo.Any(n => n == s.WebUrl)))
            {
                await server.ServerStore.DatabasesLandlord.TryGetOrCreateResourceStore(databaseName);
            }

            using (var store = new DocumentStore()
            {
                Urls = new[] {databaseResult.NodesAddedTo[0]},
                Database = databaseName,
                Certificate = adminCertificate,
                Conventions =
                {
                    DisableTopologyUpdates = true
                }
            }.Initialize())
            {
                using (var session = store.OpenAsyncSession())
                {
                    await session.StoreAsync(new User { Name = "Karmelush" }, "users/1");
                    await session.SaveChangesAsync();
                }

                var certBytes = CertificateUtils.CreateSelfSignedTestCertificate(Environment.MachineName, "RavenTestsServerReplacementCert");
                var newServerCert = new X509Certificate2(certBytes, (string)null, X509KeyStorageFlags.Exportable | X509KeyStorageFlags.PersistKeySet | X509KeyStorageFlags.MachineKeySet);

                var mre = new ManualResetEventSlim();

                leader.ServerCertificateChanged += (sender, args) => mre.Set();

                var requestExecutor = store.GetRequestExecutor();
                using (requestExecutor.ContextPool.AllocateOperationContext(out JsonOperationContext context))
                {
                    var command = new ReplaceClusterCertificateOperation(certBytes, false)
                        .GetCommand(store.Conventions, context);

                    requestExecutor.Execute(command, context);
                }

                Assert.True(mre.Wait(Debugger.IsAttached ? TimeSpan.FromMinutes(10) : TimeSpan.FromMinutes(2)), "Waited too long");
                Assert.NotNull(leader.Certificate.Certificate.Thumbprint);
                Assert.True(leader.Certificate.Certificate.Thumbprint.Equals(newServerCert.Thumbprint), "New cert is identical");

                using (var session = store.OpenSession())
                {
                    var user1 = session.Load<User>("users/1");
                    Assert.NotNull(user1);
                    Assert.Equal("Karmelush", user1.Name);
                }
            }
        }

        [Fact]
        public async Task CanTrustNewClientCertBasedOnPublicKeyPinningHash()
        {
            // Setting up two clusters with external replication cluster1 --> cluster2
            var clusterSize = 1;
            var databaseName = GetDatabaseName();

            // We generate two certificates for cluster 1, an original and a renewed certificate with same private key.
            var (cluster1CertBytes, cluster1ReplacementCertBytes) = CertificateUtils.CreateTwoTestCertificatesWithSameKey(Environment.MachineName, "RavenTestsTwoCerts");
            var cluster1CertFileName = GetTempFileName();
            var cluster1ReplacementCertFileName = GetTempFileName();
            File.WriteAllBytes(cluster1CertFileName, cluster1CertBytes);
            File.WriteAllBytes(cluster1ReplacementCertFileName, cluster1ReplacementCertBytes);

            var cert = new X509Certificate2(cluster1CertBytes);
            var replacementCert = new X509Certificate2(cluster1ReplacementCertBytes);
            Assert.Equal(CertificateUtils.GetPublicKeyPinningHash(cert), CertificateUtils.GetPublicKeyPinningHash(replacementCert));

            // Create cluster 1 with the original certificate. Later we will replace that with the renewed certificate.
            var leader1 = await CreateRaftClusterAndGetLeader(clusterSize, false, useSsl: true, serverCertPath: cluster1CertFileName);
            var cluster1Cert = new X509Certificate2(cluster1CertFileName);
            var adminCertificate1 = AskServerForClientCertificate(cluster1CertFileName, new Dictionary<string, DatabaseAccess>(), SecurityClearance.ClusterAdmin, server: leader1);
            await CreateDatabaseInCluster(databaseName, clusterSize, leader1.WebUrl, adminCertificate1);
            
            // Cluster 2 gets a normal test certificate
            var leader2 = await CreateRaftClusterAndGetLeader(clusterSize, false, useSsl: true);
            var cluster2Cert = new X509Certificate2(_selfSignedCertFileName);
            var adminCertificate2 = AskServerForClientCertificate(_selfSignedCertFileName, new Dictionary<string, DatabaseAccess>(), SecurityClearance.ClusterAdmin, server: leader2);
            await CreateDatabaseInCluster(databaseName, clusterSize, leader2.WebUrl, adminCertificate2);
            
            // This will register cluster 1's cert as a user cert in cluster 2
            AskCluster2ToTrustCluster1(cluster1Cert, cluster2Cert, new Dictionary<string, DatabaseAccess>
            {
                [databaseName] = DatabaseAccess.ReadWrite
            }, SecurityClearance.ValidUser, leader2);

            // First we'll make sure external replication works between the two clusters
            using (var store1 = new DocumentStore
            {
                Urls = new[] {leader1.WebUrl},
                Database = databaseName,
                Certificate = adminCertificate1
            }.Initialize())
            using (var store2 = new DocumentStore
            {
                Urls = new[] {leader2.WebUrl},
                Database = databaseName,
                Certificate = adminCertificate2
            }.Initialize())
            {
                var externalList = await SetupReplicationAsync((DocumentStore)store1, (DocumentStore)store2);

                using (var session = store1.OpenAsyncSession())
                {
                    await session.StoreAsync(new User { Name = "Karmelush" }, "users/1");
                    await session.SaveChangesAsync();
                }

                var replicated1 = WaitForDocumentToReplicate<User>(store2, "users/1", 10000);
                Assert.NotNull(replicated1);
                Assert.Equal("Karmelush", replicated1.Name);

                // Let's replace the certificate in cluster 1 (new cert has same private key) and make sure cluster 2 still trusts cluster 1.
                var cluster1ReplacementCert = new X509Certificate2(cluster1ReplacementCertBytes);

                var mre = new ManualResetEventSlim();

                leader1.ServerCertificateChanged += (sender, args) => mre.Set();

                var requestExecutor = store1.GetRequestExecutor();
                using (requestExecutor.ContextPool.AllocateOperationContext(out JsonOperationContext context))
                {
                    var command = new ReplaceClusterCertificateOperation(cluster1ReplacementCertBytes, false)
                        .GetCommand(store1.Conventions, context);

                    requestExecutor.Execute(command, context);
                }

                Assert.True(mre.Wait(Debugger.IsAttached ? TimeSpan.FromMinutes(10) : TimeSpan.FromMinutes(2)), "Waited too long");
                Assert.NotNull(leader1.Certificate.Certificate.Thumbprint);
                Assert.True(leader1.Certificate.Certificate.Thumbprint.Equals(cluster1ReplacementCert.Thumbprint), "New cert is identical");

                // Disable external replication
                var external = new ExternalReplication(store1.Database, $"ConnectionString-{store2.Identifier}")
                {
                    TaskId = externalList.First().TaskId,
                    Disabled = true
                };

                var responsibleNode = externalList[0].ResponsibleNode;

                var clusterNodes1 = Servers.Where(s => s.ServerStore.GetClusterTopology().TryGetNodeTagByUrl(leader1.WebUrl).HasUrl);
                var node = clusterNodes1.Single(n => n.ServerStore.NodeTag == responsibleNode);

                var db1 = await node.ServerStore.DatabasesLandlord.TryGetOrCreateResourceStore(store1.Database);
                Assert.NotNull(db1);
                var replicationConnection = db1.ReplicationLoader.OutgoingHandlers.Single(x => x.Destination is ExternalReplication);
                var res = await store1.Maintenance.SendAsync(new UpdateExternalReplicationOperation(external));

                Assert.Equal(externalList.First().TaskId, res.TaskId);

                // Make sure the command is processed
                await db1.ServerStore.Cluster.WaitForIndexNotification(res.RaftCommandIndex);
                var connectionDropped = await WaitForValueAsync(() => replicationConnection.IsConnectionDisposed, true);
                Assert.True(connectionDropped);

                // Enable external replication
                external.Disabled = false;
                res = await store1.Maintenance.SendAsync(new UpdateExternalReplicationOperation(external));
                Assert.Equal(externalList.First().TaskId, res.TaskId);

                using (var session = store1.OpenAsyncSession())
                {
                    await session.StoreAsync(new User { Name = "Avivush" }, "users/2");
                    await session.SaveChangesAsync();
                }

                var replicated2 = WaitForDocumentToReplicate<User>(store2, "users/2", 10000);
                Assert.NotNull(replicated2);
                Assert.Equal("Avivush", replicated2.Name);
            }
        }

        [Fact]
        public async Task WillNotTrustNewClientCertIfPublicKeyPinningHashIsDifferent()
        {
            // Setting up two clusters with external replication cluster1 --> cluster2
            var clusterSize = 1;
            var databaseName = GetDatabaseName();

            // We generate the first certificate before calling CreateRaftClusterAndGetLeader so that the two clusters will have separate certificates
            var cluster1CertBytes = CertificateUtils.CreateSelfSignedTestCertificate(Environment.MachineName, "RavenTests");
            var cluster1CertFileName = GetTempFileName();
            File.WriteAllBytes(cluster1CertFileName, cluster1CertBytes);

            var leader1 = await CreateRaftClusterAndGetLeader(clusterSize, false, useSsl: true, serverCertPath: cluster1CertFileName);
            var cluster1Cert = new X509Certificate2(cluster1CertFileName);
            var adminCertificate1 = AskServerForClientCertificate(cluster1CertFileName, new Dictionary<string, DatabaseAccess>(), SecurityClearance.ClusterAdmin, server: leader1);
            await CreateDatabaseInCluster(databaseName, clusterSize, leader1.WebUrl, adminCertificate1);

            // Cluster 2 gets a normal test certificate
            var leader2 = await CreateRaftClusterAndGetLeader(clusterSize, false, useSsl: true);
            var cluster2Cert = new X509Certificate2(_selfSignedCertFileName);
            var adminCertificate2 = AskServerForClientCertificate(_selfSignedCertFileName, new Dictionary<string, DatabaseAccess>(), SecurityClearance.ClusterAdmin, server: leader2);
            await CreateDatabaseInCluster(databaseName, clusterSize, leader2.WebUrl, adminCertificate2);

            // This will register cluster 1's cert as a user cert in cluster 2
            AskCluster2ToTrustCluster1(cluster1Cert, cluster2Cert, new Dictionary<string, DatabaseAccess>
            {
                [databaseName] = DatabaseAccess.ReadWrite
            }, SecurityClearance.ValidUser, leader2);

            // First we'll make sure external replication works between the two clusters
            using (var store1 = new DocumentStore
            {
                Urls = new[] {leader1.WebUrl},
                Database = databaseName,
                Certificate = adminCertificate1
            }.Initialize())
            using (var store2 = new DocumentStore
            {
                Urls = new[] {leader2.WebUrl},
                Database = databaseName,
                Certificate = adminCertificate2
            }.Initialize())
            {
                var externalList = await SetupReplicationAsync((DocumentStore)store1, (DocumentStore)store2);

                using (var session = store1.OpenAsyncSession())
                {
                    await session.StoreAsync(new User { Name = "Karmelush" }, "users/1");
                    await session.SaveChangesAsync();
                }

                var replicated1 = WaitForDocumentToReplicate<User>(store2, "users/1", 10000);
                Assert.NotNull(replicated1);
                Assert.Equal("Karmelush", replicated1.Name);

                // Let's replace the certificate in cluster 1 (new cert has different private key) and make sure cluster 2 WILL NOT trusts cluster 1.
                var certBytes = CertificateUtils.CreateSelfSignedTestCertificate(Environment.MachineName, "ReplacementCertDifferentKey");
                var cluster1ReplacementCert = new X509Certificate2(certBytes);

                var mre = new ManualResetEventSlim();

                leader1.ServerCertificateChanged += (sender, args) => mre.Set();

                var requestExecutor = store1.GetRequestExecutor();
                using (requestExecutor.ContextPool.AllocateOperationContext(out JsonOperationContext context))
                {
                    var command = new ReplaceClusterCertificateOperation(certBytes, false)
                        .GetCommand(store1.Conventions, context);

                    requestExecutor.Execute(command, context);
                }

                Assert.True(mre.Wait(Debugger.IsAttached ? TimeSpan.FromMinutes(10) : TimeSpan.FromMinutes(2)), "Waited too long");
                Assert.NotNull(leader1.Certificate.Certificate.Thumbprint);
                Assert.True(leader1.Certificate.Certificate.Thumbprint.Equals(cluster1ReplacementCert.Thumbprint), "New cert is identical");

                // Disable external replication
                var external = new ExternalReplication(store1.Database, $"ConnectionString-{store2.Identifier}")
                {
                    TaskId = externalList.First().TaskId,
                    Disabled = true
                };

                var responsibleNode = externalList[0].ResponsibleNode;

                var clusterNodes1 = Servers.Where(s => s.ServerStore.GetClusterTopology().TryGetNodeTagByUrl(leader1.WebUrl).HasUrl);
                var node = clusterNodes1.Single(n => n.ServerStore.NodeTag == responsibleNode);

                var db1 = await node.ServerStore.DatabasesLandlord.TryGetOrCreateResourceStore(store1.Database);
                Assert.NotNull(db1);
                var replicationConnection = db1.ReplicationLoader.OutgoingHandlers.Single(x => x.Destination is ExternalReplication);
                var res = await store1.Maintenance.SendAsync(new UpdateExternalReplicationOperation(external));

                Assert.Equal(externalList.First().TaskId, res.TaskId);

                // Make sure the command is processed
                await db1.ServerStore.Cluster.WaitForIndexNotification(res.RaftCommandIndex);
                var connectionDropped = await WaitForValueAsync(() => replicationConnection.IsConnectionDisposed, true);
                Assert.True(connectionDropped);

                // Enable external replication
                external.Disabled = false;
                res = await store1.Maintenance.SendAsync(new UpdateExternalReplicationOperation(external));
                Assert.Equal(externalList.First().TaskId, res.TaskId);

                using (var session = store1.OpenAsyncSession())
                {
                    await session.StoreAsync(new User { Name = "Avivush" }, "users/2");
                    await session.SaveChangesAsync();
                }

                var replicated = WaitForDocumentToReplicate<User>(store2, "users/2", 10000);
                Assert.Null(replicated);
            }
        }

        [Fact]
        public async Task PublicKeyPinningHashShouldBeEqual()
        {
            var (c1, c2) = CertificateUtils.CreateTwoTestCertificatesWithSameKey(Environment.MachineName, "sameKey");
            var c1Cert = new X509Certificate2(c1);
            var c2Cert = new X509Certificate2(c2);

            var h1 = CertificateUtils.GetPublicKeyPinningHash(c1Cert);
            var h2 = CertificateUtils.GetPublicKeyPinningHash(c2Cert);
            Assert.Equal(h1, h2);
        }

        [Fact]
        public async Task PublicKeyPinningHashShouldNotBeEqual()
        {
            // Different private key
            var c1 = CertificateUtils.CreateSelfSignedTestCertificate(Environment.MachineName, "first");
            var c2 = CertificateUtils.CreateSelfSignedTestCertificate(Environment.MachineName, "second");

            var c1Cert = new X509Certificate2(c1);
            var c2Cert = new X509Certificate2(c2);

            var h1 = CertificateUtils.GetPublicKeyPinningHash(c1Cert);
            var h2 = CertificateUtils.GetPublicKeyPinningHash(c2Cert);
            Assert.NotEqual(h1, h2);
        }
    }
}
