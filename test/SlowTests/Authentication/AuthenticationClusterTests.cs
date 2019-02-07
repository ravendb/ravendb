// -----------------------------------------------------------------------
//  <copyright file="CanAuthenticate.cs" company="Hibernating Rhinos LTD">
//      Copyright (c) Hibernating Rhinos LTD. All rights reserved.
//  </copyright>
// -----------------------------------------------------------------------

using System;
using System.Collections.Generic;
using System.Diagnostics;
using System.Linq;
using System.Security.Cryptography.X509Certificates;
using System.Threading;
using System.Threading.Tasks;
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
    public class AuthenticationClusterTests : ClusterTestBase
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
                Urls = new[] { leader.WebUrl },
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
                Urls = new[] { databaseResult.NodesAddedTo[0] },
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
            var clusterSize = 3;
            var databaseName = GetDatabaseName();

            var leader1 = await CreateRaftClusterAndGetLeader(clusterSize, false, useSsl: true);
            var adminCertificate1 = AskServerForClientCertificate(_selfSignedCertFileName, new Dictionary<string, DatabaseAccess>(), SecurityClearance.ClusterAdmin, server: leader1);
            var topology1 = await CreateDatabaseInCluster(databaseName, clusterSize, leader1.WebUrl, adminCertificate1);

            var leader2 = await CreateRaftClusterAndGetLeader(clusterSize, false, useSsl: true, createNewCert: true);
            var adminCertificate2 = AskServerForClientCertificate(_selfSignedCertFileName, new Dictionary<string, DatabaseAccess>(), SecurityClearance.ClusterAdmin, server: leader2);
            var topology2 = await CreateDatabaseInCluster(databaseName, clusterSize, leader2.WebUrl, adminCertificate2);

            // This will register cluster 1's cert as a user cert in cluster 2
            AskCluster2ToTrustCluster1(adminCertificate1, adminCertificate2, new Dictionary<string, DatabaseAccess>
            {
                [databaseName] = DatabaseAccess.ReadWrite
            }, SecurityClearance.ValidUser, leader2);


            using (var store1 = new DocumentStore
            {
                Urls = new[] { leader1.WebUrl },
                Database = databaseName,
                Certificate = adminCertificate1
            }.Initialize())
            using (var store2 = new DocumentStore
            {
                Urls = new[] { leader2.WebUrl },
                Database = databaseName,
                Certificate = adminCertificate2
            }.Initialize())
            {
                var watcher = new ExternalReplication(store1.Database, "ExternalReplication");
                await store1.Maintenance.SendAsync(new PutConnectionStringOperation<RavenConnectionString>(new RavenConnectionString
                {
                    Name = watcher.ConnectionStringName,
                    Database = watcher.Database,
                    TopologyDiscoveryUrls = store1.Urls
                }));

                IMaintenanceOperation<ModifyOngoingTaskResult> op = new UpdateExternalReplicationOperation(watcher);
                
                await store1.Maintenance.SendAsync(op);

                using (var session = store1.OpenAsyncSession())
                {
                    await session.StoreAsync(new User { Name = "Karmelush" }, "users/1");
                    await session.SaveChangesAsync();
                }
                WaitForUserToContinueTheTest(store1, debug: true, clientCert: adminCertificate1);
                WaitForUserToContinueTheTest(store2, debug: true, clientCert: adminCertificate2);

                Assert.True(WaitForDocument(store2, "users/1"));
            }

            
            /*foreach (var server in Servers)
            {
                await server.ServerStore.Cluster.WaitForIndexNotification(databaseResult.RaftCommandIndex);
            }
            foreach (var server in Servers.Where(s => databaseResult.NodesAddedTo.Any(n => n == s.WebUrl)))
            {
                await server.ServerStore.DatabasesLandlord.TryGetOrCreateResourceStore(databaseName);
            }
*/

            /*using (var store = new DocumentStore()
            {
                Urls = new[] { databaseResult.NodesAddedTo[0] },
                Database = databaseName,
                Certificate = adminCertificate1,
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

                leader1.ServerCertificateChanged += (sender, args) => mre.Set();

                var requestExecutor = store.GetRequestExecutor();
                using (requestExecutor.ContextPool.AllocateOperationContext(out JsonOperationContext context))
                {
                    var command = new ReplaceClusterCertificateOperation(certBytes, false)
                        .GetCommand(store.Conventions, context);

                    requestExecutor.Execute(command, context);
                }

                Assert.True(mre.Wait(5000));

                Assert.True(leader1.Certificate.Certificate.Thumbprint.Equals(newServerCert.Thumbprint));

                using (var session = store.OpenSession())
                {
                    var user1 = session.Load<User>("users/1");
                    Assert.NotNull(user1);
                    Assert.Equal("Karmelush", user1.Name);
                }
            }*/
        }
    }
}
