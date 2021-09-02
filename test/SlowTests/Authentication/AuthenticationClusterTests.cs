/*
// -----------------------------------------------------------------------
//  <copyright file="CanAuthenticate.cs" company="Hibernating Rhinos LTD">
//      Copyright (c) Hibernating Rhinos LTD. All rights reserved.
//  </copyright>
// -----------------------------------------------------------------------

using System;
using System.Collections.Concurrent;
using System.Collections.Generic;
using System.Diagnostics;
using System.IO;
using System.Linq;
using System.Security.Cryptography.X509Certificates;
using System.Threading;
using System.Threading.Tasks;
using FastTests.Server.Replication;
using Raven.Client.Documents;
using Raven.Client.Documents.Operations.Replication;
using Raven.Client.Http;
using Raven.Client.ServerWide;
using Raven.Client.ServerWide.Operations;
using Raven.Client.ServerWide.Operations.Certificates;
using Raven.Client.Util;
using Raven.Server.Config;
using Raven.Server.Utils;
using Raven.Tests.Core.Utils.Entities;
using Sparrow.Json;
using Sparrow.Platform;
using Sparrow.Utils;
using Xunit;
using Xunit.Abstractions;

namespace SlowTests.Authentication
{
    public class AuthenticationClusterTests : ReplicationTestBase
    {
        public AuthenticationClusterTests(ITestOutputHelper output) : base(output)
        {
        }

        [Fact]
        public async Task CanReplaceClusterCert()
        {
            var clusterSize = 3;
            var databaseName = GetDatabaseName();
            var (_, leader) = await CreateRaftCluster(clusterSize, false, useSsl: true);

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

        [Fact(Skip = "https://github.com/dotnet/corefx/issues/30691")]
        public async Task CanReplaceClusterCertWithExtensionPoint()
        {
            string loadAndRenewScript;
            string certChangedScript;

            IDictionary<string, string> customSettings1 = new ConcurrentDictionary<string, string>();
            IDictionary<string, string> customSettings2 = new ConcurrentDictionary<string, string>();
            IDictionary<string, string> customSettings3 = new ConcurrentDictionary<string, string>();
            
            var certPath = GenerateAndSaveSelfSignedCertificate();
            var cert2Path = GenerateAndSaveSelfSignedCertificate(createNew: true);
            var outputFile = Path.Combine(Path.GetTempPath(), Path.ChangeExtension(Guid.NewGuid().ToString(), ".txt"));

            var firstServerCert = new X509Certificate2(certPath, (string)null, X509KeyStorageFlags.Exportable | X509KeyStorageFlags.MachineKeySet);
            var newServerCert = new X509Certificate2(cert2Path, (string)null, X509KeyStorageFlags.Exportable | X509KeyStorageFlags.MachineKeySet);
            
            if (PlatformDetails.RunningOnPosix)
            {
                var loadAndRenewScriptNode1Path = Path.Combine(Path.GetTempPath(), Path.ChangeExtension(Guid.NewGuid().ToString(), ".sh"));
                var loadAndRenewScriptNode2Path = Path.Combine(Path.GetTempPath(), Path.ChangeExtension(Guid.NewGuid().ToString(), ".sh"));
                var loadAndRenewScriptNode3Path = Path.Combine(Path.GetTempPath(), Path.ChangeExtension(Guid.NewGuid().ToString(), ".sh"));
                var certChangedScriptNode1Path = Path.Combine(Path.GetTempPath(), Path.ChangeExtension(Guid.NewGuid().ToString(), ".sh"));
                var certChangedScriptNode2Path = Path.Combine(Path.GetTempPath(), Path.ChangeExtension(Guid.NewGuid().ToString(), ".sh"));
                var certChangedScriptNode3Path = Path.Combine(Path.GetTempPath(), Path.ChangeExtension(Guid.NewGuid().ToString(), ".sh"));

                var loadArgsNode1 = CommandLineArgumentEscaper.EscapeAndConcatenate(new List<string> { loadAndRenewScriptNode1Path, certPath });
                var renewArgsNode1 = CommandLineArgumentEscaper.EscapeAndConcatenate(new List<string> { loadAndRenewScriptNode1Path, cert2Path });
                var certChangedArgsNode1 = CommandLineArgumentEscaper.EscapeAndConcatenate(new List<string> { certChangedScriptNode1Path, outputFile });
                var loadArgsNode2 = CommandLineArgumentEscaper.EscapeAndConcatenate(new List<string> { loadAndRenewScriptNode2Path, certPath });
                var renewArgsNode2 = CommandLineArgumentEscaper.EscapeAndConcatenate(new List<string> { loadAndRenewScriptNode2Path, cert2Path });
                var certChangedArgsNode2 = CommandLineArgumentEscaper.EscapeAndConcatenate(new List<string> { certChangedScriptNode2Path, outputFile });
                var loadArgsNode3 = CommandLineArgumentEscaper.EscapeAndConcatenate(new List<string> { loadAndRenewScriptNode3Path, certPath });
                var renewArgsNode3 = CommandLineArgumentEscaper.EscapeAndConcatenate(new List<string> { loadAndRenewScriptNode3Path, cert2Path });
                var certChangedArgsNode3 = CommandLineArgumentEscaper.EscapeAndConcatenate(new List<string> { certChangedScriptNode3Path, outputFile });

                customSettings1[RavenConfiguration.GetKey(x => x.Security.CertificateLoadExec)] = "bash";
                customSettings1[RavenConfiguration.GetKey(x => x.Security.CertificateLoadExecArguments)] = $"{loadArgsNode1}";
                customSettings1[RavenConfiguration.GetKey(x => x.Security.CertificateRenewExec)] = "bash";
                customSettings1[RavenConfiguration.GetKey(x => x.Security.CertificateRenewExecArguments)] = $"{renewArgsNode1}";
                customSettings1[RavenConfiguration.GetKey(x => x.Security.CertificateChangeExec)] = "bash";
                customSettings1[RavenConfiguration.GetKey(x => x.Security.CertificateChangeExecArguments)] = $"{certChangedArgsNode1}";
                customSettings1[RavenConfiguration.GetKey(x => x.Core.ServerUrls)] = "https://" + Environment.MachineName + ":0";

                customSettings2[RavenConfiguration.GetKey(x => x.Security.CertificateLoadExec)] = "bash";
                customSettings2[RavenConfiguration.GetKey(x => x.Security.CertificateLoadExecArguments)] = $"{loadArgsNode2}";
                customSettings2[RavenConfiguration.GetKey(x => x.Security.CertificateRenewExec)] = "bash";
                customSettings2[RavenConfiguration.GetKey(x => x.Security.CertificateRenewExecArguments)] = $"{renewArgsNode2}";
                customSettings2[RavenConfiguration.GetKey(x => x.Security.CertificateChangeExec)] = "bash";
                customSettings2[RavenConfiguration.GetKey(x => x.Security.CertificateChangeExecArguments)] = $"{certChangedArgsNode2}";
                customSettings2[RavenConfiguration.GetKey(x => x.Core.ServerUrls)] = "https://" + Environment.MachineName + ":0";

                customSettings3[RavenConfiguration.GetKey(x => x.Security.CertificateLoadExec)] = "bash";
                customSettings3[RavenConfiguration.GetKey(x => x.Security.CertificateLoadExecArguments)] = $"{loadArgsNode3}";
                customSettings3[RavenConfiguration.GetKey(x => x.Security.CertificateRenewExec)] = "bash";
                customSettings3[RavenConfiguration.GetKey(x => x.Security.CertificateRenewExecArguments)] = $"{renewArgsNode3}";
                customSettings3[RavenConfiguration.GetKey(x => x.Security.CertificateChangeExec)] = "bash";
                customSettings3[RavenConfiguration.GetKey(x => x.Security.CertificateChangeExecArguments)] = $"{certChangedArgsNode3}";
                customSettings3[RavenConfiguration.GetKey(x => x.Core.ServerUrls)] = "https://" + Environment.MachineName + ":0";

                loadAndRenewScript = "#!/bin/bash\ncat -u \"$1\"";
                certChangedScript = "#!/bin/bash\nwhile read line; do\n\techo \"$line\" >> \"$1\"\ndone < /dev/stdin";

                File.WriteAllText(loadAndRenewScriptNode1Path, loadAndRenewScript);
                File.WriteAllText(loadAndRenewScriptNode2Path, loadAndRenewScript);
                File.WriteAllText(loadAndRenewScriptNode3Path, loadAndRenewScript);
                File.WriteAllText(certChangedScriptNode1Path, certChangedScript);
                File.WriteAllText(certChangedScriptNode2Path, certChangedScript);
                File.WriteAllText(certChangedScriptNode3Path, certChangedScript);

                Process.Start("chmod", $"700 {loadAndRenewScriptNode1Path}");
                Process.Start("chmod", $"700 {loadAndRenewScriptNode2Path}");
                Process.Start("chmod", $"700 {loadAndRenewScriptNode3Path}");
                Process.Start("chmod", $"700 {certChangedScriptNode1Path}");
                Process.Start("chmod", $"700 {certChangedScriptNode2Path}");
                Process.Start("chmod", $"700 {certChangedScriptNode3Path}");
            }
            else
            {
                var loadAndRenewScriptNode1Path = Path.Combine(Path.GetTempPath(), Path.ChangeExtension(Guid.NewGuid().ToString(), ".ps1"));
                var loadAndRenewScriptNode2Path = Path.Combine(Path.GetTempPath(), Path.ChangeExtension(Guid.NewGuid().ToString(), ".ps1"));
                var loadAndRenewScriptNode3Path = Path.Combine(Path.GetTempPath(), Path.ChangeExtension(Guid.NewGuid().ToString(), ".ps1"));
                var certChangedScriptNode1Path = Path.Combine(Path.GetTempPath(), Path.ChangeExtension(Guid.NewGuid().ToString(), ".ps1"));
                var certChangedScriptNode2Path = Path.Combine(Path.GetTempPath(), Path.ChangeExtension(Guid.NewGuid().ToString(), ".ps1"));
                var certChangedScriptNode3Path = Path.Combine(Path.GetTempPath(), Path.ChangeExtension(Guid.NewGuid().ToString(), ".ps1"));

                var loadArgsNode1 = CommandLineArgumentEscaper.EscapeAndConcatenate(new List<string> { "-NoProfile", loadAndRenewScriptNode1Path, certPath });
                var renewArgsNode1 = CommandLineArgumentEscaper.EscapeAndConcatenate(new List<string> { "-NoProfile", loadAndRenewScriptNode1Path, cert2Path });
                var certChangedArgsNode1 = CommandLineArgumentEscaper.EscapeAndConcatenate(new List<string> { "-NoProfile", certChangedScriptNode1Path, outputFile });
                var loadArgsNode2 = CommandLineArgumentEscaper.EscapeAndConcatenate(new List<string> { "-NoProfile", loadAndRenewScriptNode2Path, certPath });
                var renewArgsNode2 = CommandLineArgumentEscaper.EscapeAndConcatenate(new List<string> { "-NoProfile", loadAndRenewScriptNode2Path, cert2Path });
                var certChangedArgsNode2 = CommandLineArgumentEscaper.EscapeAndConcatenate(new List<string> { "-NoProfile", certChangedScriptNode2Path, outputFile });
                var loadArgsNode3 = CommandLineArgumentEscaper.EscapeAndConcatenate(new List<string> { "-NoProfile", loadAndRenewScriptNode3Path, certPath });
                var renewArgsNode3 = CommandLineArgumentEscaper.EscapeAndConcatenate(new List<string> { "-NoProfile", loadAndRenewScriptNode3Path, cert2Path });
                var certChangedArgsNode3 = CommandLineArgumentEscaper.EscapeAndConcatenate(new List<string> { "-NoProfile", certChangedScriptNode3Path, outputFile });

                customSettings1[RavenConfiguration.GetKey(x => x.Security.CertificateLoadExec)] = "powershell";
                customSettings1[RavenConfiguration.GetKey(x => x.Security.CertificateLoadExecArguments)] = $"{loadArgsNode1}";
                customSettings1[RavenConfiguration.GetKey(x => x.Security.CertificateRenewExec)] = "powershell";
                customSettings1[RavenConfiguration.GetKey(x => x.Security.CertificateRenewExecArguments)] = $"{renewArgsNode1}";
                customSettings1[RavenConfiguration.GetKey(x => x.Security.CertificateChangeExec)] = "powershell";
                customSettings1[RavenConfiguration.GetKey(x => x.Security.CertificateChangeExecArguments)] = $"{certChangedArgsNode1}";
                customSettings1[RavenConfiguration.GetKey(x => x.Core.ServerUrls)] = "https://" + Environment.MachineName + ":0";

                customSettings2[RavenConfiguration.GetKey(x => x.Security.CertificateLoadExec)] = "powershell";
                customSettings2[RavenConfiguration.GetKey(x => x.Security.CertificateLoadExecArguments)] = $"{loadArgsNode2}";
                customSettings2[RavenConfiguration.GetKey(x => x.Security.CertificateRenewExec)] = "powershell";
                customSettings2[RavenConfiguration.GetKey(x => x.Security.CertificateRenewExecArguments)] = $"{renewArgsNode2}";
                customSettings2[RavenConfiguration.GetKey(x => x.Security.CertificateChangeExec)] = "powershell";
                customSettings2[RavenConfiguration.GetKey(x => x.Security.CertificateChangeExecArguments)] = $"{certChangedArgsNode2}";
                customSettings2[RavenConfiguration.GetKey(x => x.Core.ServerUrls)] = "https://" + Environment.MachineName + ":0";

                customSettings3[RavenConfiguration.GetKey(x => x.Security.CertificateLoadExec)] = "powershell";
                customSettings3[RavenConfiguration.GetKey(x => x.Security.CertificateLoadExecArguments)] = $"{loadArgsNode3}";
                customSettings3[RavenConfiguration.GetKey(x => x.Security.CertificateRenewExec)] = "powershell";
                customSettings3[RavenConfiguration.GetKey(x => x.Security.CertificateRenewExecArguments)] = $"{renewArgsNode3}";
                customSettings3[RavenConfiguration.GetKey(x => x.Security.CertificateChangeExec)] = "powershell";
                customSettings3[RavenConfiguration.GetKey(x => x.Security.CertificateChangeExecArguments)] = $"{certChangedArgsNode3}";
                customSettings3[RavenConfiguration.GetKey(x => x.Core.ServerUrls)] = "https://" + Environment.MachineName + ":0";


                loadAndRenewScript = @"param([string]$userArg1)
try {
    $bytes = Get-Content -path $userArg1 -encoding Byte
    $stdout = [System.Console]::OpenStandardOutput()
    $stdout.Write($bytes, 0, $bytes.Length)  
}
catch {
    Write-Error $_.Exception
    exit 1
}
exit 0";

                certChangedScript = @"param([string]$userArg1)
try {
    $certBase64 = [System.Console]::ReadLine()
    Add-Content $userArg1 $certBase64
}
catch {
    Write-Error $_.Exception
    exit 1
}
exit 0";
                
                File.WriteAllText(loadAndRenewScriptNode1Path, loadAndRenewScript);
                File.WriteAllText(loadAndRenewScriptNode2Path, loadAndRenewScript); 
                File.WriteAllText(loadAndRenewScriptNode3Path, loadAndRenewScript);
                File.WriteAllText(certChangedScriptNode1Path, certChangedScript);
                File.WriteAllText(certChangedScriptNode2Path, certChangedScript);
                File.WriteAllText(certChangedScriptNode3Path, certChangedScript);
            }

            var clusterSize = 3;
            var databaseName = GetDatabaseName();
            var (_, leader) = await CreateRaftCluster(clusterSize, false, useSsl: true, customSettingsList: new List<IDictionary<string, string>>{customSettings1, customSettings2, customSettings3});

            Assert.True(leader?.Certificate?.Certificate?.Thumbprint?.Equals(firstServerCert.Thumbprint), "Cert is identical");
            
            var adminCertificate = AskServerForClientCertificate(certPath, new Dictionary<string, DatabaseAccess>(), SecurityClearance.ClusterAdmin, server: leader);

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

                var mre = new ManualResetEventSlim();

                leader.ServerCertificateChanged += (sender, args) => mre.Set();

                // This will initiate the refresh cycle, and ask a new certificate from the executable 
                leader.RefreshClusterCertificate(false, RaftIdGenerator.NewId());

                Assert.True(mre.Wait(Debugger.IsAttached ? TimeSpan.FromMinutes(10) : TimeSpan.FromMinutes(2)), "Waited too long");
                Assert.NotNull(leader.Certificate.Certificate.Thumbprint);
                Assert.True(leader.Certificate.Certificate.Thumbprint.Equals(newServerCert.Thumbprint), "New cert is identical");

                var base64NewCertWrittenByExecutable = File.ReadAllLines(outputFile)[0];
                var loadedCertificate = new X509Certificate2(Convert.FromBase64String(base64NewCertWrittenByExecutable));

                Assert.Equal(newServerCert.Thumbprint, loadedCertificate.Thumbprint);

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
            Assert.Equal(cert.GetPublicKeyPinningHash(), replacementCert.GetPublicKeyPinningHash());

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
                await db1.ServerStore.Cluster.WaitForIndexNotification(res.RaftCommandIndex);

                Assert.Equal(externalList.First().TaskId, res.TaskId);

                using (var session = store1.OpenAsyncSession())
                {
                    await session.StoreAsync(new User { Name = "Avivush" }, "users/2").ConfigureAwait(false);
                    await session.SaveChangesAsync();
                }

                //WaitForUserToContinueTheTest(store1, clientCert: adminCertificate1);
                // WaitForUserToContinueTheTest(store2, clientCert: adminCertificate2);

                var replicated = WaitForDocumentToReplicate<User>(store2, "users/2", 10000);
                Assert.Null(replicated);

                // RavenDB-13010
                Assert.True(WaitForValue(() =>
                {
                    var repStats = store1.Maintenance.Send(new GetReplicationPerformanceStatisticsOperation());
                    return repStats.Outgoing.SelectMany(x => x.Performance).Count(x => x.Errors.Count > 0) > 0;
                }, true));
            }
        }

        [Fact]
        public void PublicKeyPinningHashShouldBeEqual()
        {
            var (c1, c2) = CertificateUtils.CreateTwoTestCertificatesWithSameKey(Environment.MachineName, "sameKey");
            var c1Cert = new X509Certificate2(c1);
            var c2Cert = new X509Certificate2(c2);

            var h1 = c1Cert.GetPublicKeyPinningHash();
            var h2 = c2Cert.GetPublicKeyPinningHash();
            Assert.Equal(h1, h2);
        }

        [Fact]
        public void PublicKeyPinningHashShouldNotBeEqual()
        {
            // Different private key
            var c1 = CertificateUtils.CreateSelfSignedTestCertificate(Environment.MachineName, "first");
            var c2 = CertificateUtils.CreateSelfSignedTestCertificate(Environment.MachineName, "second");

            var c1Cert = new X509Certificate2(c1);
            var c2Cert = new X509Certificate2(c2);

            var h1 = c1Cert.GetPublicKeyPinningHash();
            var h2 = c2Cert.GetPublicKeyPinningHash();
            Assert.NotEqual(h1, h2);
        }
    }
}
*/
