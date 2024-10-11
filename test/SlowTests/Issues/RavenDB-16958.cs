using System;
using System.Collections.Concurrent;
using System.Collections.Generic;
using System.Linq;
using System.Net.Http;
using System.Threading.Tasks;
using FastTests.Server.Replication;
using Newtonsoft.Json;
using Raven.Client.Documents;
using Raven.Client.Documents.Conventions;
using Raven.Client.Documents.Subscriptions;
using Raven.Client.Http;
using Raven.Client.ServerWide;
using Raven.Client.ServerWide.Commands.Cluster;
using Raven.Client.ServerWide.Operations;
using Raven.Server;
using Raven.Server.Config;
using Raven.Server.Config.Settings;
using Raven.Server.Documents.Handlers.Debugging;
using Xunit;
using Xunit.Abstractions;

namespace SlowTests.Issues
{
    public class RavenDB_16958 : ReplicationTestBase
    {
        public RavenDB_16958(ITestOutputHelper output) : base(output)
        {
        }

        private RavenServer CreateSecuredServer(string fakePublicUrl = null, bool uniqueCerts = false)
        {
            var certificates = Certificates.GenerateAndSaveSelfSignedCertificate(createNew: uniqueCerts);
            var customSettings = new ConcurrentDictionary<string, string>();

            if (customSettings.TryGetValue(RavenConfiguration.GetKey(x => x.Security.CertificateLoadExec), out var _) == false)
                customSettings[RavenConfiguration.GetKey(x => x.Security.CertificatePath)] = certificates.ServerCertificatePath;

            customSettings[RavenConfiguration.GetKey(x => x.Core.ServerUrls)] = "https://localhost:0";

            RavenServer ravenServer = GetNewServer(new ServerCreationOptions { CustomSettings = customSettings });

            if (fakePublicUrl != null)
            {
                ravenServer.Configuration.Core.ExternalPublicTcpServerUrl =
                    new[] { fakePublicUrl }.Concat(ravenServer.ServerStore.GetNodeClusterTcpServerUrls(forExternalUse: true))
                        .Select(x => new UriSetting(x)).ToArray();
                ravenServer.Configuration.Core.ClusterPublicTcpServerUrl = new[] { fakePublicUrl }.Concat(ravenServer.ServerStore.GetNodeClusterTcpServerUrls(forExternalUse: false))
                    .Select(x => new UriSetting(x)).ToArray();
                ravenServer.Configuration.Core.PublicTcpServerUrl = new UriSetting(fakePublicUrl);
            }

            return ravenServer;
        }

        [Fact]
        public async Task MoveOnToNextTcpAddressOnGuidFailInSubscriptions()
        {
            //Create same cert servers. B tries to open subscription but accidentally connects to A, after which the guid check fails and failover occurs.
            using (var serverA = CreateSecuredServer())
            using (var serverB = CreateSecuredServer(serverA.ServerStore.GetNodeTcpServerUrl(), false))
            {
                var database = GetDatabaseName();

                using (var storeB = new DocumentStore { Urls = new[] { serverB.WebUrl }, Certificate = serverB.Certificate.Certificate, Database = database }.Initialize())
                using (var storeA = new DocumentStore { Urls = new[] { serverA.WebUrl }, Certificate = serverA.Certificate.Certificate, Database = database }.Initialize())
                {
                    storeB.Maintenance.Server.Send(new CreateDatabaseOperation(new DatabaseRecord(database)));
                    storeA.Maintenance.Server.Send(new CreateDatabaseOperation(new DatabaseRecord(database)));

                    using (var session = storeB.OpenSession())
                    {
                        session.Store(new User { Age = 31 }, "users/1");
                        session.Store(new User { Age = 27 }, "users/12");
                        session.Store(new User { Age = 25 }, "users/3");

                        session.SaveChanges();
                    }

                    var id = storeB.Subscriptions.Create<User>();
                    await using (var subscription = storeB.Subscriptions.GetSubscriptionWorker<User>(new SubscriptionWorkerOptions(id)
                    {
                        TimeToWaitBeforeConnectionRetry = TimeSpan.FromSeconds(5)
                    }))
                    {
                        var keys = new BlockingCollection<string>();
                        var ages = new BlockingCollection<int>();

                        _ = subscription.Run(batch =>
                        {
                            batch.Items.ForEach(x => keys.Add(x.Id));
                            batch.Items.ForEach(x => ages.Add(x.Result.Age));
                        });

                        await AssertWaitForValueAsync(() => Task.FromResult(keys.Count), 3);
                        await AssertWaitForValueAsync(() => Task.FromResult(ages.Count), 3);
                    }
                }
            }
        }

        [Fact]
        public async Task MoveOnToNextTcpAddressOnCertFailInSubscriptions()
        {
            // create 2 servers with different certs. A tries to connect to itself but accidentaly tries to connect to B, cert is rejected and failover occurs.
            // A then connects to itself successfully.
            using (var serverA = CreateSecuredServer())
            using (var serverB = CreateSecuredServer(serverA.ServerStore.GetNodeTcpServerUrl(), true))
            {
                var database = GetDatabaseName();

                using (var storeB = new DocumentStore { Urls = new[] { serverB.WebUrl }, Certificate = serverB.Certificate.Certificate, Database = database }.Initialize())
                using (var storeA = new DocumentStore { Urls = new[] { serverA.WebUrl }, Certificate = serverA.Certificate.Certificate, Database = database }.Initialize())
                {
                    storeB.Maintenance.Server.Send(new CreateDatabaseOperation(new DatabaseRecord(database)));
                    storeA.Maintenance.Server.Send(new CreateDatabaseOperation(new DatabaseRecord(database)));

                    using (var session = storeB.OpenSession())
                    {
                        session.Store(new User { Age = 31 }, "users/1");
                        session.Store(new User { Age = 27 }, "users/12");
                        session.Store(new User { Age = 25 }, "users/3");

                        session.SaveChanges();
                    }

                    var id = storeB.Subscriptions.Create<User>();
                    await using (var subscription = storeB.Subscriptions.GetSubscriptionWorker<User>(new SubscriptionWorkerOptions(id)
                    {
                        TimeToWaitBeforeConnectionRetry = TimeSpan.FromSeconds(5)
                    }))
                    {
                        var keys = new BlockingCollection<string>();
                        var ages = new BlockingCollection<int>();

                        _ = subscription.Run(batch =>
                         {
                             batch.Items.ForEach(x => keys.Add(x.Id));
                             batch.Items.ForEach(x => ages.Add(x.Result.Age));
                         });

                        await AssertWaitForValueAsync(() => Task.FromResult(keys.Count), 3);
                        await AssertWaitForValueAsync(() => Task.FromResult(ages.Count), 3);
                    }
                }
            }
        }

        [Fact]
        public async Task MoveOnToNextTcpAddressOnCertFailInReplication()
        {
            //A tries to replicate with B. B sends tcpInfo with C's url. A fails to connect to C with cert and then moves on to connect to B.
            using (var serverC = CreateSecuredServer(uniqueCerts: true)) //certC
            using (var serverA = CreateSecuredServer(uniqueCerts: false)) //certA
            using (var serverB = CreateSecuredServer(serverC.ServerStore.GetNodeTcpServerUrl(), false)) //certA
            {
                var databaseA = GetDatabaseName("DB_A");
                var databaseB = GetDatabaseName("DB_B");
                var databaseC = GetDatabaseName("DB_C");

                using (var storeB = new DocumentStore { Urls = new[] { serverB.WebUrl }, Certificate = serverB.Certificate.Certificate, Database = databaseB }.Initialize())
                using (var storeA = new DocumentStore { Urls = new[] { serverA.WebUrl }, Certificate = serverA.Certificate.Certificate, Database = databaseA }.Initialize())
                using (var storeC = new DocumentStore { Urls = new[] { serverC.WebUrl }, Certificate = serverC.Certificate.Certificate, Database = databaseC }.Initialize())
                {
                    storeB.Maintenance.Server.Send(new CreateDatabaseOperation(new DatabaseRecord(databaseB)));
                    storeA.Maintenance.Server.Send(new CreateDatabaseOperation(new DatabaseRecord(databaseA)));
                    storeC.Maintenance.Server.Send(new CreateDatabaseOperation(new DatabaseRecord(databaseC)));

                    await SetupReplicationAsync(storeA, storeB);

                    using (var s1 = storeA.OpenSession())
                    {
                        s1.Store(new User(), "foo/bar");
                        s1.SaveChanges();
                    }

                    Assert.True(WaitForDocument(storeA, "foo/bar", 15000));
                    Assert.True(WaitForDocument(storeB, "foo/bar", 15000));
                }
            }
        }

        [Fact]
        public async Task MoveOnToNextTcpAddressOnGuidFailInReplication()
        {
            //A tries to replicate with B. B sends tcpInfo with C's url. A accepts C's certificate and then fails on Guid check. Will failover and then succeed.
            using (var serverC = CreateSecuredServer(uniqueCerts: false)) //certA
            using (var serverA = CreateSecuredServer(uniqueCerts: false)) //certA
            using (var serverB = CreateSecuredServer(serverC.ServerStore.GetNodeTcpServerUrl(), false)) //certA
            {
                var database = GetDatabaseName();

                using (var storeB = new DocumentStore { Urls = new[] { serverB.WebUrl }, Certificate = serverB.Certificate.Certificate, Database = database }.Initialize())
                using (var storeA = new DocumentStore { Urls = new[] { serverA.WebUrl }, Certificate = serverA.Certificate.Certificate, Database = database }.Initialize())
                using (var storeC = new DocumentStore { Urls = new[] { serverC.WebUrl }, Certificate = serverC.Certificate.Certificate, Database = database }.Initialize())
                {
                    storeB.Maintenance.Server.Send(new CreateDatabaseOperation(new DatabaseRecord(database)));
                    storeA.Maintenance.Server.Send(new CreateDatabaseOperation(new DatabaseRecord(database)));
                    storeC.Maintenance.Server.Send(new CreateDatabaseOperation(new DatabaseRecord(database)));

                    await SetupReplicationAsync(storeA, storeB);

                    using (var s1 = storeA.OpenSession())
                    {
                        s1.Store(new User(), "foo/bar");
                        s1.SaveChanges();
                    }

                    Assert.True(WaitForDocument(storeA, "foo/bar", 15000));
                    Assert.True(WaitForDocument(storeB, "foo/bar", 15000));
                }
            }
        }

        [Fact]
        public async Task MoveOnToNextTcpAddressOnGuidFailInCluster()
        {
            //Leader asks node for tcpInfo and node answers with Leader's url. Leader will try to connect to Node and end up connecting to itself,
            //guid check will fail, and failover will happen.

            var cluster = await CreateRaftClusterWithSsl(1, watcherCluster: true);
            var serverB = CreateSecuredServer(cluster.Leader.ServerStore.GetNodeTcpServerUrl(), uniqueCerts: false);

            using (var requestExecutor = ClusterRequestExecutor.CreateForShortTermUse(cluster.Leader.WebUrl, cluster.Leader.Certificate.Certificate, DocumentConventions.DefaultForServer))
            using (requestExecutor.ContextPool.AllocateOperationContext(out var ctx))
            {
                string database = GetDatabaseName();

                await requestExecutor.ExecuteAsync(new AddClusterNodeCommand(serverB.WebUrl, serverB.ServerStore.NodeTag), ctx);

                using (var leaderStore =
                    new DocumentStore { Urls = new[] { cluster.Leader.WebUrl }, Certificate = cluster.Leader.Certificate.Certificate, Database = database }.Initialize())
                {
                    var res = leaderStore.Maintenance.Server.Send(new CreateDatabaseOperation(new DatabaseRecord(database)));
                }

                using (var storeB = new DocumentStore { Urls = new[] { serverB.WebUrl }, Certificate = serverB.Certificate.Certificate, Database = database }.Initialize())
                {
                    var name = await WaitForValueAsync(() =>
                    {
                        var record = storeB.Maintenance.Server.Send(new GetDatabaseRecordOperation(database));
                        return record.DatabaseName;
                    }, database);
                    Assert.Equal(database, name);
                }
            }
        }

        [Fact]
        public async Task MoveOnToNextTcpAddressOnGuidFailInPingTest()
        {
            using (var serverA = CreateSecuredServer())
            using (var serverB = CreateSecuredServer(serverA.ServerStore.GetNodeTcpServerUrl(), false))
            {
                using (var handler = DefaultRavenHttpClientFactory.CreateHttpMessageHandler(serverA.Certificate.Certificate, true, true))
                using (var client = new HttpClient(handler))
                {
                    var url = $"{serverA.WebUrl}/admin/debug/node/ping?url={Uri.EscapeDataString(serverB.WebUrl)}";
                    var rawResponse = (await client.GetAsync(url)).Content.ReadAsStringAsync().Result;
                    var res = JsonConvert.DeserializeObject<HttpPingResult>(rawResponse);
                    Assert.NotNull(res);
                    Assert.Equal(1, res.Result.Count);
                    Assert.True(res.Result[0].Log.Count(exp => exp.Contains("but instead reached a server with Id")) > 0);
                }
            }
        }

        private class HttpPingResult
        {
#pragma warning disable 649
            public List<NodeDebugHandler.PingResult> Result;
#pragma warning restore 649
        }

        private class User
        {
            public int Age;
        }
    }
}
