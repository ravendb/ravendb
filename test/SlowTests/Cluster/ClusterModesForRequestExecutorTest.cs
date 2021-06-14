using System;
using System.Collections.Generic;
using System.Linq;
using System.Threading;
using System.Threading.Tasks;
using Raven.Client.Documents;
using Raven.Client.Documents.Conventions;
using Raven.Client.Documents.Operations;
using Raven.Client.Documents.Session;
using Raven.Client.Http;
using Raven.Client.ServerWide.Commands;
using Raven.Server;
using Sparrow.Json;
using Tests.Infrastructure;
using Xunit;
using Xunit.Abstractions;

namespace SlowTests.Cluster
{
    //tests for RavenDB-7533
    public class ClusterModesForRequestExecutorTest : ClusterTestBase
    {
        public ClusterModesForRequestExecutorTest(ITestOutputHelper output) : base(output)
        {
        }

        private class User
        {
            public string Id { get; set; }
            public string Name { get; set; }
        }

        private string ReplacePort(string urlWithPort, int port)
        {
            var url = urlWithPort.Substring(0, urlWithPort.LastIndexOf(":", StringComparison.Ordinal));
            return $"{url}:{port}";
        }

        [Fact]
        public async Task ProxyServer_should_work()
        {
            int serverPort = 10000;

            var cluster = await CreateRaftCluster(1, customSettings: GetServerSettingsForPort(false, out _, out _));
            var server = cluster.Leader;

            await using (var proxy = new ProxyServer(ref serverPort, Convert.ToInt32(server.ServerStore.GetNodeHttpServerUrl().Split(':')[2])))
            {
                var databaseName = GetDatabaseName();
                using (var documentStore = new DocumentStore
                {
                    Database = databaseName,
                    Urls = new[] { ReplacePort(server.WebUrl, serverPort) }
                })
                {
                    documentStore.Initialize();

                    var (raftIndex, _) = await CreateDatabaseInCluster(databaseName, 1, server.WebUrl);
                    await WaitForRaftIndexToBeAppliedInCluster(raftIndex, TimeSpan.FromSeconds(10));

                    var totalReadBeforeChanges = proxy.TotalRead;
                    var totalWriteBeforeChanges = proxy.TotalWrite;

                    using (var session = documentStore.OpenSession())
                    {
                        session.Store(new { Foo = "Bar" }, "foo/bar");
                        session.SaveChanges();
                    }

                    using (var session = documentStore.OpenSession())
                    {
                        var doc = session.Load<object>("foo/bar");
                        Assert.NotNull(doc);

                        Assert.True(proxy.TotalRead > 0);
                        Assert.True(proxy.TotalWrite > 0);

                        Assert.True(proxy.TotalRead > totalReadBeforeChanges);
                        Assert.True(proxy.TotalWrite > totalWriteBeforeChanges);
                    }
                }
            }
        }

        [Fact(Skip = "RavenDB-9020")]
        public async Task Fastst_node_should_choose_the_node_without_delay()
        {
            NoTimeouts();
            var databaseName = GetDatabaseName();

            var (leader, serversToProxies) = await CreateRaftClusterWithProxiesAsync(3);
            var followers = Servers.Where(x => x.ServerStore.IsLeader() == false).ToArray();

            var conventionsForLoadBalancing = new DocumentConventions
            {
                ReadBalanceBehavior = ReadBalanceBehavior.FastestNode
            };

            //set proxies with delays to all servers except follower2
            using (var leaderStore = new DocumentStore
            {
                Urls = new[] { ReplacePort(leader.WebUrl, serversToProxies[leader].Port) },
                Database = databaseName,
                Conventions = conventionsForLoadBalancing
            })
            {
                leaderStore.Initialize();

                var (index, _) = await CreateDatabaseInCluster(databaseName, 3, leader.WebUrl);
                await WaitForRaftIndexToBeAppliedInCluster(index, TimeSpan.FromSeconds(30));
                var leaderRequestExecutor = leaderStore.GetRequestExecutor();

                //make sure we have updated topology --> more deterministic test
                await leaderRequestExecutor.UpdateTopologyAsync(new RequestExecutor.UpdateTopologyParameters(new ServerNode
                {
                    ClusterTag = leader.ServerStore.NodeTag,
                    Database = databaseName,
                    Url = leader.WebUrl
                })
                {
                    TimeoutInMs = 5000
                });

                ApplyProxiesOnRequestExecutor(serversToProxies, leaderRequestExecutor);

                //wait until all nodes in database cluster are members (and not promotables)
                //GetDatabaseTopologyCommand -> does not retrieve promotables
                using (var context = JsonOperationContext.ShortTermSingleUse())
                {
                    var topology = new Topology();
                    while (topology.Nodes?.Count != 3)
                    {
                        var topologyGetCommand = new GetDatabaseTopologyCommand();
                        await leaderRequestExecutor.ExecuteAsync(topologyGetCommand, context).ConfigureAwait(false);
                        topology = topologyGetCommand.Result;
                        Thread.Sleep(50);
                    }
                }

                //set delays to all servers except follower2
                foreach (var server in Servers)
                {
                    if (server == followers[1])
                        continue;

                    serversToProxies[server].ConnectionDelay = 300;
                }

                using (var session = leaderStore.OpenSession())
                {
                    session.Store(new User { Name = "John Dow" }, "users/1");
                    session.SaveChanges();
                }

                while (leaderRequestExecutor.InSpeedTestPhase)
                {
                    using (var session = leaderStore.OpenSession())
                    {
                        session.Load<User>("users/1");
                    }
                }

                var fastest = leaderRequestExecutor.GetFastestNode().Result.Node;
                var follower2Proxy = ReplacePort(followers[1].WebUrl, serversToProxies[followers[1]].Port);

                Assert.Equal(follower2Proxy, fastest.Url);
            }
        }

        private void ApplyProxiesOnRequestExecutor(Dictionary<RavenServer, ProxyServer> serversToProxies, RequestExecutor requestExecutor)
        {
            void ApplyProxies(object sender, TopologyUpdatedEventArgs args)
            {
                var topology = args.Topology;
                if (topology == null)
                    return;
                for (var i = 0; i < topology.Nodes.Count; i++)
                {
                    var node = topology.Nodes[i];
                    var kvp = serversToProxies.FirstOrDefault(x => x.Key.ServerStore.NodeTag == node.ClusterTag);
                    Assert.NotNull(kvp);

                    node.Url = ReplacePort(node.Url, kvp.Value.Port);

                    topology.Nodes[i] = node;
                }
            }

            if (requestExecutor.Topology != null)
            {
                ApplyProxies(requestExecutor, new TopologyUpdatedEventArgs(requestExecutor.Topology));
            }

            requestExecutor.OnTopologyUpdated += ApplyProxies;
        }

        [Fact]
        public async Task Round_robin_load_balancing_should_work()
        {
            var databaseName = GetDatabaseName();
            var cluster = await CreateRaftCluster(3, watcherCluster: true);
            var leader = cluster.Leader;
            var followers = Servers.Where(x => x.ServerStore.IsLeader() == false).ToArray();
            var conventionsForLoadBalancing = new DocumentConventions
            {
                ReadBalanceBehavior = ReadBalanceBehavior.RoundRobin
            };

            using (var leaderStore = new DocumentStore
            {
                Urls = new[] { leader.WebUrl },
                Database = databaseName,
                Conventions = conventionsForLoadBalancing
            })
            using (var follower1 = new DocumentStore
            {
                Urls = new[] { followers[0].WebUrl },
                Database = databaseName,
                Conventions = conventionsForLoadBalancing
            })
            using (var follower2 = new DocumentStore
            {
                Urls = new[] { followers[1].WebUrl },
                Database = databaseName,
                Conventions = conventionsForLoadBalancing
            })
            using (var context = JsonOperationContext.ShortTermSingleUse())
            {
                leaderStore.Initialize();
                follower1.Initialize();
                follower2.Initialize();

                var (index, _) = await CreateDatabaseInCluster(databaseName, 3, leader.WebUrl);
                await WaitForRaftIndexToBeAppliedInCluster(index, TimeSpan.FromSeconds(30));
                var leaderRequestExecutor = leaderStore.GetRequestExecutor();

                //make sure we have updated topology --> more deterministic test
                await leaderRequestExecutor.UpdateTopologyAsync(new RequestExecutor.UpdateTopologyParameters(new ServerNode
                {
                    ClusterTag = leader.ServerStore.NodeTag,
                    Database = databaseName,
                    Url = leader.WebUrl
                })
                {
                    TimeoutInMs = 5000,
                    ForceUpdate = true
                });

                //wait until all nodes in database cluster are members (and not promotables)
                //GetDatabaseTopologyCommand -> does not retrieve promotables
                var topology = new Topology();
                while (topology.Nodes?.Count != 3)
                {
                    var topologyGetCommand = new GetDatabaseTopologyCommand();
                    await leaderRequestExecutor.ExecuteAsync(topologyGetCommand, context);
                    topology = topologyGetCommand.Result;
                    Thread.Sleep(50);
                }

                foreach (var server in Servers)
                    await server.ServerStore.Cluster.WaitForIndexNotification(index);

                using (var session = leaderStore.OpenSession())
                {
                    session.Store(new User { Name = "John Dow" });
                    session.Store(new User { Name = "Jack Dow" });
                    session.Store(new User { Name = "Jane Dow" });
                    session.Store(new User { Name = "FooBar" }, "marker");
                    session.SaveChanges();

                    await WaitForDocumentInClusterAsync<User>(session as DocumentSession, "marker", x => true, leader.ServerStore.Configuration.Cluster.OperationTimeout.AsTimeSpan);
                }

                var usedUrls = new List<string>();

                for (var i = 0; i < 3; i++)
                {
                    using (var session = leaderStore.OpenSession())
                    {
                        // ReSharper disable once ReturnValueOfPureMethodIsNotUsed
                        var res = session.Query<User>()
                            .Customize(c => c.WaitForNonStaleResults(TimeSpan.FromSeconds(15)))
                            .Where(u => u.Name.StartsWith("Ja")).ToList();
                        Assert.Equal(2, res.Count);
                        usedUrls.Add((await session.Advanced.GetCurrentSessionNode()).Url.ToLower());
                    }
                }

                foreach (var url in usedUrls)
                {
                    Assert.Single(usedUrls, url);
                }
            }
        }

        [Fact]
        public async Task Round_robin_load_balancing_with_failing_node_should_work()
        {
            var databaseName = GetDatabaseName();
            var (_, leader) = await CreateRaftCluster(3);
            var followers = Servers.Where(x => x.ServerStore.IsLeader() == false).ToArray();

            var conventionsForLoadBalancing = new DocumentConventions
            {
                ReadBalanceBehavior = ReadBalanceBehavior.RoundRobin
            };
            using (var leaderStore = new DocumentStore
            {
                Urls = new[] { leader.WebUrl },
                Database = databaseName,
                Conventions = conventionsForLoadBalancing
            })
            using (var follower1 = new DocumentStore
            {
                Urls = new[] { followers[0].WebUrl },
                Database = databaseName,
                Conventions = conventionsForLoadBalancing
            })
            using (var follower2 = new DocumentStore
            {
                Urls = new[] { followers[1].WebUrl },
                Database = databaseName,
                Conventions = conventionsForLoadBalancing
            })
            using (var context = JsonOperationContext.ShortTermSingleUse())
            {
                leaderStore.Initialize();
                follower1.Initialize();
                follower2.Initialize();

                var (index, _) = await CreateDatabaseInCluster(databaseName, 3, leader.WebUrl);
                await WaitForRaftIndexToBeAppliedInCluster(index, TimeSpan.FromSeconds(30));
                var leaderRequestExecutor = leaderStore.GetRequestExecutor();

                //wait until all nodes in database cluster are members (and not promotables)
                //GetDatabaseTopologyCommand -> does not retrieve promotables
                var topology = new Topology();
                while (topology.Nodes?.Count != 3)
                {
                    var topologyGetCommand = new GetDatabaseTopologyCommand();
                    await leaderRequestExecutor.ExecuteAsync(topologyGetCommand, context);
                    topology = topologyGetCommand.Result;
                    Thread.Sleep(50);
                }

                foreach (var server in Servers)
                    await server.ServerStore.Cluster.WaitForIndexNotification(index);

                using (var session = leaderStore.OpenSession())
                {
                    session.Store(new User { Name = "John Dow" });
                    session.Store(new User { Name = "Jack Dow" });
                    session.Store(new User { Name = "Jane Dow" });
                    session.Store(new User { Name = "FooBar" }, "marker");
                    session.SaveChanges();

                    await WaitForDocumentInClusterAsync<User>(session as DocumentSession,
                        "marker", x => true,
                        leader.ServerStore.Configuration.Cluster.OperationTimeout.AsTimeSpan);
                }

                using (var requestExecutor = RequestExecutor.Create(follower1.Urls, databaseName, null, follower1.Conventions))
                {
                    do //make sure there are three nodes in the topology
                    {
                        await Task.Delay(100);
                    } while (requestExecutor.TopologyNodes == null);

                    DisposeServerAndWaitForFinishOfDisposal(leader);

                    var failedRequests = new HashSet<(string, Exception)>();

                    requestExecutor.OnFailedRequest += (sender, args) => failedRequests.Add((args.Url, args.Exception));

                    using (var tmpContext = JsonOperationContext.ShortTermSingleUse())
                    {
                        for (var sessionId = 0; sessionId < 5; sessionId++)
                        {
                            requestExecutor.Cache.Clear(); //make sure we do not use request cache
                            await requestExecutor.ExecuteAsync(new GetStatisticsOperation().GetCommand(DocumentConventions.Default, tmpContext), tmpContext);
                        }
                    }
                }
            }
        }

        [Fact]
        public async Task RavenDB_7992()
        {
            //here we test that when choosing Fastest-Node as the ReadBalanceBehavior,
            //we can execute commands that use a context, without it leading to a race condition

            var databaseName = GetDatabaseName();
            var (_, leader) = await CreateRaftCluster(3);

            var conventionsForLoadBalancing = new DocumentConventions
            {
                ReadBalanceBehavior = ReadBalanceBehavior.FastestNode
            };

            using (var leaderStore = new DocumentStore
            {
                Urls = new[] { leader.WebUrl },
                Database = databaseName,
                Conventions = conventionsForLoadBalancing
            })
            {
                leaderStore.Initialize();

                var (index, _) = await CreateDatabaseInCluster(databaseName, 3, leader.WebUrl);
                await WaitForRaftIndexToBeAppliedInCluster(index, TimeSpan.FromSeconds(30));

                using (var session = leaderStore.OpenSession())
                {
                    session.Store(new User { Name = "Jon Snow" });
                    session.SaveChanges();
                }

                using (var session = leaderStore.OpenSession())
                {
                    session.Query<User>().Where(u => u.Name.StartsWith("Jo"));
                }
            }
        }
    }
}
