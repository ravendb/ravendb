using System;
using System.Collections.Generic;
using System.Diagnostics;
using System.Linq;
using System.Net.Http;
using System.Threading;
using System.Threading.Tasks;
using Raven.Client.Documents;
using Raven.Client.Documents.Commands;
using Raven.Client.Documents.Conventions;
using Raven.Client.Documents.Session;
using Raven.Client.Http;
using Raven.Client.Server.Commands;
using Raven.Server;
using Sparrow.Json;
using Tests.Infrastructure;
using Xunit;

namespace SlowTests.Cluster
{
    //tests for RavenDB-7533
    public class ClusterModesForRequestExecutorTest : ClusterTestBase
    {
        public class User
        {
            public string Id { get; set; }
            public string Name { get; set; }
        }

        [Fact]
        public async Task Round_robin_load_balancing_should_work()
        {
            var databaseName = "Round_robin_load_balancing_should_work" + Guid.NewGuid();
            var leader = await CreateRaftClusterAndGetLeader(3);
            var followers = Servers.Where(x => x.ServerStore.IsLeader() == false).ToArray();

            var conventionsForLoadBalancing = new DocumentConventions
            {
                ReadBalanceBehavior = ReadBalanceBehavior.RoundRobin
            };

            using (var leaderStore = new DocumentStore
            {
                Urls = leader.WebUrls,
                Database = databaseName,
                Conventions = conventionsForLoadBalancing
            })
            using (var follower1 = new DocumentStore
            {
                Urls = followers[0].WebUrls,
                Database = databaseName,
                Conventions = conventionsForLoadBalancing
            })
            using (var follower2 = new DocumentStore
            {
                Urls = followers[1].WebUrls,
                Database = databaseName,
                Conventions = conventionsForLoadBalancing
            })
            using (var context = JsonOperationContext.ShortTermSingleUse())
            {
                leaderStore.Initialize();
                follower1.Initialize();
                follower2.Initialize();

                var (index, _) = await CreateDatabaseInCluster(databaseName, 3, leader.WebUrls[0]);
                await WaitForRaftIndexToBeAppliedInCluster(index, TimeSpan.FromSeconds(30));
                var leaderRequestExecutor = leaderStore.GetRequestExecutor();

                //make sure we have updated topology --> more deterministic test
                await leaderRequestExecutor.UpdateTopologyAsync(new ServerNode
                {
                    ClusterTag = leader.ServerStore.NodeTag,
                    Database = databaseName,
                    Url = leader.WebUrls[0]
                },  5000);

                //wait until all nodes in database cluster are members (and not promotables)
                //GetTopologyCommand -> does not retrieve promotables
                var topology = new Topology();
                while (topology.Nodes?.Count != 3)
                {
                    var topologyGetCommand = new GetTopologyCommand();
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

                    await WaitForDocumentInClusterAsync<User>(session as DocumentSession, "marker", x => true, leader.ServerStore.Configuration.Cluster.ClusterOperationTimeout.AsTimeSpan);
                }

                var usedUrls = new List<string>();

                for (var i = 0; i < 3; i++)
                {
                    using (var session = leaderStore.OpenSession())
                    {
                        // ReSharper disable once ReturnValueOfPureMethodIsNotUsed
                        session.Query<User>().Where(u => u.Name.StartsWith("Ja")).ToList();
                        usedUrls.Add((await leaderRequestExecutor.GetCurrentNode()).Item2.Url.ToLower());
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
            var databaseName = "Round_robin_load_balancing_should_work" + Guid.NewGuid();
            var leader = await CreateRaftClusterAndGetLeader(3);
            var followers = Servers.Where(x => x.ServerStore.IsLeader() == false).ToArray();

            var conventionsForLoadBalancing = new DocumentConventions
            {
                ReadBalanceBehavior = ReadBalanceBehavior.RoundRobin
            };

            using (var leaderStore = new DocumentStore
            {
                Urls = leader.WebUrls,
                Database = databaseName,
                Conventions = conventionsForLoadBalancing
            })
            using (var follower1 = new DocumentStore
            {
                Urls = followers[0].WebUrls,
                Database = databaseName,
                Conventions = conventionsForLoadBalancing
            })
            using (var follower2 = new DocumentStore
            {
                Urls = followers[1].WebUrls,
                Database = databaseName,
                Conventions = conventionsForLoadBalancing
            })
            using (var context = JsonOperationContext.ShortTermSingleUse())
            {
                leaderStore.Initialize();
                follower1.Initialize();
                follower2.Initialize();

                var (index, _) = await CreateDatabaseInCluster(databaseName, 3, leader.WebUrls[0]);
                await WaitForRaftIndexToBeAppliedInCluster(index, TimeSpan.FromSeconds(30));
                var leaderRequestExecutor = leaderStore.GetRequestExecutor();

                //wait until all nodes in database cluster are members (and not promotables)
                //GetTopologyCommand -> does not retrieve promotables
                var topology = new Topology();
                while (topology.Nodes?.Count != 3)
                {
                    var topologyGetCommand = new GetTopologyCommand();
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
                        leader.ServerStore.Configuration.Cluster.ClusterOperationTimeout.AsTimeSpan);
                }

                var requestExecutor = RequestExecutor.Create(follower1.Urls, databaseName, null, follower1.Conventions);
                do //make sure there are three nodes in the topology
                {
                    await Task.Delay(100);
                } while (requestExecutor.TopologyNodes == null);

                DisposeServerAndWaitForFinishOfDisposal(leader);
            
                var failedRequests = new HashSet<(string, HttpRequestException)>();

                requestExecutor.FailedRequest += (url, e) => failedRequests.Add((url, e));



                using (var tmpContext = JsonOperationContext.ShortTermSingleUse())
                {
                    for (var sessionId = 0; sessionId < 5; sessionId++)
                    {
                        requestExecutor.Cache.Clear(); //make sure we do not use request cache
                        await requestExecutor.ExecuteAsync(new GetStatisticsCommand(), tmpContext, CancellationToken.None, sessionId);
                    }
                }
            }
        }     
    }
}