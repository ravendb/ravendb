using System;
using System.Collections.Generic;
using System.Diagnostics;
using System.Linq;
using System.Net.Http;
using System.Threading;
using System.Threading.Tasks;
using Raven.Client.Documents;
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
                }, 5000);

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
                        usedUrls.Add((await leaderRequestExecutor.GetCurrentNode()).Url.ToLower());
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

                DisposeServerAndWaitForFinishOfDisposal(followers[0]);

                //make sure we have updated topology --> more deterministic test
                await MakeSureToUpdateTopologyAsync(leaderRequestExecutor, databaseName, leader);

                var succeededRequests = new List<string>();
                var failedRequests = new HashSet<(string, HttpRequestException)>();
                leaderRequestExecutor.SucceededRequest += url => succeededRequests.Add(url);
                leaderRequestExecutor.FailedRequest += (url, e) => failedRequests.Add((url, e));

                //do query enough times to 100% hit all nodes in the cluster,
                //so we can hit the offline node several times
                for (var i = 0; i < 20; i++)
                {
                    leaderRequestExecutor.Cache.Clear(); //make sure we do not use request cache
                    using (var session = leaderStore.OpenSession())
                    {
                        // ReSharper disable once ReturnValueOfPureMethodIsNotUsed
                        session.Query<User>().Where(u => u.Name.StartsWith("Ja")).ToList();
                    }
                }

                //count only query requests -> discard requests such as update topology
                //this may or maynot fail, depending on cluster -> if the test
                //is slow enough, the topology will get updated to have only two nodes
                bool IsQueryUrl(string url) => url.Contains("/databases/") && url.Contains("/queries/dynamic/");

                var queryRequests = succeededRequests.Where(IsQueryUrl).ToList();
                var distinctRequests = queryRequests.Distinct().ToList();

                Assert.Equal(2, distinctRequests.Count);

                //since we are doing round robin, we should hit each endpoint at least twice
                Assert.True(queryRequests.Count(requestUrl => requestUrl.Equals(distinctRequests[0])) > 1);
                Assert.True(queryRequests.Count(requestUrl => requestUrl.Equals(distinctRequests[1])) > 1);

                //we do query enough times to hit the offline endpoint at least once
                Assert.True(failedRequests.Count(x => IsQueryUrl(x.Item1)) >= 1);
            }
        }

        private async Task MakeSureToUpdateTopologyAsync(RequestExecutor requestExecutor, string databaseName, RavenServer server)
        {
            var hasSucceded = false;
            while (!hasSucceded)
            {
                hasSucceded = await requestExecutor.UpdateTopologyAsync(new ServerNode
                {
                    ClusterTag = server.ServerStore.NodeTag,
                    Database = databaseName,
                    Url = server.WebUrls[0]
                }, 5000);
            }
        }
    }
}