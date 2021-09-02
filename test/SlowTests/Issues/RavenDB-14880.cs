using System;
using System.Collections.Generic;
using System.Linq;
using System.Threading.Tasks;
using Raven.Client.Documents;
using Raven.Client.Documents.Operations.Configuration;
using Raven.Client.Http;
using Raven.Client.ServerWide.Operations;
using Raven.Client.ServerWide.Operations.Configuration;
using Raven.Tests.Core.Utils.Entities;
using Tests.Infrastructure;
using Xunit;
using Xunit.Abstractions;

namespace SlowTests.Issues
{
    public class RavenDB_14880 : ClusterTestBase
    {
        public RavenDB_14880(ITestOutputHelper output) : base(output)
        {
        }

        [Theory]
        [InlineData(false)]
        [InlineData(true)]
        public async Task UpdateClientConfigurationOnlyWhenRequired(bool isServerWide)
        {
            var cluster = await CreateRaftCluster(3);
            var leader = cluster.Leader;

            using (var store = GetDocumentStore(new Options
            {
                Server = leader, 
                ReplicationFactor = 2
            }))
            {
                using (var session = store.OpenSession())
                {
                    session.Store(new User());
                    session.SaveChanges();
                }

                ExecuteQuery(store);
                WaitForIndexing(store);

                var re = store.GetRequestExecutor(store.Database);
                var configurationChanges = new List<long>();

                re.ClientConfigurationChanged += (sender, tuple) => configurationChanges.Add(tuple.RaftCommandIndex);

                SetClientConfiguration(new ClientConfiguration {ReadBalanceBehavior = ReadBalanceBehavior.RoundRobin, Disabled = false});

                var value = WaitForValue(() =>
                {
                    ExecuteQuery(store);
                    return configurationChanges.Count;
                }, 1);

                Assert.Equal(1, value);

                SetClientConfiguration(new ClientConfiguration {ReadBalanceBehavior = ReadBalanceBehavior.None, Disabled = true});

                value = WaitForValue(() =>
                {
                    ExecuteQuery(store);
                    return configurationChanges.Count;
                }, 2);

                Assert.Equal(2, value);

                var i = 0;
                long prev;
                do
                {
                    prev = re.ClientConfigurationEtag;
                    ExecuteQuery(store);
                    Assert.Equal(2, configurationChanges.Count);
                    i++;
                } while (i < 5 || prev != re.ClientConfigurationEtag);

                Assert.Equal(re.ClientConfigurationEtag, configurationChanges.Last());

                void SetClientConfiguration(ClientConfiguration clientConfiguration)
                {
                    if (isServerWide)
                    {
                        store.Maintenance.Server.Send(new PutServerWideClientConfigurationOperation(clientConfiguration));
                    }
                    else
                    {
                        store.Maintenance.Send(new PutClientConfigurationOperation(clientConfiguration));
                    }
                }
            }
        }

        [Fact]
        public async Task UpdateTopologyWhenNeeded()
        {
            var (_, leader) = await CreateRaftCluster(3);
            var toplogyUpdatesCount = 0;
            Topology topology = null;
            using (var store = GetDocumentStore(new Options
            {
                Server = leader,
                ReplicationFactor = 2,
                ModifyDocumentStore = s =>
                {
                    s.OnTopologyUpdated += (sender, tuple) =>
                    {
                        if (sender?.GetType() == typeof(RequestExecutor))
                        {
                            toplogyUpdatesCount++;
                            topology = tuple.Topology;
                        }
                    };
                }
            }))
            {
                var databaseName = store.Database;
                using (var session = store.OpenSession())
                {
                    session.Store(new User());
                    session.SaveChanges();
                }

                Assert.Equal(1, toplogyUpdatesCount);

                // shouldn't trigger a topology update
                ExecuteQuery(store);
                WaitForIndexing(store);

                Assert.Equal(1, toplogyUpdatesCount);

                var topologyEtag = topology.Etag;
                var nodeTagToAdd = Servers.Select(x => x.ServerStore.NodeTag).Except(topology.Nodes.Select(x => x.ClusterTag)).Single();
                await store.Maintenance.Server.SendAsync(new AddDatabaseNodeOperation(databaseName, nodeTagToAdd));

                ExecuteQuery(store);

                await WaitAndAssertForValueAsync(() => topology.Etag > topologyEtag, true);

                Assert.Equal(2, toplogyUpdatesCount);
            }
        }

        private static void ExecuteQuery(IDocumentStore store)
        {
            using (var session = store.OpenSession())
            {
                var result = session.Query<User>().Customize(x => x.NoCaching()).ToList();
                Assert.Equal(1, result.Count);
            }
        }
    }
}
