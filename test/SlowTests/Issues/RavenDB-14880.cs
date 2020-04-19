using System;
using System.Collections.Generic;
using System.Linq;
using System.Threading.Tasks;
using Raven.Client.Documents;
using Raven.Client.Documents.Operations.Configuration;
using Raven.Client.Http;
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
            const string databaseName = "test";
            var leader = await CreateRaftClusterAndGetLeader(3);
            using (var store = new DocumentStore
            {
                Urls = new[] { leader.WebUrl },
                Database = databaseName
            }.Initialize())
            {
                var (index, _) = await CreateDatabaseInCluster(databaseName, 2, leader.WebUrl);
                await WaitForRaftIndexToBeAppliedInCluster(index, TimeSpan.FromSeconds(30));

                using (var session = store.OpenSession())
                {
                    session.Store(new User());
                    session.SaveChanges();
                }

                ExecuteQuery();
                WaitForIndexing(store);

                var re = store.GetRequestExecutor(databaseName);
                var configurationChanges = new HashSet<long>();

                re.ClientConfigurationChanged += (sender, tuple) =>
                {
                    // since this is a cluster operation, when we fail-over to another node for reads,
                    // the other node might not get the configuration change yet and we might get the "old" configuration back
                    configurationChanges.Add(re.ClientConfigurationEtag);
                };

                SetClientConfiguration(new ClientConfiguration
                {
                    ReadBalanceBehavior = ReadBalanceBehavior.RoundRobin,
                    Disabled = false
                });

                var value = WaitForValue(() =>
                {
                    ExecuteQuery();
                    return configurationChanges.Count;
                }, 1);

                Assert.Equal(1, value);

                SetClientConfiguration(new ClientConfiguration
                {
                    ReadBalanceBehavior = ReadBalanceBehavior.None,
                    Disabled = true
                });

                value = WaitForValue(() =>
                {
                    ExecuteQuery();
                    return configurationChanges.Count;
                }, 2);

                Assert.Equal(2, value);

                for (var i = 0; i < 5; i++)
                {
                    ExecuteQuery();
                    Assert.Equal(2, configurationChanges.Count);
                }

                Assert.Equal(re.ClientConfigurationEtag, configurationChanges.Last());

                void ExecuteQuery()
                {
                    using (var session = store.OpenSession())
                    {
                        var result = session.Query<User>().Customize(x => x.NoCaching()).ToList();
                        Assert.Equal(1, result.Count);
                    }
                }

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
    }
}
