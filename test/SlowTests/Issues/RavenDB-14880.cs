using System;
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

        [Fact]
        public async Task UpdateClientConfigurationOnlyWhenRequired()
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
                var clientConfigurationEtag = re.ClientConfigurationEtag;
                var hadSameClientConfiguration = false;

                var configurationChangesCount = 0;
                re.ClientConfigurationChanged += (sender, tuple) =>
                {
                    configurationChangesCount++;
                    hadSameClientConfiguration |= clientConfigurationEtag == re.ClientConfigurationEtag;
                    clientConfigurationEtag = re.ClientConfigurationEtag;
                };

                store.Maintenance.Server.Send(new PutServerWideClientConfigurationOperation(new ClientConfiguration
                {
                    ReadBalanceBehavior = ReadBalanceBehavior.RoundRobin,
                    Disabled = false
                }));

                var value = WaitForValue(() =>
                {
                    ExecuteQuery();
                    return configurationChangesCount;
                }, 1);

                Assert.Equal(1, value);

                store.Maintenance.Server.Send(new PutServerWideClientConfigurationOperation(new ClientConfiguration
                {
                    ReadBalanceBehavior = ReadBalanceBehavior.None,
                    Disabled = true
                }));

                value = WaitForValue(() =>
                {
                    ExecuteQuery();
                    return configurationChangesCount;
                }, 2);

                Assert.Equal(2, value);

                for (var i = 0; i < 5; i++)
                {
                    ExecuteQuery();
                    Assert.Equal(2, configurationChangesCount);
                }

                Assert.False(hadSameClientConfiguration);

                void ExecuteQuery()
                {
                    using (var session = store.OpenSession())
                    {
                        var result = session.Query<User>().Customize(x => x.NoCaching()).ToList();
                        Assert.Equal(1, result.Count);
                    }
                }
            }
        }
    }
}
