using System.Linq;
using System.Threading.Tasks;
using Raven.Client.Documents;
using Raven.Client.Documents.Operations.Configuration;
using Raven.Client.Http;
using Raven.Client.ServerWide.Commands;
using Raven.Client.ServerWide.Operations;
using Raven.Client.ServerWide.Operations.Configuration;
using Sparrow.Json;
using Tests.Infrastructure;
using Xunit;
using Xunit.Abstractions;

namespace SlowTests.Client
{
    public class ClientConfigurationTests : ClusterTestBase
    {
        public ClientConfigurationTests(ITestOutputHelper output) : base(output)
        {
        }

        [Fact]
        public void CanSetClientConfigurationOnDatabaseCreation()
        {
            using (var store = GetDocumentStore(new Options
            {
                ModifyDatabaseRecord = r => r.Client = new ClientConfiguration
                {
                    MaxNumberOfRequestsPerSession = 50
                }
            }))
            {
                var requestExecutor = store.GetRequestExecutor();

                using (var session = store.OpenSession())
                {
                    session.Load<dynamic>("users/1"); // forcing client configuration update
                }

                Assert.Equal(50, requestExecutor.Conventions.MaxNumberOfRequestsPerSession);
            }
        }

        [Fact]
        public void CanSetClientConfigurationAfterDatabaseCreation()
        {
            using (var store = GetDocumentStore(new Options
            {
                ModifyDatabaseRecord = r => r.Client = new ClientConfiguration
                {
                    MaxNumberOfRequestsPerSession = 50
                }
            }))
            {
                var requestExecutor = store.GetRequestExecutor();

                using (var session = store.OpenSession())
                {
                    session.Load<dynamic>("users/1"); // forcing client configuration update
                }

                Assert.Equal(50, requestExecutor.Conventions.MaxNumberOfRequestsPerSession);

                var databaseRecord = store.Maintenance.Server.Send(new GetDatabaseRecordOperation(store.Database));
                databaseRecord.Client = null;
                store.Maintenance.Server.Send(new UpdateDatabaseOperation(databaseRecord, databaseRecord.Etag));

                using (var session = store.OpenSession())
                {
                    session.Load<dynamic>("users/1"); // forcing client configuration update
                }

                Assert.Equal(30, requestExecutor.Conventions.MaxNumberOfRequestsPerSession);
            }
        }

        [Fact]
        public void ChangeClientConfigurationForDatabase()
        {
            using (var store = GetDocumentStore())
            {
                var requestExecutor = store.GetRequestExecutor();
                store.Maintenance.Send(new PutClientConfigurationOperation(new ClientConfiguration { ReadBalanceBehavior = ReadBalanceBehavior.None, Disabled = false }));

                using (var session = store.OpenSession())
                {
                    session.Load<dynamic>("users/1"); // forcing client configuration update
                }
                store.Maintenance.Send(new PutClientConfigurationOperation(new ClientConfiguration { ReadBalanceBehavior = ReadBalanceBehavior.RoundRobin, Disabled = false }));

                using (var session = store.OpenSession())
                {
                    session.Load<dynamic>("users/1"); // forcing client configuration update
                }
                Assert.Equal(ReadBalanceBehavior.None, store.Conventions.ReadBalanceBehavior);
                Assert.Equal(ReadBalanceBehavior.RoundRobin, requestExecutor.Conventions.ReadBalanceBehavior);
            }
        }

        [Fact]
        public void DatabaseClientConfigurationHasPrecedenceOverGlobal()
        {
            DoNotReuseServer(); // we modify global server configuration, and it impacts other tests
            using (var store = GetDocumentStore())
            {
                var requestExecutor = store.GetRequestExecutor();

                store.Maintenance.Send(new PutClientConfigurationOperation(new ClientConfiguration { ReadBalanceBehavior = ReadBalanceBehavior.RoundRobin, Disabled = false }));
                store.Maintenance.Server.Send(new PutServerWideClientConfigurationOperation(new ClientConfiguration { ReadBalanceBehavior = ReadBalanceBehavior.FastestNode, Disabled = false }));

                using (var session = store.OpenSession())
                {
                    session.Load<dynamic>("users/1"); // forcing client configuration update
                }
                Assert.Equal(ReadBalanceBehavior.None, store.Conventions.ReadBalanceBehavior);
                Assert.Equal(ReadBalanceBehavior.RoundRobin, requestExecutor.Conventions.ReadBalanceBehavior);
            }
        }

        [Fact]
        public async Task ChangeClientConfiguration_ShouldUpdateTheClient()
        {
            var putDatabaseClientConfigDisabled = new PutClientConfigurationOperation(new ClientConfiguration { Disabled = true });
            const int numberOfNodes = 3;

            var (_, leader) = await CreateRaftCluster(numberOfNodes, watcherCluster:true);
            using var store = GetDocumentStore(new Options
                {
                    Server = leader,
                    ReplicationFactor = numberOfNodes,
                    ModifyDocumentStore = documentStore =>
                    {
                        documentStore.Conventions.ReadBalanceBehavior = ReadBalanceBehavior.RoundRobin;
                        documentStore.Conventions.MaxNumberOfRequestsPerSession = 99;
                    }
                });
            var requestExecutor = store.GetRequestExecutor();

            var origin = requestExecutor.Conventions.MaxNumberOfRequestsPerSession;
            await store.Maintenance.SendAsync(new PutClientConfigurationOperation(GetClientConfiguration(100)));
            await AssertWaitForClientConfiguration(100);

            await store.Maintenance.SendAsync(putDatabaseClientConfigDisabled);
            await AssertWaitForClientConfiguration(origin);
            
            await store.Maintenance.SendAsync(new PutClientConfigurationOperation(GetClientConfiguration(101)));
            await store.Maintenance.Server.SendAsync(new PutServerWideClientConfigurationOperation(GetClientConfiguration(102)));
            await AssertWaitForClientConfiguration(101);
                
            await store.Maintenance.SendAsync(putDatabaseClientConfigDisabled);
            await AssertWaitForClientConfiguration(102);
                
            await store.Maintenance.SendAsync(new PutClientConfigurationOperation(GetClientConfiguration(103)));
            await AssertWaitForClientConfiguration(103);

            await store.Maintenance.SendAsync(putDatabaseClientConfigDisabled);
            await AssertWaitForClientConfiguration(102);
            
            await store.Maintenance.Server.SendAsync(new PutServerWideClientConfigurationOperation(new ClientConfiguration{Disabled = true}));
            await AssertWaitForClientConfiguration(origin);
            
            async Task AssertWaitForClientConfiguration(int maxNumberOfRequestsPerSession)
            {
                await WaitForValueAsync(async () =>
                {
                    using var session = store.OpenAsyncSession();
                    await session.LoadAsync<dynamic>("users/1");
                    return requestExecutor.Conventions.MaxNumberOfRequestsPerSession;
                }, maxNumberOfRequestsPerSession);
                Assert.Equal(maxNumberOfRequestsPerSession, requestExecutor.Conventions.MaxNumberOfRequestsPerSession);
            }

            static ClientConfiguration GetClientConfiguration(int maxNumberOfRequestsPerSession)
            {
                return new ClientConfiguration
                {
                    ReadBalanceBehavior = ReadBalanceBehavior.RoundRobin, 
                    MaxNumberOfRequestsPerSession = maxNumberOfRequestsPerSession, 
                    Disabled = false
                };
            }
        }

        [Fact]
        public void ChangeGloballyClientConfiguration()
        {
            DoNotReuseServer(); // we modify global server configuration, and it impacts other tests
            using (var store = GetDocumentStore())
            {
                var requestExecutor = store.GetRequestExecutor();
                store.Maintenance.Server.Send(new PutServerWideClientConfigurationOperation(new ClientConfiguration { ReadBalanceBehavior = ReadBalanceBehavior.None, Disabled = false }));

                using (var session = store.OpenSession())
                {
                    session.Load<dynamic>("users/1"); // forcing client configuration update
                }
                store.Maintenance.Server.Send(new PutServerWideClientConfigurationOperation(new ClientConfiguration { ReadBalanceBehavior = ReadBalanceBehavior.RoundRobin, Disabled = false }));

                using (var session = store.OpenSession())
                {
                    session.Load<dynamic>("users/1"); // forcing client configuration update
                }
                Assert.Equal(ReadBalanceBehavior.None, store.Conventions.ReadBalanceBehavior);
                Assert.Equal(ReadBalanceBehavior.RoundRobin, requestExecutor.Conventions.ReadBalanceBehavior);
            }
        }

        [Fact]
        public void RavenDB_13737()
        {
            DoNotReuseServer(); // we modify global server configuration, and it impacts other tests
            using (var store = GetDocumentStore())
            using (var testedStore = new DocumentStore
            {
                Database = store.Database,
                Urls = store.Urls,
            }.Initialize())
            {
                store.Maintenance.Send(new PutClientConfigurationOperation(new ClientConfiguration { ReadBalanceBehavior = ReadBalanceBehavior.RoundRobin, Disabled = false }));
                store.Maintenance.Server.Send(new PutServerWideClientConfigurationOperation(new ClientConfiguration { ReadBalanceBehavior = ReadBalanceBehavior.FastestNode, Disabled = false }));

                using (var session = testedStore.OpenSession())
                {
                    var before = session.Advanced.RequestExecutor.NumberOfServerRequests;
                    session.Load<dynamic>("users/1"); // forcing client configuration update
                    Assert.Equal(2, session.Advanced.RequestExecutor.NumberOfServerRequests - before);
                }

                using (var session = testedStore.OpenSession())
                {
                    var before = session.Advanced.RequestExecutor.NumberOfServerRequests;
                    session.Load<dynamic>("users/1");
                    Assert.Equal(1, session.Advanced.RequestExecutor.NumberOfServerRequests - before);
                }
            }
        }

        [Fact]
        public async Task PutClientConfiguration_ShouldNotChangeTopologyEtag()
        {
            using var store = GetDocumentStore();

            var initTopology = await GetTopology();

            // Just increment topology Etag so it will not be initial value -1
            var record = await store.Maintenance.Server.SendAsync(new GetDatabaseRecordOperation(store.Database));
            var fixedOrder = record.Topology.AllNodes.ToList();
            await store.Maintenance.Server.SendAsync(new ReorderDatabaseMembersOperation(store.Database, fixedOrder, fixedTopology: true));

            var topologyBefore = await GetTopology();
            Assert.True(topologyBefore.Etag > initTopology.Etag);

            await store.Maintenance.SendAsync(new PutClientConfigurationOperation(new ClientConfiguration { ReadBalanceBehavior = ReadBalanceBehavior.RoundRobin }));
            var topologyAfter = await GetTopology();

            Assert.Equal(topologyBefore.Etag, topologyAfter.Etag);

            async Task<Topology> GetTopology()
            {
                using var context = JsonOperationContext.ShortTermSingleUse();
                var requestExecutor = store.GetRequestExecutor();
                var topologyGetCommand = new GetDatabaseTopologyCommand();
                await requestExecutor.ExecuteAsync(topologyGetCommand, context);
                return topologyGetCommand.Result;
            }
        }

    }
}
