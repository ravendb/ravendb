using System.Threading.Tasks;
using FastTests;
using Raven.Client.Documents.Operations.Configuration;
using Raven.Client.Http;
using Raven.Client.ServerWide.Operations.Configuration;
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
            var (_, leader) = await CreateRaftCluster(3);
            using var store = GetDocumentStore(new Options{Server = leader, });
            var requestExecutor = store.GetRequestExecutor();

            var origin = requestExecutor.Conventions.MaxNumberOfRequestsPerSession;
            await store.Maintenance.SendAsync(new PutClientConfigurationOperation(new ClientConfiguration { MaxNumberOfRequestsPerSession = 100, Disabled = false }));
            await AssertWaitForClientConfiguration(100);
            
            await store.Maintenance.SendAsync(new PutClientConfigurationOperation(new ClientConfiguration { Disabled = true }));
            await AssertWaitForClientConfiguration(origin);
            
            await store.Maintenance.SendAsync(new PutClientConfigurationOperation(new ClientConfiguration { MaxNumberOfRequestsPerSession = 101, Disabled = false }));
            await store.Maintenance.Server.SendAsync(new PutServerWideClientConfigurationOperation(new ClientConfiguration { MaxNumberOfRequestsPerSession = 102, Disabled = false }));
            await AssertWaitForClientConfiguration(101);
                
            await store.Maintenance.SendAsync(new PutClientConfigurationOperation(new ClientConfiguration { Disabled = true}));
            await AssertWaitForClientConfiguration(102);
                
            await store.Maintenance.SendAsync(new PutClientConfigurationOperation(new ClientConfiguration { MaxNumberOfRequestsPerSession = 103, Disabled = false }));
            await AssertWaitForClientConfiguration(103);

            await store.Maintenance.SendAsync(new PutClientConfigurationOperation(new ClientConfiguration { Disabled = true}));
            await AssertWaitForClientConfiguration(102);
            
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
            {
                store.Maintenance.Send(new PutClientConfigurationOperation(new ClientConfiguration { ReadBalanceBehavior = ReadBalanceBehavior.RoundRobin, Disabled = false }));
                store.Maintenance.Server.Send(new PutServerWideClientConfigurationOperation(new ClientConfiguration { ReadBalanceBehavior = ReadBalanceBehavior.FastestNode, Disabled = false }));

                using (var session = store.OpenSession())
                {
                    session.Load<dynamic>("users/1"); // forcing client configuration update
                    Assert.Equal(5, session.Advanced.RequestExecutor.NumberOfServerRequests);
                }

                using (var session = store.OpenSession())
                {
                    session.Load<dynamic>("users/1");
                    Assert.Equal(6, session.Advanced.RequestExecutor.NumberOfServerRequests);
                }
            }
        }

    }
}
