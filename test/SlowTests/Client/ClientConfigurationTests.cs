using FastTests;
using Raven.Client.Documents.Operations.Configuration;
using Raven.Client.Http;
using Raven.Client.ServerWide.Operations.Configuration;
using Xunit;

namespace SlowTests.Client
{
    public class ClientConfigurationTests : RavenTestBase
    {
       
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
