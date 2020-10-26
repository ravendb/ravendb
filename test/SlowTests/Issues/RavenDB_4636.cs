using FastTests;
using Raven.Client.Documents.Operations.Configuration;
using Raven.Client.Http;
using Raven.Client.ServerWide.Operations.Configuration;
using Xunit;
using Xunit.Abstractions;

namespace SlowTests.Issues
{
    public class RavenDB_4636 : RavenTestBase
    {
        public RavenDB_4636(ITestOutputHelper output) : base(output)
        {
        }

        [Fact]
        public void CanInjectConfigurationFromServer()
        {
            DoNotReuseServer();// we modify global server configuration, and it impacts other tests
            using (var store = GetDocumentStore())
            {
                var requestExecutor = store.GetRequestExecutor();

                Assert.Equal(30, store.Conventions.MaxNumberOfRequestsPerSession);
                Assert.Equal(30, requestExecutor.Conventions.MaxNumberOfRequestsPerSession);

                var result = store.Maintenance.Send(new GetClientConfigurationOperation());
                var serverResult = store.Maintenance.Server.Send(new GetServerWideClientConfigurationOperation());
                Assert.Null(serverResult);
                Assert.Null(result.Configuration);

                store.Maintenance.Server.Send(new PutServerWideClientConfigurationOperation(new ClientConfiguration { MaxNumberOfRequestsPerSession = 10 }));

                result = store.Maintenance.Send(new GetClientConfigurationOperation());
                serverResult = store.Maintenance.Server.Send(new GetServerWideClientConfigurationOperation());
                Assert.NotNull(serverResult);
                Assert.Equal(10, serverResult.MaxNumberOfRequestsPerSession.Value);

                using (var session = store.OpenSession())
                {
                    session.Load<dynamic>("users/1"); // forcing client configuration update
                }

                Assert.Equal(30, store.Conventions.MaxNumberOfRequestsPerSession);
                Assert.Equal(10, requestExecutor.Conventions.MaxNumberOfRequestsPerSession);

                store.Maintenance.Server.Send(new PutServerWideClientConfigurationOperation(new ClientConfiguration { MaxNumberOfRequestsPerSession = 10, Disabled = true }));

                using (var session = store.OpenSession())
                {
                    session.Load<dynamic>("users/1"); // forcing client configuration update
                }

                Assert.Equal(30, store.Conventions.MaxNumberOfRequestsPerSession);
                Assert.Equal(30, requestExecutor.Conventions.MaxNumberOfRequestsPerSession);

                store.Maintenance.Send(new PutClientConfigurationOperation(new ClientConfiguration { MaxNumberOfRequestsPerSession = 15 }));

                Assert.Equal(30, store.Conventions.MaxNumberOfRequestsPerSession);
                Assert.Equal(15, requestExecutor.Conventions.MaxNumberOfRequestsPerSession);

                store.Maintenance.Send(new PutClientConfigurationOperation(new ClientConfiguration { MaxNumberOfRequestsPerSession = 15, Disabled = true }));

                Assert.Equal(30, store.Conventions.MaxNumberOfRequestsPerSession);
                Assert.Equal(30, requestExecutor.Conventions.MaxNumberOfRequestsPerSession);

                store.Maintenance.Server.Send(new PutServerWideClientConfigurationOperation(new ClientConfiguration { MaxNumberOfRequestsPerSession = 10, Disabled = false }));

                using (var session = store.OpenSession())
                {
                    session.Load<dynamic>("users/1"); // forcing client configuration update
                }

                Assert.Equal(30, store.Conventions.MaxNumberOfRequestsPerSession);
                Assert.Equal(10, requestExecutor.Conventions.MaxNumberOfRequestsPerSession);

                store.Maintenance.Server.Send(new PutServerWideClientConfigurationOperation(new ClientConfiguration { MaxNumberOfRequestsPerSession = 20, ReadBalanceBehavior = ReadBalanceBehavior.FastestNode }));

                using (var session = store.OpenSession())
                {
                    session.Load<dynamic>("users/1"); // forcing client configuration update
                }

                Assert.Equal(30, store.Conventions.MaxNumberOfRequestsPerSession);
                Assert.Equal(ReadBalanceBehavior.None, store.Conventions.ReadBalanceBehavior);

                Assert.Equal(20, requestExecutor.Conventions.MaxNumberOfRequestsPerSession);
                Assert.Equal(ReadBalanceBehavior.FastestNode, requestExecutor.Conventions.ReadBalanceBehavior);

                store.Maintenance.Server.Send(new PutServerWideClientConfigurationOperation(new ClientConfiguration { ReadBalanceBehavior = ReadBalanceBehavior.RoundRobin, Disabled = true }));

                using (var session = store.OpenSession())
                {
                    session.Load<dynamic>("users/1"); // forcing client configuration update
                }

                Assert.Equal(ReadBalanceBehavior.None, store.Conventions.ReadBalanceBehavior);
                Assert.Equal(ReadBalanceBehavior.None, requestExecutor.Conventions.ReadBalanceBehavior);

                store.Maintenance.Server.Send(new PutServerWideClientConfigurationOperation(new ClientConfiguration { ReadBalanceBehavior = ReadBalanceBehavior.RoundRobin, Disabled = false }));

                using (var session = store.OpenSession())
                {
                    session.Load<dynamic>("users/1"); // forcing client configuration update
                }

                Assert.Equal(ReadBalanceBehavior.None, store.Conventions.ReadBalanceBehavior);
                Assert.Equal(ReadBalanceBehavior.RoundRobin, requestExecutor.Conventions.ReadBalanceBehavior);

            }
        }
    }
}
