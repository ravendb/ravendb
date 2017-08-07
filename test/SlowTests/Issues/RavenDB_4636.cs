using FastTests;
using Raven.Client.Documents.Operations.Configuration;
using Raven.Client.ServerWide;
using Raven.Client.ServerWide.Operations.Configuration;
using Xunit;

namespace SlowTests.Issues
{
    public class RavenDB_4636 : RavenTestBase
    {
        [Fact]
        public void CanInjectConfigurationFromServer()
        {
            DoNotReuseServer();// we modify global server configuration, and it impacts other tests
            using (var store = GetDocumentStore())
            {
                var requestExecutor = store.GetRequestExecutor();

                Assert.Equal(30, store.Conventions.MaxNumberOfRequestsPerSession);
                Assert.Equal(30, requestExecutor.Conventions.MaxNumberOfRequestsPerSession);

                var result = store.Admin.Send(new GetClientConfigurationOperation());
                var serverResult = store.Admin.Server.Send(new GetServerWideClientConfigurationOperation());
                Assert.Null(serverResult);
                Assert.Null(result);

                store.Admin.Server.Send(new PutServerWideClientConfigurationOperation(new ClientConfiguration { MaxNumberOfRequestsPerSession = 10 }));

                result = store.Admin.Send(new GetClientConfigurationOperation());
                serverResult = store.Admin.Server.Send(new GetServerWideClientConfigurationOperation());
                Assert.NotNull(serverResult);
                Assert.Equal(10, serverResult.MaxNumberOfRequestsPerSession.Value);
                Assert.Equal(result.RaftCommandIndex, requestExecutor.ClientConfigurationEtag);

                using (var session = store.OpenSession())
                {
                    session.Load<dynamic>("users/1"); // forcing client configuration update
                }

                Assert.Equal(30, store.Conventions.MaxNumberOfRequestsPerSession);
                Assert.Equal(10, requestExecutor.Conventions.MaxNumberOfRequestsPerSession);

                store.Admin.Server.Send(new PutServerWideClientConfigurationOperation(new ClientConfiguration { MaxNumberOfRequestsPerSession = 10, Disabled = true }));

                using (var session = store.OpenSession())
                {
                    session.Load<dynamic>("users/1"); // forcing client configuration update
                }

                Assert.Equal(30, store.Conventions.MaxNumberOfRequestsPerSession);
                Assert.Equal(30, requestExecutor.Conventions.MaxNumberOfRequestsPerSession);

                store.Admin.Send(new PutClientConfigurationOperation(new ClientConfiguration { MaxNumberOfRequestsPerSession = 15 }));

                using (var session = store.OpenSession())
                {
                    session.Load<dynamic>("users/1"); // forcing client configuration update
                }

                Assert.Equal(30, store.Conventions.MaxNumberOfRequestsPerSession);
                Assert.Equal(15, requestExecutor.Conventions.MaxNumberOfRequestsPerSession);

                store.Admin.Send(new PutClientConfigurationOperation(new ClientConfiguration { MaxNumberOfRequestsPerSession = 15, Disabled = true }));

                using (var session = store.OpenSession())
                {
                    session.Load<dynamic>("users/1"); // forcing client configuration update
                }

                Assert.Equal(30, store.Conventions.MaxNumberOfRequestsPerSession);
                Assert.Equal(30, requestExecutor.Conventions.MaxNumberOfRequestsPerSession);

                store.Admin.Server.Send(new PutServerWideClientConfigurationOperation(new ClientConfiguration { MaxNumberOfRequestsPerSession = 10, Disabled = false }));

                using (var session = store.OpenSession())
                {
                    session.Load<dynamic>("users/1"); // forcing client configuration update
                }

                Assert.Equal(30, store.Conventions.MaxNumberOfRequestsPerSession);
                Assert.Equal(10, requestExecutor.Conventions.MaxNumberOfRequestsPerSession);
            }
        }
    }
}