using FastTests;
using Raven.Client.Server.Operations.Configuration;
using Xunit;

namespace SlowTests.Issues
{
    public class RavenDB_4636 : RavenTestBase
    {
        [Fact]
        public void CanInjectConfigurationFromServer()
        {
            using (var store = GetDocumentStore())
            {
                Assert.Equal(30, store.Conventions.MaxNumberOfRequestsPerSession);

                var result = store.Admin.Server.Send(new GetClientConfigurationOperation());
                Assert.Null(result);

                store.Admin.Server.Send(new PutClientConfigurationOperation(new ClientConfiguration { MaxNumberOfRequestsPerSession = 10 }));

                result = store.Admin.Server.Send(new GetClientConfigurationOperation());
                Assert.NotNull(result);
                Assert.True(result.RaftCommandIndex > 0);
                Assert.Equal(10, result.Configuration.MaxNumberOfRequestsPerSession.Value);

                using (var session = store.OpenSession())
                {
                    session.Load<dynamic>("users/1"); // forcing client configuration update
                }

                Assert.Equal(10, store.Conventions.MaxNumberOfRequestsPerSession);
                Assert.Equal(result.RaftCommandIndex, store.GetRequestExecutor().ClientConfigurationEtag);

                var differentRequestExecutor = store.GetRequestExecutor("DoNotExist");
                Assert.Equal(result.RaftCommandIndex, differentRequestExecutor.ClientConfigurationEtag);

                store.Admin.Server.Send(new PutClientConfigurationOperation(new ClientConfiguration { MaxNumberOfRequestsPerSession = 10, Disabled = true }));
                result = store.Admin.Server.Send(new GetClientConfigurationOperation());

                using (var session = store.OpenSession())
                {
                    session.Load<dynamic>("users/1"); // forcing client configuration update
                }

                Assert.Equal(30, store.Conventions.MaxNumberOfRequestsPerSession);
                Assert.Equal(result.RaftCommandIndex, store.GetRequestExecutor().ClientConfigurationEtag);
                Assert.Equal(result.RaftCommandIndex, differentRequestExecutor.ClientConfigurationEtag);
            }
        }
    }
}