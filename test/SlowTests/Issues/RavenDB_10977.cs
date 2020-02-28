using FastTests;
using Raven.Client.Documents;
using Raven.Client.Documents.Session;
using Xunit;
using Xunit.Abstractions;

namespace SlowTests.Issues
{
    public class RavenDB_10977 : RavenTestBase
    {
        public RavenDB_10977(ITestOutputHelper output) : base(output)
        {
        }

        [Fact]
        public void OnSessionCreatedEventWorks()
        {
            using (IDocumentStore store = GetDocumentStore())
            {
                store.OnSessionCreated += (sender, args) =>
                {
                    switch (args.Session)
                    {
                        case DocumentSession session:
                            session.Advanced.MaxNumberOfRequestsPerSession = 999;
                            break;
                        case AsyncDocumentSession asyncSession:
                            asyncSession.Advanced.MaxNumberOfRequestsPerSession = 111;
                            break;
                    }
                };

                using (var session = store.OpenSession())
                {
                    Assert.Equal(999, session.Advanced.MaxNumberOfRequestsPerSession);
                }

                using (var session = store.OpenAsyncSession())
                {
                    Assert.Equal(111, session.Advanced.MaxNumberOfRequestsPerSession);
                }
            }
        }
    }
}
