using Raven.Abstractions.Data;
using Raven.Server;
using Raven.Tests.Document;
using Xunit;

namespace Raven.Tests.Embedded
{
    public class EmbeddedTests
    {
        [Fact]
        public void Can_get_documents()
        {
            using (var server = new RavenDbServer { RunInMemory = true })
            {
                using (var session = server.DocumentStore.OpenSession())
                {
                    session.Store(new Company {Name = "Company A", Id = "1"});
                    session.Store(new Company {Name = "Company B", Id = "2"});
                    session.SaveChanges();
                }
                JsonDocument[] jsonDocuments = server.DocumentStore.DatabaseCommands.GetDocuments(0, 10, true);
                Assert.Equal(2, jsonDocuments.Length);
            }
        }
    }
}