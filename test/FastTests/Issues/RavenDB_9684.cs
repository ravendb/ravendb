using System.Linq;
using Raven.Client.ServerWide;
using Raven.Client.ServerWide.Operations;
using Xunit;

namespace FastTests.Issues
{
    public class RavenDB_9684 : RavenTestBase
    {
        private class Document
        {
            public string Id { get; set; }
        }

        [Fact]
        public void Should_be_able_to_query_tenant_without_default_database_set()
        {
            var database = "my-db";
            using (var store = GetDocumentStore())
            {
                store.Database = null;
                store.Maintenance.Server.Send(new CreateDatabaseOperation(new DatabaseRecord(database)));
                using (var session = store.OpenSession(database))
                {
                    session.Query<Document>()
                        .Customize(x => x.WaitForNonStaleResults())
                        .ToList();
                }
            }
        }
    }
}
