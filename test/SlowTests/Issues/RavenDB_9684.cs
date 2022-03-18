using System.Linq;
using FastTests;
using Raven.Client.ServerWide;
using Raven.Client.ServerWide.Operations;
using Xunit;
using Xunit.Abstractions;

namespace SlowTests.Issues
{
    public class RavenDB_9684 : RavenTestBase
    {
        public RavenDB_9684(ITestOutputHelper output) : base(output)
        {
        }

        private class Document
        {
            public string Id { get; set; }
        }

        [Fact]
        public void Should_be_able_to_query_tenant_without_default_database_set()
        {
            var database = GetDatabaseName();
            using (var store = GetDocumentStore(new Options
            {
                ModifyDocumentStore = documentStore =>
                {
                    documentStore.Database = null;
                }
            }))
            {
                Assert.Null(store.Database);
                store.Maintenance.Server.Send(new CreateDatabaseOperation(new DatabaseRecord(database)));

                using (Databases.EnsureDatabaseDeletion(database, store))
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
