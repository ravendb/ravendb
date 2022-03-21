using FastTests;
using Raven.Client.Exceptions;
using Raven.Client.ServerWide;
using Raven.Client.ServerWide.Operations;
using Xunit;
using Xunit.Abstractions;

namespace SlowTests.Issues
{
    public class RavenDB_7701 : RavenTestBase
    {
        public RavenDB_7701(ITestOutputHelper output) : base(output)
        {
        }

        [Fact]
        public void cannot_create_database_with_the_same_name()
        {
            using (var store = GetDocumentStore())
            {
                var doc = new DatabaseRecord(GetDatabaseName());

                using (Databases.EnsureDatabaseDeletion(doc.DatabaseName, store))
                {
                    store.Maintenance.Server.Send(new CreateDatabaseOperation(doc));

                    Assert.Throws<ConcurrencyException>(() => store.Maintenance.Server.Send(new CreateDatabaseOperation(doc)));
                }
            }
        }
    }
}
