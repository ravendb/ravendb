using Raven.Client.Exceptions;
using Raven.Client.Server;
using Raven.Client.Server.Operations;
using Xunit;

namespace FastTests.Issues
{
    public class RavenDB_7701 : RavenTestBase
    {
        [Fact]
        public void cannot_create_database_with_the_same_name()
        {
            using (var store = GetDocumentStore())
            {
                var doc = MultiDatabase.CreateDatabaseDocument("test");

                store.Admin.Server.Send(new CreateDatabaseOperation(doc));

                Assert.Throws<ConcurrencyException>(() => store.Admin.Server.Send(new CreateDatabaseOperation(doc)));
            }
        }
    }
}
