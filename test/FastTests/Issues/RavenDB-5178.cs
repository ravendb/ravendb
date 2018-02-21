using Raven.Client.ServerWide;
using Raven.Client.ServerWide.Operations;
using Xunit;

namespace FastTests.Issues
{
    public class RavenDB_5178 : RavenTestBase
    {
        [NonLinuxFact]
        public void CanUsePathLongerThan260Chars()
        {
            using (var store = GetDocumentStore())
            {
                var longName = "LongDatabaseName_" + new string('z', 100);

                var doc = new DatabaseRecord(longName);

                store.Maintenance.Server.Send(new CreateDatabaseOperation(doc));

                using (EnsureDatabaseDeletion(doc.DatabaseName, store))
                {
                    store.Database = longName;

                    var db = GetDocumentDatabaseInstanceFor(store).Result;

                    Assert.Equal(db.Name, longName);
                }
            }
        }
    }
}
