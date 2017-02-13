using Raven.Client.Extensions;
using Raven.Client.Operations.Databases;
using Xunit;

namespace FastTests.Issues
{
    public class RavenDB_5178 : RavenNewTestBase
    {
        [NonLinuxFact]
        public void CanUsePathLongerThan260Chars()
        {
            using (var store = GetDocumentStore())
            {
                var longName = "LongDatabaseName_";

                for (int i = 0; i < 1000; i++)
                {
                    longName += "z";
                }

                var doc = MultiDatabase.CreateDatabaseDocument(longName);

                store.Admin.Send(new CreateDatabaseOperation(doc));

                store.DefaultDatabase = longName;

                var db = GetDocumentDatabaseInstanceFor(store).Result;

                Assert.Equal(db.Name, longName);

            }           
        }
    }
}
