using Xunit;

namespace FastTests.Issues
{
    public class RavenDB_5178 : RavenTestBase
    {
        [NonLinuxFact]
        public void CanUsePathLongerThan260Chars()
        {
            DoNotReuseServer();

            var longName = "LongDatabaseName_" + new string('z', 100);

            using (var store = GetDocumentStore(new Options
            {
                ModifyDatabaseName = _ => longName
            }))
            {
                var db = GetDocumentDatabaseInstanceFor(store).Result;
                Assert.Equal(db.Name, longName);
            }
        }
    }
}
