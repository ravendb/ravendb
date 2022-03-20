using System.Threading.Tasks;
using Xunit;
using Xunit.Abstractions;

namespace FastTests.Issues
{
    public class RavenDB_5178 : RavenTestBase
    {
        public RavenDB_5178(ITestOutputHelper output) : base(output)
        {
        }

        [NonLinuxFact]
        public async Task CanUsePathLongerThan260Chars()
        {
            DoNotReuseServer();

            var longName = "LongDatabaseName_" + new string('z', 100);

            using (var store = GetDocumentStore(new Options
            {
                ModifyDatabaseName = _ => longName
            }))
            {
                var db = await Databases.GetDocumentDatabaseInstanceFor(store);
                Assert.Equal(db.Name, longName);
            }
        }
    }
}
