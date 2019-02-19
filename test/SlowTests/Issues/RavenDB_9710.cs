using FastTests;
using Xunit;

namespace SlowTests.Issues
{
    public class RavenDB_9710 : RavenTestBase
    {
        [Fact]
        public void Should_be_able_to_use_smuggler_for_without_default_database()
        {
            Options options = new Options
            {
                ModifyDocumentStore = store => store.Database = null
            };
            using (var store = GetDocumentStore(options))
            {
                var smuggler = store.Smuggler.ForDatabase("some-db");
            }
        }
    }
}
