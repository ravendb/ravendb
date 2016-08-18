using System.Linq;
using System.Threading.Tasks;
using FastTests;
using Raven.Client.Indexes;
using Xunit;

namespace SlowTests.MailingList
{
    public class CanDeleteIndex : RavenTestBase
    {
        private class AllDocs : AbstractIndexCreationTask<object>
        {
            public AllDocs() { Map = docs => from doc in docs select new { }; }
        }

        [Fact]
        public async Task WithNoErrors()
        {
            using (var store = await GetDocumentStore())
            {
                new AllDocs().Execute(store);
                store.DatabaseCommands.DeleteIndex("AllDocs");
            }
        }
    }
}
