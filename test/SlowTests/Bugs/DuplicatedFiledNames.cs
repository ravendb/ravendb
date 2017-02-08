using FastTests;
using Raven.NewClient.Client.Indexing;
using Raven.NewClient.Operations.Databases.Indexes;
using Xunit;
using System.Linq;

namespace SlowTests.Bugs
{
    public class DuplicatedFiledNames : RavenNewTestBase
    {
        [Fact]
        public void ShouldNotDoThat()
        {
            using(var store = GetDocumentStore())
            {
                store.Admin.Send(new PutIndexOperation("test", new IndexDefinition
                {
                    Maps = { "from doc in docs.ClickBalances select new { doc.AccountId } " }
                }));

                using(var s = store.OpenSession())
                {
                    var accountId = 1;
                    s.Query<ClickBalance>("test")
                        .Where(x => x.AccountId == accountId)
                        .ToList();
                }
            }
        }

        private class ClickBalance
        {
            public int AccountId { get; set; }
        }
    }

 
}
