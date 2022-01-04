using System.Diagnostics;
using System.Linq;
using FastTests;
using Raven.Client.Documents.Linq;
using Raven.Client.Documents.Linq.Indexing;
using Xunit;
using Xunit.Abstractions;

namespace SlowTests.Issues
{
    public class RavenDB_17420 : RavenTestBase
    {
        public RavenDB_17420(ITestOutputHelper output) : base(output)
        {
        }

        private class Item
        {
            public string Name;
        }
        
        [Fact]
        public void Can_use_boost_on_in_query()
        {
            using var store = GetDocumentStore();

            using (var s = store.OpenSession())
            {
                s.Store(new Item{Name = "ET"});
                s.SaveChanges();
            }

            using (var s = store.OpenSession())
            {
                Item first = s.Advanced. DocumentQuery<Item>()
                    .WhereIn(x=>x.Name, new[]{"ET", "Alien"}).Boost(0)
                    .First();

                Assert.Equal(0, s.Advanced.GetMetadataFor(first).GetDouble("@index-score"));
            }
        }
    }
}
