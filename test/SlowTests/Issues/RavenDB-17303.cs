using System.Linq;
using FastTests;
using Raven.Client.Documents;
using Raven.Client.Documents.Indexes;
using Raven.Client.Documents.Linq;
using Raven.Client.Documents.Session;
using Xunit;
using Xunit.Abstractions;

namespace SlowTests.Issues
{
    public class RavenDB_17303 : RavenTestBase
    {
        public RavenDB_17303(ITestOutputHelper output) : base(output)
        {
        }

        private class Item
        {
            public string Name;
        }

        private class Index : AbstractIndexCreationTask<Item>
        {
            public Index()
            {
                Map = items => from i in items
                    select new { i.Name };
                Indexes[i => i.Name] = FieldIndexing.Exact;
            }
        }

        [Fact]
        public void SortNullsWithAlphaNumerics()
        {
            using var store = GetDocumentStore();

            using (var s = store.OpenSession())
            {
                s.Store(new Item { Name = "1BC" });
                s.Store(new Item { Name = null});
                s.Store(new Item { Name = "02BC" });
                s.Store(new Item { Name = "Pla" });
                s.Store(new Item { Name = "Me" });
                s.SaveChanges();
            }
            
            new Index().Execute(store);
            WaitForIndexing(store);
            
            using (var s = store.OpenSession())
            {
                var names = s.Query<Item, Index>()
                    .OrderBy(x => x.Name, OrderingType.AlphaNumeric)
                    .Select(x=>x.Name)
                    .ToList();
                WaitForUserToContinueTheTest(store);
                Assert.Equal(new[]{null, "1BC", "02BC", "Me", "Pla"}, names);
            }

        }
    }
}
