using System.Collections.Generic;
using System.Linq;
using FastTests;
using Raven.Client.Documents;
using Raven.Client.Documents.Indexes;
using Xunit;
using Xunit.Abstractions;

namespace SlowTests.Issues
{
    public class RavenDB_15234 : RavenTestBase
    {

        public RavenDB_15234(ITestOutputHelper output) : base(output)
        {
        }

        public class Item
        {
            public Dictionary<string, string> Fields;
        }

        public class Index : AbstractIndexCreationTask<Item>
        {
            public Index()
            {
                Map = items => from item in items
                    select new {_ = item.Fields.Select(f => CreateField("Fields_" + f.Key, f.Value))};
            }
        }
        
        [Fact]
        public void CanUseFieldStartingWithNumber_InDynamicQuery()
        {
            using var store = GetDocumentStore();

            using (var s = store.OpenSession())
            {
                var q = s.Query<Item>().Where(x => x.Fields["1-A"] == "Blue").ToString();
                Assert.Equal("from 'Items' where Fields.1-A = $p0", q);
            }
        }
        
        [Fact]
        public void CanUseFieldWithSlash_InDynamicQuery()
        {
            using var store = GetDocumentStore();

            using (var s = store.OpenSession())
            {
                var q = s.Query<Item>().Where(x => x.Fields["users/1-A"] == "Blue").ToString();
                Assert.Equal("from 'Items' where Fields.'users/1-A' = $p0", q);
            }
        }
        
        [Fact]
        public void CanUseFieldStartingWithNumber_InStatic()
        {
            using var store = GetDocumentStore();
            new Index().Execute(store);
            using (var s = store.OpenSession())
            {
                var q = s.Query<Item, Index>().Where(x => x.Fields["1-A"] == "Blue").ToString();
                Assert.Equal("from index 'Index' where Fields_1-A = $p0", q);
            }
        }
        
        [Fact]
        public void CanUseFieldsWithSlash_InStatic()
        {
            using var store = GetDocumentStore();
            new Index().Execute(store);
            using (var s = store.OpenSession())
            {
                var q = s.Query<Item, Index>().Where(x => x.Fields["users/1-A"] == "Blue").ToString();
                Assert.Equal("from index 'Index' where 'Fields_users/1-A' = $p0", q);
            }
        }
    }
}
