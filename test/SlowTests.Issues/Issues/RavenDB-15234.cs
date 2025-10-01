using System.Collections.Generic;
using System.Linq;
using FastTests;
using Raven.Client.Documents.Indexes;
using Tests.Infrastructure;
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

        [RavenTheory(RavenTestCategory.Indexes | RavenTestCategory.Querying)]
        [RavenData(SearchEngineMode = RavenSearchEngineMode.All, DatabaseMode = RavenDatabaseMode.All)]
        public void CanUseFieldStartingWithNumber_InDynamicQuery(Options options)
        {
            using var store = GetDocumentStore(options);

            using (var s = store.OpenSession())
            {
                var q = s.Query<Item>().Where(x => x.Fields["1-A"] == "Blue").ToString();
                Assert.Equal("from 'Items' where Fields.1-A = $p0", q);
            }
        }

        [RavenTheory(RavenTestCategory.Indexes | RavenTestCategory.Querying)]
        [RavenData(SearchEngineMode = RavenSearchEngineMode.All, DatabaseMode = RavenDatabaseMode.All)]
        public void CanUseFieldWithSlash_InDynamicQuery(Options options)
        {
            using var store = GetDocumentStore(options);

            using (var s = store.OpenSession())
            {
                var q = s.Query<Item>().Where(x => x.Fields["users/1-A"] == "Blue").ToString();
                Assert.Equal("from 'Items' where Fields.'users/1-A' = $p0", q);
            }
        }

        [RavenTheory(RavenTestCategory.Indexes | RavenTestCategory.Querying)]
        [RavenData(SearchEngineMode = RavenSearchEngineMode.All, DatabaseMode = RavenDatabaseMode.All)]
        public void CanUseFieldStartingWithNumber_InStatic(Options options)
        {
            using var store = GetDocumentStore(options);
            new Index().Execute(store);
            using (var s = store.OpenSession())
            {
                var q = s.Query<Item, Index>().Where(x => x.Fields["1-A"] == "Blue");
                Assert.Equal("from index 'Index' where Fields_1-A = $p0", q.ToString());
            }
        }

        [RavenTheory(RavenTestCategory.Indexes | RavenTestCategory.Querying)]
        [RavenData(SearchEngineMode = RavenSearchEngineMode.All, DatabaseMode = RavenDatabaseMode.All)]
        public void CanUseFieldsWithSlash_InStatic(Options options)
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
