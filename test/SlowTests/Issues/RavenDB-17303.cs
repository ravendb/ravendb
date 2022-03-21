using System;
using System.Linq;
using System.Threading.Tasks;
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
            Indexes.WaitForIndexing(store);
            
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
        
        [Fact]
        public async Task PagingWithAlphaNumericSorting()
        {
            using (var store = GetDocumentStore())
            {
                int nullNameCount = 4;
                using (var session = store.OpenAsyncSession())
                {
                    new GreatIndex().Execute(store);
                    var document1 = new TestDocument { Document = new TestDocument2() { Name = "EGOR" }, SomeRandomDate = new DateTime(2021, 6, 14) };
                    for (int i = 0; i < nullNameCount; i++)
                    {
                        await session.StoreAsync(document1);
                        var document2 = new TestDocument { Document = new TestDocument2() { Name = null } };
                        await session.StoreAsync(document2);
                    }

                    for (int i = 0; i < 2; i++)
                    {

                        var document3 = new TestDocument { Document = new TestDocument2() { Name = $"{i}_EGOR" } };
                        await session.StoreAsync(document3);
                    }
                    await session.SaveChangesAsync();

                }

                Indexes.WaitForIndexing(store);

                WaitForUserToContinueTheTest(store);
                using (var s = store.OpenAsyncSession())
                {
                    var pageSize = 2;
                    var total = 0;
                    while (total != nullNameCount - pageSize)
                    {
                        var res = await s.Advanced.AsyncRawQuery<TestResult>($@"from index 'GreatIndex' as u
where u.RandomDate < ""2021-11-31""
order by u.DocsName as alphanumeric
select u.DocsName limit {total}, {pageSize}").ToListAsync();

                        foreach (var p in res)
                        {
                            Assert.Null(p.DocsName);
                        }

                        total += pageSize;
                    }

                    Assert.Equal(nullNameCount - pageSize, total);
                }
            }
        }
        private class GreatIndex : AbstractIndexCreationTask<TestDocument>
        {
            public override string IndexName => "GreatIndex";

            public GreatIndex()
            {
                Map = users => from user in users
                    select new
                    {
                        RandomDate = user.SomeRandomDate,
                        DocsName = user.Document.Name
                    };
                StoreAllFields(FieldStorage.Yes);
            }
        }

        private class TestDocument
        {
            public string Id { get; set; }
            public TestDocument2 Document{ get; set; }
            public DateTime SomeRandomDate { get; set; }
        }

        private class TestDocument2
        {
            public string Id { get; set; }
            public string Name { get; set; }
        }

        private class TestResult
        {
            public string Id { get; set; }
            public string DocsName { get; set; }
        }
    }
}
