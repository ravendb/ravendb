using System.Diagnostics;
using System.Linq;
using FastTests;
using Raven.Client.Documents.Indexes;
using Raven.Client.Documents.Linq;
using Xunit;
using Xunit.Abstractions;

namespace SlowTests.MailingList
{
    public class Mouhong : RavenTestBase
    {
        public Mouhong(ITestOutputHelper output) : base(output)
        {
        }

        [Fact]
        public void CanSortDescending()
        {
            using (var store = GetDocumentStore())
            {
                new TestItemIndex().Execute(store);

                // Initialize database with unordered documents
                using (var session = store.OpenSession())
                {
                    session.Store(new TestItem
                    {
                        Weight = 0.0198,
                        Letter = "C"
                    });
                    session.Store(new TestItem
                    {
                        Weight = 0.9997,
                        Letter = "D"
                    });
                    session.Store(new TestItem
                    {
                        Weight = 0.0001,
                        Letter = "A"
                    });
                    session.Store(new TestItem
                    {
                        Weight = 0.0099,
                        Letter = "B"
                    });

                    session.SaveChanges();
                }

                Indexes.WaitForIndexing(store);

                using (var session = store.OpenSession())
                {
                    var item = session.Query<TestItem, TestItemIndex>()
                        .OrderByDescending(x => x.Letter)
                        .First();

                    Assert.Equal("D", item.Letter);
                }

                using (var session = store.OpenSession())
                {
                    // - NOT Work: This can NOT retrieve ordered result
                    var item = session.Query<TestItem, TestItemIndex>()
                        .OrderByDescending(x => x.Weight)
                        .First();

                    Assert.Equal(0.9997d, item.Weight);
                }

                using (var session = store.OpenSession())
                {
                    // - NOT Work
                    var item = session.Advanced.DocumentQuery<TestItem, TestItemIndex>()
                        .OrderByDescending(x => x.Weight)
                        .First();

                    Assert.Equal(0.9997d, item.Weight);
                }

                using (var session = store.OpenSession())
                {
                    // - Work: This can retrieve ordered result
                    var query = session.Advanced.DocumentQuery<TestItem, TestItemIndex>()
                        .AddOrder("Weight", true)
                        .ToList();

                    Assert.Equal(0.9997d, query.First().Weight);
                    foreach (var item in query)
                    {
                        Debug.WriteLine(item.Weight);
                    }
                }
            }
        }

        private class TestItem
        {
            public string Id { get; set; }
            public double Weight { get; set; }
            public string Letter { get; set; }
        }

        private class TestItemIndex : AbstractIndexCreationTask<TestItem>
        {
            public TestItemIndex()
            {
                Map = items => from i in items
                               select new
                               {
                                   i.Id,
                                   i.Weight,
                                   i.Letter
                               };

            }
        }
    }
}
