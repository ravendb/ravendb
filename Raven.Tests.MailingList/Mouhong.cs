using System.Diagnostics;
using System.Linq;
using Raven.Client.Linq;
using Raven.Client.Indexes;
using Raven.Tests.Common;

using Xunit;

namespace Raven.Tests.MailingList
{
    public class Mouhong : RavenTest
    {
        [Fact]
        public void CanSortDescending()
        {
            using (var store = NewDocumentStore())
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

                WaitForIndexing(store);

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

        public class TestItem
        {
            public string Id { get; set; }
            public double Weight { get; set; }
            public string Letter { get; set; }
        }

        public class TestItemIndex : AbstractIndexCreationTask<TestItem>
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

//                Index(x => x.Weight, Raven.Abstractions.Indexing.FieldIndexing.NotAnalyzed);
                Sort(x => x.Weight, Raven.Abstractions.Indexing.SortOptions.Double);
            }
        }
    }
}