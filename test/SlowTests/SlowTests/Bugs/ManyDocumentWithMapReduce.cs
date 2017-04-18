using System.Collections.Concurrent;
using System.Collections.Generic;
using System.Linq;
using FastTests;
using Xunit;

namespace SlowTests.SlowTests.Bugs
{
    public class ManyDocumentBeingIndexed : RavenTestBase
    {
        private class TestDocument
        {
            public string Id { get; set; }
        }

        [Fact]
        public void WouldBeIndexedProperly()
        {
            using (var store = GetDocumentStore())
            {
                using (var session = store.OpenSession())
                {
                    // Create the temp index before we populate the db.
                    session.Query<TestDocument>()
                           .Customize(x => x.WaitForNonStaleResultsAsOfNow())
                           .Count();
                }

                const int expectedCount = 5000;
                var ids = new ConcurrentQueue<string>();
                for (int i = 0; i < expectedCount; i++)
                {
                    {
                        using (var session = store.OpenSession())
                        {
                            var testDocument = new TestDocument();
                            session.Store(testDocument);
                            ids.Enqueue(session.Advanced.GetDocumentId(testDocument));
                            session.SaveChanges();
                        }
                    }
                }

                using (var session = store.OpenSession())
                {
                    var items = session.Query<TestDocument>()
                                       .Customize(x => x.WaitForNonStaleResults())
                                       .Take(5005)
                                       .ToList();

                    var missing = new List<int>();
                    for (int i = 1; i <= 5000; i++)
                    {
                        if (items.Any(x => x.Id == i.ToString()) == false)
                            missing.Add(i);
                    }

                    Assert.Equal(expectedCount, items.Count);
                }
            }
        }
    }
}
