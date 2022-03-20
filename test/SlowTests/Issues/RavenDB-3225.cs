using System;
using System.Collections.Generic;
using System.Linq;
using FastTests;
using Raven.Client.Documents.Linq;
using Raven.Client.Documents.Session;
using Xunit;
using Xunit.Abstractions;

namespace SlowTests.Issues
{
    public class TestingQuery : RavenTestBase
    {
        public TestingQuery(ITestOutputHelper output) : base(output)
        {
        }

        private class TestDataObject
        {
            public string A { get; set; }
            public string B { get; set; }
            public string C { get; set; }
            public DateTimeOffset Created { get; set; }
            public TimeSpan Span { get; set; }
        }

        [Fact]
        public void DateTimeMultipleTermsQueryShouldWork()
        {
            using (var store = GetDocumentStore())
            {
                using (var session = store.OpenSession())
                {
                    session.Store(new TestDataObject() { A = "X", B = "B1", C = "C1", Created = new DateTimeOffset(2000, 1, 10, 9, 30, 44, new TimeSpan()) });
                    session.Store(new TestDataObject() { A = "X", B = "B2", C = "C2", Created = new DateTimeOffset(2001, 1, 10, 9, 30, 44, new TimeSpan()) });
                    session.Store(new TestDataObject() { A = "X", B = "B3", C = "C3", Created = new DateTimeOffset(2002, 2, 10, 9, 30, 44, new TimeSpan()) });
                    session.Store(new TestDataObject() { A = "Y", B = "B4", C = "C4", Created = new DateTimeOffset(2003, 3, 10, 9, 30, 44, new TimeSpan()) });
                    session.Store(new TestDataObject() { A = "Y", B = "B5", C = "C5", Created = new DateTimeOffset(2004, 4, 10, 9, 30, 44, new TimeSpan()) });

                    session.SaveChanges();
                }

                using (var session = store.OpenSession())
                {
                    var ids = new List<string> { "X" };

                    QueryStatistics stats;
                    var query = session.Query<TestDataObject>().Statistics(out stats);

                    query = query.Where(t => t.A.In(ids)); //PUTTING THIS FIRST RETURNS 0 RESULTS
                    query = query.Where(t => t.Created >= new DateTimeOffset(2001, 1, 1, 8, 1, 12, new TimeSpan()));

                    var sueryStr = query.ToString();
                    //WaitForUserToContinueTheTest(store);
                    Indexes.WaitForIndexing(store);
                    var results = query.ToList();

                    Assert.NotNull(results);
                    Assert.Equal(2, results.Count);
                }
            }
        }
    }
}
