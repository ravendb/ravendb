using System;
using System.Linq;
using FastTests;
using Raven.Client.Documents.Indexes;
using Xunit;
using Xunit.Abstractions;

namespace SlowTests.Issues
{
    public class RavenDB_14703 : RavenTestBase
    {
        public RavenDB_14703(ITestOutputHelper output) : base(output)
        {
        }

        [Fact]
        public void ShouldWork()
        {
            const string mySeries = "I";
            const int myYear = 2020;

            using (var store = GetDocumentStore())
            {
                new MyIndex().Execute(store);

                using (var session = store.OpenSession())
                {
                    session.Store(new MyDocument
                    {
                        Id = "MyDocument/1",
                        Series = mySeries,
                        Progressive = 1,
                        Date = new DateTime(myYear, 1, 1)
                    });
                    session.Store(new MyDocument
                    {
                        Id = "MyDocument/2",
                        Series = mySeries,
                        Progressive = 2,
                        Date = new DateTime(myYear, 1, 2)
                    });
                    session.SaveChanges();
                }
                Indexes.WaitForIndexing(store);
                using (var session = store.OpenSession())
                {
                    var q = session.Advanced.DocumentQuery<MyIndex.Result>(typeof(MyIndex).Name);

                    q = q.AndAlso().WhereEquals(d => d.Year, myYear);
                    q = q.AndAlso().WhereEquals(d => d.Series, mySeries);

                    var res = q.ToList();

                    Assert.Equal(1, res.Count);
                    Assert.Equal(2, res.First().Progressive);
                    Assert.Equal("MyDocument/2", res.First().Id);
                }
            }
        }

        private class MyIndex : AbstractIndexCreationTask<MyDocument, MyIndex.Result>
        {
            public class Result
            {
                public int Year { get; set; }
                public string Series { get; set; }
                public int Progressive { get; set; }
                public DateTime Date { get; set; }
                public string Id { get; set; }
            }

            public MyIndex()
            {
                Map = docs => from d in docs
                              select new
                              {
                                  d.Date.Year,
                                  d.Series,
                                  d.Progressive,
                                  d.Date,
                                  d.Id
                              };

                Reduce = results => from result in results
                                    group result by new { result.Year, result.Series } into g
                                    let resultByProgressivoDesc = g.OrderByDescending(r => r.Progressive)
                                    select new
                                    {
                                        g.Key.Year,
                                        g.Key.Series,
                                        Progressive = resultByProgressivoDesc.Select(r => r.Progressive).FirstOrDefault(),
                                        Date = g.OrderByDescending(r => r.Progressive).Select(r => r.Date).FirstOrDefault(),
                                        Id = resultByProgressivoDesc.Select(r => r.Id).FirstOrDefault()
                                    };

                StoreAllFields(FieldStorage.Yes);
            }
        }

        private class MyDocument
        {
            public string Id { get; set; }
            public string Series { get; set; }
            public int Progressive { get; set; }
            public DateTime Date { get; set; }
        }
    }
}
