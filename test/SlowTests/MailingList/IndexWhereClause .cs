using System.Linq;
using FastTests;
using Raven.Client.Documents.Indexes;
using Xunit;

namespace SlowTests.MailingList
{
    public class IndexWhereClause : RavenTestBase
    {
        [Fact]
        public void Where_clause_with_greater_than_or_less_than()
        {
            using (var store = GetDocumentStore())
            {
                new MyIndex().Execute(store);

                using (var session = store.OpenSession())
                {
                    session.Store(new Album() { Title = "RavenDB" });
                    session.Store(new Album() { Title = "RavenDB" });
                    session.Store(new Album() { Title = "RavenDB" });
                    session.Store(new Album() { Title = "RavenDB" });

                    session.SaveChanges();
                    var albums = session.Query<Album>().Customize(c => c.WaitForNonStaleResults()).ToList();
                    Assert.Equal(albums.Count, 4);

                    var result1 = session.Query<MyIndex.ReduceResult, MyIndex>().Customize(c => c.WaitForNonStaleResults()).Where(i =>
                            i.Count == 4).ToList();
                    Assert.Equal(result1.Count, 1);

                    var result2 = session.Query<MyIndex.ReduceResult, MyIndex>().Customize(c => c.WaitForNonStaleResults()).Where(i =>
                            i.Count > 1).ToList();
                    Assert.Equal(result2.Count, 1);

                    var result3 = session.Query<MyIndex.ReduceResult, MyIndex>().Customize(c => c.WaitForNonStaleResults()).Where(i =>
                            i.Count < 5).ToList();
                    Assert.Equal(result3.Count, 1);
                }
            }
        }

        private class Album
        {
            public string Id { get; set; }
            public string Title { get; set; }
            public decimal Price { get; set; }
        }

        private class MyIndex : AbstractIndexCreationTask<Album, MyIndex.ReduceResult>
        {
            public class ReduceResult
            {
                public string Title { get; set; }
                public int Count { get; set; }
            }

            public MyIndex()
            {
                this.Map = albums => from album in albums
                                     select new ReduceResult { Title = album.Title, Count = 1 };
                this.Reduce = results => from r in results
                                         group r by r.Title into
                                             g
                                         select new ReduceResult
                                         {
                                             Title = g.Key,
                                             Count = g.Sum(x =>
                                                 x.Count)
                                         };
            }
        }
    }
}
