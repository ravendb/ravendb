using System.Linq;
using FastTests;
using Raven.Client.Documents;
using Raven.Client.Documents.Indexes;
using Raven.Client.Documents.Session;
using Xunit;

namespace SlowTests.MailingList
{
    public class SortTest : RavenTestBase
    {
        [Fact]
        public void ArticlesAreReturnedInIdOrder()
        {
            using (IDocumentStore docStore = GetDocumentStore())
            {
                // Creates 1000 documents with Ids 1 through 1000
                CreateArticles(docStore, 1000);

                CreateIndexes(docStore);
                WaitForIndexing(docStore);

                // verfiy that Article 2 is the second thing
                using (var session = docStore.OpenSession())
                {
                    // verfiy that Article 2 is the second article on the first page of articles
                    var results = session.Query<Article, Articles_byArticleId>()
                                         .OrderBy(article => article.Id)
                                         .Take(10)
                                         .ToList();
                    Assert.NotNull(results);
                    Assert.NotEmpty(results);
                    Assert.Equal(10, results.Count());
                    Assert.Equal("10", results[1].Id);

                    // modifiy Article 2 and wait for the index to update.
                    UpdateArticle(session, 2, "Changed #");
                    WaitForIndexing(docStore);

                    // Article 2 should still be the second item in the first page.
                    results = session.Query<Article, Articles_byArticleId>()
                                         .OrderBy(article => article.Id)
                                         .Take(10)
                                         .ToList();
                    Assert.NotNull(results);
                    Assert.NotEmpty(results);
                    Assert.Equal(10, results.Count());
                    Assert.Equal("10", results[1].Id);
                }
            }
        }

        private void CreateArticles(IDocumentStore docStore, int numArticles)
        {
            for (int i = 1; i <= numArticles; i += 25)
            {
                using (var session = docStore.OpenSession())
                {
                    for (int j = 0; j < 25 && (j + i) <= numArticles; j++)
                    {
                        int id = i + j;
                        CreateArticle(session, id);
                    }
                    session.SaveChanges();
                }
            }
        }

        private static void CreateArticle(IDocumentSession session, int id, string title = null)
        {
            session.Store(new Article
            {
                Id = id.ToString(),
                Title = (title ?? "Title #") + id,
                Authors = new UserRef[] { new UserRef { Id = id, Name = "User Number" + id } },
                Abstract = "Something about nothing",
                Content = "Once upon a midnight dreary, as I pondered weak and weary, ...",
                Tags = new string[] { "Poetry", "Horror" }
            });
        }
        private static void UpdateArticle(IDocumentSession session, int id, string title = null)
        {
            session.Load<Article>(id.ToString()).Title = (title ?? "Title #") + id;
            session.SaveChanges();
        }

        private static void CreateIndexes(IDocumentStore docStore)
        {
            new Articles_byArticleId().Execute(docStore);
        }

        private class Articles_byArticleId : AbstractIndexCreationTask<Article, Article>
        {
            public Articles_byArticleId()
            {
                Map = articles => from article in articles
                                  select new { Id = article.Id };
            }
        }

        private class Article
        {
            public string Id { get; set; }
            public string Title { get; set; }
            public UserRef[] Authors { get; set; }
            public string Abstract { get; set; }
            public string Content { get; set; }
            public string[] Tags { get; set; }
            public Rating Rating { get; set; }
        }

        private class Rating
        {
            public int VoteTotal { get; set; }
            public int WeightTotal { get; set; }
            public int Count { get; set; }
            public float Score { get; set; }
            public int[] VoteCounts { get; set; }
        }

        private class UserRef
        {
            public int Id { get; set; }
            public string Name { get; set; }
        }
    }
}
