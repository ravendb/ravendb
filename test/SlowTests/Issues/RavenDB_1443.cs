using System;
using System.Collections.Generic;
using System.Linq;
using System.Linq.Expressions;
using FastTests;
using Raven.Client.Documents;
using Raven.Client.Documents.Indexes;
using Raven.Client.Documents.Queries.MoreLikeThis;
using Xunit;
using Xunit.Abstractions;

namespace SlowTests.Issues
{
    public class RavenDB_1443 : RavenTestBase
    {
        public RavenDB_1443(ITestOutputHelper output) : base(output)
        {
        }

        public class Article
        {
            public string Id { get; set; }
            public string Name { get; set; }
            public string ArticleBody { get; set; }
        }

        public class ArticleIndex : AbstractIndexCreationTask<Article>
        {
            public ArticleIndex()
            {
                Map = docs => from doc in docs
                              select new
                              {
                                  doc.ArticleBody
                              };

                Stores = new Dictionary<Expression<Func<Article, object>>, FieldStorage>
                         {
                             {
                                 x => x.ArticleBody, FieldStorage.Yes
                             }
                         };
            }
        }

        [Fact]
        public void CanUseMoreLikeThisLazy()
        {
            using (var store = GetDocumentStore())
            {
                ActualTestCase(store);
            }
        }

        private void ActualTestCase(IDocumentStore store)
        {
            var lorem = new[]
            {
                "Lorem ipsum dolor sit amet, consectetur adipiscing elit. Etiam semper, leo sit amet auctor aliquam, erat ligula dictum eros",
                " Aliquam eleifend, dui vitae fermentum bibendum, nunc sem tempus risus, posuere interdum arcu diam sit amet sem",
                " Suspendisse a nunc rutrum, rutrum arcu ut, tempor est."
            };

            var index = new ArticleIndex();
            store.ExecuteIndex(index);

            using (var session = store.OpenSession())
            {
                for (int i = 0; i < 10; i++)
                {
                    var article = new Article()
                    {
                        Name = "Art/" + i,
                        ArticleBody = lorem[i % 3]
                    };
                    session.Store(article, "articles/" + i);
                }

                session.SaveChanges();
            }

            Indexes.WaitForIndexing((DocumentStore)store);

            using (var session = store.OpenSession())
            {
                var oldRequests = session.Advanced.NumberOfRequests;

                var moreLikeThisLazy = session.Query<Article, ArticleIndex>()
                    .MoreLikeThis(f => f.UsingDocument(x => x.Id == "articles/0").WithOptions(new MoreLikeThisOptions
                    {
                        MinimumTermFrequency = 0,
                        MinimumDocumentFrequency = 0
                    }))
                    .Lazily();

                Assert.Equal(oldRequests, session.Advanced.NumberOfRequests);
                Assert.NotEmpty(moreLikeThisLazy.Value);

                Assert.Equal(oldRequests + 1, session.Advanced.NumberOfRequests);
            }
        }

    }
}
