using System.Collections.Generic;
using System.Linq;
using FastTests;
using Raven.Client.Documents;
using Raven.Client.Documents.Indexes;
using Raven.Client.Documents.Queries.Facets;
using Xunit;

namespace SlowTests.MailingList
{
    public class FacetTest : RavenTestBase
    {
        [Fact]
        public void CanFacetOnList()
        {
            using (var store = GetDocumentStore())
            {
                new BlogIndex().Execute(store);

                var facets = new List<Facet>
                {
                    new Facet
                    {
                        FieldName = "Tags",
                        Options = new FacetOptions
                        {
                            TermSortMode = FacetTermSortMode.CountDesc
                        }
                    }
                };

                using (var session = store.OpenSession())
                {
                    session.Store(new FacetSetup() { Facets = facets, Id = "facets/BlogFacets" });

                    var post1 = new BlogPost
                    {
                        Title = "my first blog",
                        Tags = new List<string>() { "news", "funny" }
                    };
                    session.Store(post1);

                    var post2 = new BlogPost
                    {
                        Title = "my second blog",
                        Tags = new List<string>() { "lame", "news" }
                    };
                    session.Store(post2);
                    session.SaveChanges();
                }

                WaitForIndexing(store);

                using (var session = store.OpenSession())
                {
                    var q = session.Query<BlogPost, BlogIndex>();

                    var f = q.AggregateUsing("facets/BlogFacets").Execute();

                    Assert.Equal(1, f.Count);
                    Assert.Equal(3, f["Tags"].Values.Count);
                    Assert.Equal("news", f["Tags"].Values[0].Range);
                    Assert.Equal(2, f["Tags"].Values[0].Count);

                }

            }
        }

        private class BlogPost
        {
            public string Title { get; set; }
            public List<string> Tags { get; set; }
        }

        private class BlogIndex : AbstractIndexCreationTask<BlogPost>
        {
            public BlogIndex()
            {
                Map = blogs => from b in blogs
                               select new
                               {
                                   Tags = b.Tags
                               };
                Store("Tags", FieldStorage.Yes);
                Index("Tags", FieldIndexing.Exact);
            }
        }
    }
}
