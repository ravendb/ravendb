using System.Collections.Generic;
using System.Linq;
using FastTests;
using Raven.Abstractions.Data;
using Raven.Client;
using Raven.Client.Indexes;
using Xunit;

namespace SlowTests.MailingList
{
    public class FacetTest : RavenTestBase
    {
        [Fact(Skip = "Missing feature: Facets")]
        public void CanFacetOnList()
        {
            using (var store = GetDocumentStore())
            {
                new BlogIndex().Execute(store);

                var facets = new List<Facet>{
                    new Facet{Name = "Tags", TermSortMode= FacetTermSortMode.HitsDesc}
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

                    var f = q.ToFacets("facets/BlogFacets");

                    Assert.Equal(1, f.Results.Count);
                    Assert.Equal(3, f.Results["Tags"].Values.Count);
                    Assert.Equal("news", f.Results["Tags"].Values[0].Range);
                    Assert.Equal(2, f.Results["Tags"].Values[0].Hits);

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
                Store("Tags", Raven.Abstractions.Indexing.FieldStorage.Yes);
                Index("Tags", Raven.Abstractions.Indexing.FieldIndexing.NotAnalyzed);
            }
        }
    }
}
