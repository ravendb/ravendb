using System.Collections.Generic;
using System.Linq;
using FastTests;
using Raven.Client.Documents.Indexes;
using Raven.Client.Documents.Queries.Highlighting;
using Xunit;
using Xunit.Abstractions;

namespace SlowTests.Issues
{
    public class SearchHighlightings : RavenTestBase
    {
        public SearchHighlightings(ITestOutputHelper output) : base(output)
        {
        }

        private class BlogPost
        {
            public string Id { get; set; }
            public string Content { get; set; }
            public List<string> Tags { get; set; }
        }

        private class BlogPosts_ForSearch : AbstractIndexCreationTask<BlogPost, BlogPosts_ForSearch.Result>
        {
            public class Result
            {
                public string Id { get; set; }
                public string[] SearchText { get; set; }
            }

            public BlogPosts_ForSearch()
            {
                Map = posts =>
                    from post in posts
                    select new Result
                    {
                        Id = post.Id,
                        SearchText = post.Tags
                            .Concat(new[]
                                {
                                post.Content
                                })
                            .ToArray(),
                    };

                Index(f => f.SearchText, FieldIndexing.Search);
                TermVector(f => f.SearchText, FieldTermVector.WithPositionsAndOffsets);

                StoreAllFields(FieldStorage.Yes);
            }
        }

        [Fact]
        public void WorkWithPostfixWildcard()
        {
            using (var store = GetDocumentStore())
            {
                new BlogPosts_ForSearch().Execute(store);

                using (var session = store.OpenSession())
                {
                    session.Store(new BlogPost
                    {
                        Content = @"Lorem ipsum dolor sit amet, consetetur sadipscing elitr, sed diam nonumy eirmod tempor invidunt ut labore et dolore magna aliquyam erat, sed diam voluptua.",
                        Tags = new List<string> { "Raven", "Microsoft", "Apple" }
                    });
                    session.SaveChanges();
                }

                Indexes.WaitForIndexing(store);

                using (var session = store.OpenSession())
                {
                    var result = session.Advanced.DocumentQuery<object, BlogPosts_ForSearch>()
                        .SelectFields<BlogPosts_ForSearch.Result>()
                        .Highlight(f => f.SearchText, 128, 10, out Highlightings highlightings)
                        .Search(f => f.SearchText, "lorem")
                        .ToList();

                    //That works
                    Assert.NotEmpty(result);
                    Assert.NotEmpty(highlightings.GetFragments(result[0].Id));

                }

                using (var session = store.OpenSession())
                {
                    Highlightings highlightings;

                    var result = session.Advanced.DocumentQuery<object, BlogPosts_ForSearch>()
                        .SelectFields<BlogPosts_ForSearch.Result>()
                        .Highlight(f => f.SearchText, 128, 10, out highlightings)
                        .Search(f => f.SearchText, "lore*") //Postfix wildcard here
                        .ToList();

                    Assert.NotEmpty(result);
                    Assert.NotEmpty(highlightings.GetFragments(result[0].Id)); //No highlightings
                }
            }
        }
    }
}
