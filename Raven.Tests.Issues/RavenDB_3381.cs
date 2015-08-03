using System.Collections.Generic;
using System.Linq;
using Raven.Abstractions.Indexing;
using Raven.Client;
using Raven.Client.Indexes;
using Raven.Tests.Helpers;
using Xunit;

public class SearchHighlightings : RavenTestBase
{
    public class BlogPost
    {
        public string Id { get; set; }
        public string Content { get; set; }
        public List<string> Tags { get; set; }
    }

    public class BlogPosts_ForSearch : AbstractIndexCreationTask<BlogPost, BlogPosts_ForSearch.Result>
    {
        public class Result
        {
            public string Id { get; set; }
            public string[] SearchText { get; set; }
        }

        public BlogPosts_ForSearch()
        {
            this.Map = posts =>
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

            this.Index(f => f.SearchText, FieldIndexing.Analyzed);
            this.TermVector(f => f.SearchText, FieldTermVector.WithPositionsAndOffsets);

            this.StoreAllFields(FieldStorage.Yes);
        }
    }

    [Fact]
    public void WorkWithPostfixWildcard()
    {
        using (var store = NewRemoteDocumentStore(fiddler:true))        
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

            WaitForIndexing(store);

            using (var session = store.OpenSession())
            {
                FieldHighlightings highlightings;

                var result = session.Advanced.DocumentQuery<object, BlogPosts_ForSearch>()
                    .SelectFields<BlogPosts_ForSearch.Result>()
                    .Highlight(f => f.SearchText, 128, 10, out highlightings)
                    .Search(f => f.SearchText, "lorem", EscapeQueryOptions.EscapeAll)
                    .ToList();

                //That works
                Assert.NotEmpty(result);
                Assert.NotEmpty(highlightings.GetFragments(result[0].Id));
            }

            using (var session = store.OpenSession())
            {
                FieldHighlightings highlightings;

                var result = session.Advanced.DocumentQuery<object, BlogPosts_ForSearch>()
                    .SelectFields<BlogPosts_ForSearch.Result>()
                    .Highlight(f => f.SearchText, 128, 10, out highlightings)
                    .Search(f => f.SearchText, "lore*", EscapeQueryOptions.AllowPostfixWildcard) //Postfix wildcard here
                    .ToList();

                Assert.NotEmpty(result);
                Assert.NotEmpty(highlightings.GetFragments(result[0].Id)); //No highlightings
            }
        }
    }
}