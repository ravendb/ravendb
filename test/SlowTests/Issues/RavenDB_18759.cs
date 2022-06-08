using System.Collections.Generic;
using System.Linq;
using FastTests;
using Raven.Client.Documents.Commands.Batches;
using Raven.Client.Documents.Indexes;
using Raven.Client.Documents.Operations;
using Raven.Client.Documents.Operations.Indexes;
using Xunit;
using Xunit.Abstractions;

namespace SlowTests.Issues;

public class RavenDB_18759 : RavenTestBase
{
    public RavenDB_18759(ITestOutputHelper output) : base(output)
    {
    }

    [Fact]
    public void JavaScript_Indexes_Should_Handle_Undefined_Properly()
    {
        using (var store = GetDocumentStore())
        {
            using (var session = store.OpenSession())
            {
                session.Store(new BlogPost
                {
                    Author = "John",
                    Comments = new List<BlogPostComment>
                    {
                        new()
                        {
                            Author = "Moon",
                            Comments = new List<BlogPostComment>
                            {
                                new()
                                {
                                    Author = "Bob"
                                },
                                new()
                                {
                                    Author = "Adel",
                                    Comments = new List<BlogPostComment>
                                    {
                                        new()
                                        {
                                            Author = "Moon"
                                        }
                                    }
                                }
                            }
                        }
                    }
                }, "blogs/1");

                session.SaveChanges();

                session.Advanced.Defer(new PatchCommandData("blogs/1", null, patch: new PatchRequest
                {
                    Script = @"delete this.Comments[0].Comments[0].Comments"
                }));

                session.SaveChanges();
            }

            new BlogPosts_ByCommentAuthor().Execute(store);
            new BlogPosts_ByCommentAuthor_JS().Execute(store);

            Indexes.WaitForIndexing(store);

            var terms1 = store.Maintenance.Send(new GetTermsOperation(new BlogPosts_ByCommentAuthor().IndexName, "Author", fromValue: null));
            var terms2 = store.Maintenance.Send(new GetTermsOperation(new BlogPosts_ByCommentAuthor_JS().IndexName, "Author", fromValue: null));

            Assert.Equal(4, terms1.Length);
            Assert.Equal(terms1.OrderBy(x => x), terms2.OrderBy(x => x));

            var stats1 = store.Maintenance.Send(new GetIndexStatisticsOperation(new BlogPosts_ByCommentAuthor().IndexName));
            var stats2 = store.Maintenance.Send(new GetIndexStatisticsOperation(new BlogPosts_ByCommentAuthor_JS().IndexName));

            Assert.Equal(5, stats1.EntriesCount);
            Assert.Equal(5, stats2.EntriesCount);
        }
    }

    private class BlogPosts_ByCommentAuthor : AbstractIndexCreationTask<BlogPost, BlogPosts_ByCommentAuthor.Result>
    {
        public class Result
        {
            public string Author { get; set; }
        }

        public BlogPosts_ByCommentAuthor()
        {
            Map = blogposts => from blogpost in blogposts
                               let comments = Recurse(blogpost, x => x.Comments)
                               from comment in comments
                               select new Result
                               {
                                   Author = comment.Author
                               };
        }
    }

    public class BlogPosts_ByCommentAuthor_JS : AbstractJavaScriptIndexCreationTask
    {
        public BlogPosts_ByCommentAuthor_JS()
        {
            Maps = new HashSet<string>
                {
                    @"map('BlogPosts', function (blogpost) {
                        return recurse(blogpost, x => x.Comments).map(function (comment) {
                            return {
                                Author: comment.Author
                            };
                        });
                    });"
                };
        }
    }

    private class BlogPost
    {
        public string Author { get; set; }
        public string Title { get; set; }
        public string Text { get; set; }

        public List<BlogPostComment> Comments { get; set; }
    }

    private class BlogPostComment
    {
        public string Author { get; set; }
        public string Text { get; set; }

        public List<BlogPostComment> Comments { get; set; }
    }
}
