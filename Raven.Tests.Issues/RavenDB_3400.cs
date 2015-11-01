using System;
using System.Collections.Generic;
using System.Linq;
using Raven.Client.Indexes;
using Raven.Tests.Helpers;
using Xunit;

namespace Raven.Tests.Issues
{
    public class RavenDB_3400 :RavenTestBase
    {
        [Fact]
        public void To_Facet_Lazy_Async()
        {
            using (var store = NewDocumentStore())
            {
                new PostsIndex().Execute(store);
                new PostsTransformer().Execute(store);

                using (var documentSession = store.OpenSession())
                {
                    var post = new Post()
                    {
                        DateTime = DateTime.UtcNow,
                        Comments = new List<Comment> {new Comment() {Text = "Test comment", Likes = new List<string> {"users/1", "users/2"}}}
                    };
                    documentSession.Store(post);
                    documentSession.SaveChanges();
                }

                WaitForIndexing(store);

                using (var documentSession = store.OpenSession())
                {
                    var posts = documentSession.Query<PostsIndex.Result, PostsIndex>()
                        .TransformWith<PostsTransformer, PostsTransformer.Result>()
                        .ToList();
                    Assert.Equal(1, posts.Count);
                }
            }
        }


        public class PostsTransformer : AbstractTransformerCreationTask<PostsIndex.Result>
        {
            public class Result
            {
                public string Id { get; set; }
                public DateTime DateTime { get; set; }
                public IEnumerable<ReducedComment> Comments { get; set; }
            }

            public class ReducedComment
            {
                public string Text { get; set; }
                public long LikesCount { get; set; }
            }

            public PostsTransformer()
            {
                TransformResults = posts => from post in posts
                                            select new Result
                                            {
                                                Id = post.Id,
                                                Comments = LoadDocument<Post>(post.Id).Comments.Take(3).Select(x => new ReducedComment()
                                                {
                                                    Text = x.Text,
                                                    LikesCount = Enumerable.Count<string>(x.Likes)
                                                })
                                            };
            }
        }

        public class PostsIndex : AbstractIndexCreationTask<Post, PostsIndex.Result>
        {
            public class Result
            {
                public string Id { get; set; }
                public DateTime DateTime { get; set; }
            }

            public PostsIndex()
            {
                Map = posts => from post in posts
                               select new Result
                               {
                                   Id = post.Id,
                                   DateTime = post.DateTime
                               };
            }
        }

        public class Post
        {
            public string Id { get; set; }
            public DateTime DateTime { get; set; }
            public IList<Comment> Comments { get; set; }

            public Post()
            {
                Comments = new List<Comment>();
            }
        }

        public class Comment
        {
            public string Text { get; set; }
            public IList<string> Likes { get; set; }

            public Comment()
            {
                Likes = new List<string>();
            }
        }
    }

}
