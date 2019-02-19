using Raven.Client.Documents;
using Raven.Client.Documents.Indexes;
using System.Collections.Generic;
using System.Linq;
using System.Threading.Tasks;
using FastTests;
using Xunit;

namespace SlowTests.Issues
{
    public class RavenDB_9104 : RavenTestBase
    {
        public class BlogPost
        {
            public string Id { get; set; }

            public string Title { get; set; }

            public string Username { get; set; }

            public string Post { get; set; }

            public List<string> BlogComments { get; set; }
        }

        public class BlogComment
        {
            public string Id { get; set; }

            public string Username { get; set; }

            public string Comment { get; set; }

            public string BlogCommentRatingId { get; set; }
        }

        public class BlogCommentRating
        {
            public string Id { get; set; }

            public decimal Rating { get; set; }
        }

        public class BlogPostAll
            : AbstractMultiMapIndexCreationTask<BlogPostAll.Mapping>
        {
            public class Mapping
            {
                public string Id { get; set; }

                public string Title { get; set; }

                public string Username { get; set; }

                public IEnumerable<string> Comments { get; set; }
            }

            public BlogPostAll()
            {
                AddMap<BlogPost>(results =>
                    from result in results
                    let blogComments = LoadDocument<BlogComment>(result.BlogComments)
                    select new Mapping
                    {
                        Id = result.Id,
                        Username = result.Username,
                        Title = result.Title,
                        Comments = blogComments.Select(a => a.Comment)
                    });
            }
        }

        public class BlogPostWithAverageRatingAll
            : AbstractMultiMapIndexCreationTask<BlogPostAll.Mapping>
        {
            public class Mapping
            {
                public string Id { get; set; }

                public string Title { get; set; }

                public string Username { get; set; }

                public IEnumerable<string> Comments { get; set; }

                public decimal AverageRating { get; set; }
            }

            public BlogPostWithAverageRatingAll()
            {
                AddMap<BlogPost>(results =>
                    from result in results
                    let blogComments = LoadDocument<BlogComment>(result.BlogComments)
                    let blogCommentRatings = LoadDocument<BlogCommentRating>(blogComments.Select(a => a.BlogCommentRatingId)) 
                        ?? new[] { new BlogCommentRating { Rating = 0}}
                    select new Mapping
                    {
                        Id = result.Id,
                        Username = result.Username,
                        Title = result.Title,
                        Comments = blogComments.Select(a => a.Comment),
                        AverageRating = blogCommentRatings.Average(a => a.Rating)
                    });
            }
        }

        [Fact]
        public async Task NullListAsync()
        {
            using (var store = GetDocumentStore())
            {
                await store.ExecuteIndexAsync(new BlogPostAll());

                using (var session = store.OpenAsyncSession())
                {
                    for (var i = 0; i < 5; i++)
                    {
                        await session.StoreAsync(new BlogPost
                        {
                            Title = "Lorem Ipsum " + i,
                            Username = "Anonymous",
                            Post = "Lorem Ipsum is simply dummy text of the printing and typesetting industry. " +
                            "Lorem Ipsum has been the industry's standard dummy text ever since the 1500s, when an unknown printer took a galley of type and scrambled it to make a type specimen book. " +
                            "It has survived not only five centuries, but also the leap into electronic typesetting, remaining essentially unchanged. " +
                            "It was popularised in the 1960s with the release of Letraset sheets containing Lorem Ipsum passages, and more recently with desktop publishing software like Aldus PageMaker including versions of Lorem Ipsum."
                        }, "BlogPosts/" + i);
                    }

                    await session.SaveChangesAsync();

                    var query = await session.Query<BlogPostAll.Mapping, BlogPostAll>()
                        .Customize(x=>x.WaitForNonStaleResults())
                        .As<BlogPost>()
                        .ToListAsync();

                    Assert.Equal(5, query.Count);
                }
            }
        }

        [Fact]
        public async Task EmptyListAsync()
        {
            using (var store = GetDocumentStore())
            {
                await store.ExecuteIndexAsync(new BlogPostAll());

                using (var session = store.OpenAsyncSession())
                {
                    for (var i = 0; i < 5; i++)
                    {
                        await session.StoreAsync(new BlogPost
                        {
                            Title = "Lorem Ipsum " + i,
                            Username = "Anonymous",
                            BlogComments = new List<string>(),
                            Post = "Lorem Ipsum is simply dummy text of the printing and typesetting industry. " +
                            "Lorem Ipsum has been the industry's standard dummy text ever since the 1500s, when an unknown printer took a galley of type and scrambled it to make a type specimen book. " +
                            "It has survived not only five centuries, but also the leap into electronic typesetting, remaining essentially unchanged. " +
                            "It was popularised in the 1960s with the release of Letraset sheets containing Lorem Ipsum passages, and more recently with desktop publishing software like Aldus PageMaker including versions of Lorem Ipsum."
                        }, "BlogPosts/" + i);
                    }

                    await session.SaveChangesAsync();

                    

                    var query = await session.Query<BlogPostAll.Mapping, BlogPostAll>()
                        .Customize(x => x.WaitForNonStaleResults())
                        .As<BlogPost>()
                        .ToListAsync();

                    Assert.Equal(5, query.Count);
                }
            }
        }

        [Fact]
        public async Task ListContainingNullAsync()
        {
            using (var store = GetDocumentStore())
            {
                await store.ExecuteIndexAsync(new BlogPostAll());

                using (var session = store.OpenAsyncSession())
                {
                    for (var i = 0; i < 5; i++)
                    {
                        await session.StoreAsync(new BlogPost
                        {
                            Title = "Lorem Ipsum " + i,
                            Username = "Anonymous",
                            BlogComments = new List<string>()
                            {
                                null
                            },

                            Post = "Lorem Ipsum is simply dummy text of the printing and typesetting industry. " +
                            "Lorem Ipsum has been the industry's standard dummy text ever since the 1500s, when an unknown printer took a galley of type and scrambled it to make a type specimen book. " +
                            "It has survived not only five centuries, but also the leap into electronic typesetting, remaining essentially unchanged. " +
                            "It was popularised in the 1960s with the release of Letraset sheets containing Lorem Ipsum passages, and more recently with desktop publishing software like Aldus PageMaker including versions of Lorem Ipsum."
                        }, "BlogPosts/" + i);
                    }

                    await session.SaveChangesAsync();

                    
                    var query = await session.Query<BlogPostAll.Mapping, BlogPostAll>()
                        .Customize(x => x.WaitForNonStaleResults())
                        .As<BlogPost>()
                        .ToListAsync();

                    Assert.Equal(5, query.Count);
                }
            }
        }

        [Fact]
        public async Task ListWithRatingAsync()
        {
            using (var store = GetDocumentStore())
            {
                await store.ExecuteIndexAsync(new BlogPostWithAverageRatingAll());

                using (var session = store.OpenAsyncSession())
                {
                    var rating = new BlogCommentRating()
                    {
                        Rating = 4
                    };

                    await session.StoreAsync(rating, "BlogCommentRating/" + 1);

                    for (var i = 0; i < 5; i++)
                    {
                        await session.StoreAsync(new BlogComment
                        {
                            Username = "Anonymous",
                            BlogCommentRatingId = "BlogCommentRating/" + 1,
                            Comment = "Lorem Ipsum is simply dummy text of the printing and typesetting industry. " +
                            "Lorem Ipsum has been the industry's standard dummy text ever since the 1500s, when an unknown printer took a galley of type and scrambled it to make a type specimen book. " +
                            "It has survived not only five centuries, but also the leap into electronic typesetting, remaining essentially unchanged. " +
                            "It was popularised in the 1960s with the release of Letraset sheets containing Lorem Ipsum passages, and more recently with desktop publishing software like Aldus PageMaker including versions of Lorem Ipsum."
                        }, "BlogComments/" + i);
                    }

                    for (var i = 0; i < 5; i++)
                    {
                        await session.StoreAsync(new BlogPost
                        {
                            Title = "Lorem Ipsum " + i,
                            Username = "Anonymous",
                            BlogComments = new List<string>()
                            {
                                "BlogComments/" + i,
                            },
                            Post = "Lorem Ipsum is simply dummy text of the printing and typesetting industry. " +
                            "Lorem Ipsum has been the industry's standard dummy text ever since the 1500s, when an unknown printer took a galley of type and scrambled it to make a type specimen book. " +
                            "It has survived not only five centuries, but also the leap into electronic typesetting, remaining essentially unchanged. " +
                            "It was popularised in the 1960s with the release of Letraset sheets containing Lorem Ipsum passages, and more recently with desktop publishing software like Aldus PageMaker including versions of Lorem Ipsum."
                        }, "BlogPosts/" + i);
                    }

                    await session.SaveChangesAsync();

                    
                    var query = await session.Query<BlogPostWithAverageRatingAll.Mapping, BlogPostWithAverageRatingAll>()
                        .Customize(x => x.WaitForNonStaleResults())
                        .As<BlogPost>()
                        .ToListAsync();

                    Assert.Equal(5, query.Count);
                }
            }
        }

        [Fact]
        public async Task ListWithRatingNullAsync()
        {
            using (var store = GetDocumentStore())
            {
                await store.ExecuteIndexAsync(new BlogPostWithAverageRatingAll());

                using (var session = store.OpenAsyncSession())
                {
                    for (var i = 0; i < 5; i++)
                    {
                        await session.StoreAsync(new BlogPost
                        {
                            Title = "Lorem Ipsum " + i,
                            Username = "Anonymous",
                            BlogComments = null,
                            Post = "Lorem Ipsum is simply dummy text of the printing and typesetting industry. " +
                            "Lorem Ipsum has been the industry's standard dummy text ever since the 1500s, when an unknown printer took a galley of type and scrambled it to make a type specimen book. " +
                            "It has survived not only five centuries, but also the leap into electronic typesetting, remaining essentially unchanged. " +
                            "It was popularised in the 1960s with the release of Letraset sheets containing Lorem Ipsum passages, and more recently with desktop publishing software like Aldus PageMaker including versions of Lorem Ipsum."
                        }, "BlogPosts/" + i);
                    }

                    await session.SaveChangesAsync();
                    
                    var query = await session.Query<BlogPostWithAverageRatingAll.Mapping, BlogPostWithAverageRatingAll>()
                        .Customize(x=>x.WaitForNonStaleResults())
                        .As<BlogPost>()
                        .ToListAsync();

                    Assert.Equal(5, query.Count);
                }
            }
        }
    }
}
