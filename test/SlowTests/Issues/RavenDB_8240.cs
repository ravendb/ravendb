using System.Collections.Generic;
using System.Linq;
using FastTests;
using Orders;
using Raven.Client.Documents.Indexes;
using Raven.Tests.Core.Utils.Entities;
using Xunit;
using Xunit.Abstractions;

namespace SlowTests.Issues
{
    public class RavenDB_8240 : RavenTestBase
    {
        public RavenDB_8240(ITestOutputHelper output) : base(output)
        {
        }

        [Fact]
        public void Can_group_by_constant_in_static_index()
        {
            using (var store = GetDocumentStore())
            {
                var indexes = new List<AbstractIndexCreationTask>()
                {
                    new Posts_Statistics_GroupByString(),
                    new Posts_Statistics_GroupByNumber(),
                    new Posts_Statistics_GroupByFalse(),
                    new Posts_Statistics_GroupByNull(),
                    new Posts_Statistics_QuerySyntax_GroupByString(),
                    new Posts_Statistics_QuerySyntax_GroupByNumber(),
                    new Posts_Statistics_QuerySyntax_GroupByTrue(),
                    new Posts_Statistics_QuerySyntax_GroupByNull(),
                };

                foreach (var index in indexes)
                {
                    store.ExecuteIndex(index);
                }

                using (var session = store.OpenSession())
                {
                    session.Store(new Post
                    {
                        Comments = new Post[4]
                    });

                    session.Store(new Post
                    {
                        Comments = new Post[4]
                    });

                    session.Store(new Post
                    {
                        Comments = new Post[4]
                    });

                    session.SaveChanges();

                    Indexes.WaitForIndexing(store);

                    foreach (var index in indexes)
                    {
                        var results = session.Query<Posts_Statistics_GroupByString.Result>(index.IndexName).ToList();

                        Assert.Equal(1, results.Count);
                        Assert.Equal(3, results[0].PostsCount);
                        Assert.Equal(12, results[0].CommentsCount);
                        store.ExecuteIndex(index);
                    }
                }
            }
        }

        [Fact]
        public void Can_group_by_constant_in_dynamic_query()
        {
            using (var store = GetDocumentStore())
            {
                using (var session = store.OpenSession())
                {
                    session.Store(new Product
                    {
                        PricePerUnit = 10
                    });

                    session.Store(new Product
                    {
                        PricePerUnit = 10
                    });

                    session.SaveChanges();

                    foreach (var query in new[]
                    {
                        session.Query<Product>().GroupBy(x => "Name").Select(g => new // "Name" needs to be a constant, not a product field name
                        {
                            TotalPrice = g.Sum(x => x.PricePerUnit),
                            ProductsCount = g.Count()
                        }),
                        session.Query<Product>().GroupBy(x => 1).Select(g => new 
                        {
                            TotalPrice = g.Sum(x => x.PricePerUnit),
                            ProductsCount = g.Count()
                        }),
                        session.Query<Product>().GroupBy(x => true).Select(g => new 
                        {
                            TotalPrice = g.Sum(x => x.PricePerUnit),
                            ProductsCount = g.Count()
                        }),
                        session.Query<Product>().GroupBy(x => (string)null).Select(g => new
                        {
                            TotalPrice = g.Sum(x => x.PricePerUnit),
                            ProductsCount = g.Count()
                        })
                    })
                    {
                        var results = query.ToList();

                        Assert.Equal(1, results.Count);
                        Assert.Equal(20, results[0].TotalPrice);
                        Assert.Equal(2, results[0].ProductsCount);
                    }
                }
            }
        }

        public class Posts_Statistics_GroupByString : AbstractIndexCreationTask<Post, Posts_Statistics_GroupByString.Result>
        {
            public class Result
            {
                public int? PostsCount { get; set; }
                public int CommentsCount { get; set; }
            }

            public Posts_Statistics_GroupByString()
            {
                Map = posts => from postComment in posts
                    select new
                    {
                        PostsCount = 1,
                        CommentsCount = postComment.Comments.Length
                    };

                Reduce = results => from result in results
                    group result by "constant"
                    into g
                    select new
                    {
                        PostsCount = g.Sum(x => x.PostsCount),
                        CommentsCount = g.Sum(x => x.CommentsCount),
                    };
            }
        }

        public class Posts_Statistics_GroupByNumber : AbstractIndexCreationTask<Post, Posts_Statistics_GroupByString.Result>
        {
            public Posts_Statistics_GroupByNumber()
            {
                Map = posts => from postComment in posts
                    select new
                    {
                        PostsCount = 1,
                        CommentsCount = postComment.Comments.Length
                    };

                Reduce = results => from result in results
                    group result by 1
                    into g
                    select new
                    {
                        PostsCount = g.Sum(x => x.PostsCount),
                        CommentsCount = g.Sum(x => x.CommentsCount),
                    };
            }
        }

        public class Posts_Statistics_GroupByFalse : AbstractIndexCreationTask<Post, Posts_Statistics_GroupByString.Result>
        {
            public Posts_Statistics_GroupByFalse()
            {
                Map = posts => from postComment in posts
                    select new
                    {
                        PostsCount = 1,
                        CommentsCount = postComment.Comments.Length
                    };

                Reduce = results => from result in results
                    group result by 1
                    into g
                    select new
                    {
                        PostsCount = g.Sum(x => x.PostsCount),
                        CommentsCount = g.Sum(x => x.CommentsCount),
                    };
            }
        }

        public class Posts_Statistics_GroupByNull : AbstractIndexCreationTask<Post, Posts_Statistics_GroupByString.Result>
        {
            public Posts_Statistics_GroupByNull()
            {
                Map = posts => from postComment in posts
                    select new
                    {
                        PostsCount = 1,
                        CommentsCount = postComment.Comments.Length
                    };

                Reduce = results => from result in results
                    group result by (string)null
                    into g
                    select new
                    {
                        PostsCount = g.Sum(x => x.PostsCount),
                        CommentsCount = g.Sum(x => x.CommentsCount),
                    };
            }
        }

        public class Posts_Statistics_QuerySyntax_GroupByString : AbstractIndexCreationTask
        {
            public override IndexDefinition CreateIndexDefinition()
            {
                return new IndexDefinition
                {
                    Name = "Posts_Statistics_QuerySyntax",
                    Maps =
                    {
                        @"from postComment in docs.Posts
                    select new
                    {
                        PostsCount = 1,
                        CommentsCount = postComment.Comments.Length
                    }"
                    },
                    Reduce = @"from result in results
                    group result by ""constant""
                    into g
                    select new
                    {
                        PostsCount = g.Sum(x => x.PostsCount),
                        CommentsCount = g.Sum(x => x.CommentsCount),
                    };"
                };
            }
        }

        public class Posts_Statistics_QuerySyntax_GroupByNumber : AbstractIndexCreationTask
        {
            public override IndexDefinition CreateIndexDefinition()
            {
                return new IndexDefinition
                {
                    Name = "Posts_Statistics_QuerySyntax",
                    Maps =
                    {
                        @"from postComment in docs.Posts
                    select new
                    {
                        PostsCount = 1,
                        CommentsCount = postComment.Comments.Length
                    }"
                    },
                    Reduce = @"from result in results
                    group result by 1
                    into g
                    select new
                    {
                        PostsCount = g.Sum(x => x.PostsCount),
                        CommentsCount = g.Sum(x => x.CommentsCount),
                    };"
                };
            }
        }

        public class Posts_Statistics_QuerySyntax_GroupByTrue : AbstractIndexCreationTask
        {
            public override IndexDefinition CreateIndexDefinition()
            {
                return new IndexDefinition
                {
                    Name = "Posts_Statistics_QuerySyntax",
                    Maps =
                    {
                        @"from postComment in docs.Posts
                    select new
                    {
                        PostsCount = 1,
                        CommentsCount = postComment.Comments.Length
                    }"
                    },
                    Reduce = @"from result in results
                    group result by true
                    into g
                    select new
                    {
                        PostsCount = g.Sum(x => x.PostsCount),
                        CommentsCount = g.Sum(x => x.CommentsCount),
                    };"
                };
            }
        }

        public class Posts_Statistics_QuerySyntax_GroupByNull : AbstractIndexCreationTask
        {
            public override IndexDefinition CreateIndexDefinition()
            {
                return new IndexDefinition
                {
                    Name = "Posts_Statistics_QuerySyntax",
                    Maps =
                    {
                        @"from postComment in docs.Posts
                    select new
                    {
                        PostsCount = 1,
                        CommentsCount = postComment.Comments.Length
                    }"
                    },
                    Reduce = @"from result in results
                    group result by ((object)null)
                    into g
                    select new
                    {
                        PostsCount = g.Sum(x => x.PostsCount),
                        CommentsCount = g.Sum(x => x.CommentsCount),
                    };"
                };
            }
        }
    }
}
