﻿using System.Linq;
using FastTests;
using Raven.Client.Documents.Indexes;
using Raven.Tests.Core.Utils.Entities;
using Xunit;

namespace SlowTests.Issues
{
    public class RavenDB_8240 : RavenTestBase
    {
        [Fact]
        public void Can_group_by_constant_in_static_index()
        {
            using (var store = GetDocumentStore())
            {
                store.ExecuteIndex(new Posts_Statistics());

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

                    var results = session.Query<Posts_Statistics.Result, Posts_Statistics>().Customize(x => x.WaitForNonStaleResults()).ToList();

                    Assert.Equal(1, results.Count);
                    Assert.Equal(3, results[0].PostsCount);
                    Assert.Equal(12, results[0].CommentsCount);
                }
            }
        }

        public class Posts_Statistics : AbstractIndexCreationTask<Post, Posts_Statistics.Result>
        {
            public class Result
            {
                public int? PostsCount { get; set; }
                public int CommentsCount { get; set; }
            }

            public Posts_Statistics()
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
    }
}
