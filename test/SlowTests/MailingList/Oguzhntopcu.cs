// -----------------------------------------------------------------------
//  <copyright file="Oguzhntopcu.cs" company="Hibernating Rhinos LTD">
//      Copyright (c) Hibernating Rhinos LTD. All rights reserved.
//  </copyright>
// -----------------------------------------------------------------------

using System.Collections.Generic;
using System.Linq;
using FastTests;
using Raven.Client.Documents;
using Raven.Client.Documents.Indexes;
using Raven.Client.Documents.Linq;
using Xunit;

namespace SlowTests.MailingList
{
    public class Oguzhntopcu : RavenTestBase
    {
        private class Result
        {
            public string Id { get; set; }
            public string UserName { get; set; }
            public string Password { get; set; }
            public string SenderId { get; set; }
            public PostStatus[] PostStatus { get; set; }
            public string Title { get; set; }
            public string Body { get; set; }
        }

        private class Transformed
        {
            public string Id { get; set; }
            public string UserName { get; set; }
            public string Password { get; set; }
            public string SenderId { get; set; }
            public PostStatus[] PostStatus { get; set; }
            public string Title { get; set; }
            public string Body { get; set; }
        }

        private class SearchIndex : AbstractMultiMapIndexCreationTask<Result>
        {
            public SearchIndex()
            {
                AddMap<Post>(items => from x in items
                                      select new
                                      {
                                          x.Id,
                                          UserName = (string)null,
                                          Password = (string)null,
                                          x.SenderId,
                                          x.Title,
                                          x.Body,
                                          PostStatus = x.PostStatuses
                                      });

                AddMap<User>(items => from x in items
                                      from post in x.ReducedPosts
                                      select new
                                      {
                                          post.Id,
                                          x.UserName,
                                          x.Password,
                                          SenderId = x.Id,
                                          Title = (string)null,
                                          Body = (string)null,
                                          PostStatus = new[] { PostStatus.None },
                                      });

                Reduce = results => from x in results
                                    group x by x.Id
                                        into g
                                    select new
                                    {
                                        Id = g.Key,
                                        g.FirstOrDefault(i => i.UserName != null).UserName,
                                        g.FirstOrDefault(i => i.Password != null).Password,
                                        g.FirstOrDefault(i => i.SenderId != null).SenderId,
                                        g.FirstOrDefault(i => i.Title != null).Title,
                                        g.FirstOrDefault(i => i.Body != null).Body,
                                        PostStatus = g.SelectMany(i => i.PostStatus).Where(i => i != PostStatus.None).ToArray(),
                                    };

                Index(i => i.UserName, FieldIndexing.Search);
            }
        }

        private class User
        {
            public string Id { get; set; }
            public string UserName { get; set; }
            public string Password { get; set; }

            public ReducedPost[] ReducedPosts { get; set; }
        }

        private class Post
        {
            public string Id { get; set; }
            public string SenderId { get; set; }
            public PostStatus[] PostStatuses { get; set; }

            public string Title { get; set; }

            public string Body { get; set; }
        }

        private class ReducedPost
        {
            public string Id { get; set; }
        }

        private enum PostStatus : byte
        {
            None = 0,
            Ok = 1,
            Edited = 2,
            Suspended = 4,
            Deleted = 8,
        }

        private IEnumerable<object> FakeDatas
        {
            get
            {
                yield return new User
                {
                    Id = "user/1",
                    Password = "pass",
                    UserName = "user",
                    ReducedPosts = new[]
                    {
                        new ReducedPost {Id = "post/1"},
                        new ReducedPost {Id = "post/2"}
                    },
                };

                yield return new Post
                {
                    Id = "post/1",
                    SenderId = "user/1",
                    Body = "body",
                    Title = "title",
                    PostStatuses = new[] { PostStatus.Deleted, PostStatus.Edited }
                };

                yield return new Post
                {
                    Id = "post/2",
                    SenderId = "user/1",
                    Body = "body 2",
                    Title = "title 2",
                    PostStatuses = new[] { PostStatus.Ok }
                };
            }
        }

        [Fact]
        public void CanQueryOnFlagArrays()
        {
            using (var store = GetDocumentStore(new Options
            {
                ModifyDocumentStore = s =>
                {
                    s.Conventions.SaveEnumsAsIntegers = true;
                }
            }))
            {
                store.ExecuteIndex(new SearchIndex());

                PopulateData(store);

                WaitForIndexing(store);

                using (var session = store.OpenSession())
                {
                    var query = session.Query<Result, SearchIndex>()
                        .Customize(i => i.WaitForNonStaleResults())
                        .Where(i => i.PostStatus.Equals(PostStatus.Edited));

                    var data = query.FirstOrDefault();
                    RavenTestHelper.AssertNoIndexErrors(store);
                    Assert.NotNull(data);
                    Assert.NotNull(data.PostStatus);
                    //Assert.Contains(PostStatus.Edited, data.PostStatus);
                }
            }
        }

        private void PopulateData(IDocumentStore store)
        {
            using (var session = store.OpenSession())
            {
                FakeDatas.ToList().ForEach(session.Store);

                session.SaveChanges();
            }
        }
    }
}
