using System.Collections.Generic;
using FastTests;

using Xunit;
using System.Linq;
using System.Threading;
using Raven.NewClient.Client.Indexes;
using Raven.NewClient.Client.Indexing;
using Raven.NewClient.Operations.Databases.Indexes;

namespace SlowTests.Core.Indexing
{
    public class Fanout : RavenNewTestBase
    {
        private class User
        {
            public string Id { get; set; }
            public string Name { get; set; }
            public List<User> Friends { get; set; }
        }

        private class UsersAndFriendsIndex : AbstractIndexCreationTask<User>
        {
            public override string IndexName => "UsersAndFriends";

            public override IndexDefinition CreateIndexDefinition()
            {
                return new IndexDefinition
                {
                    Maps = { @"docs.Users.SelectMany(user => user.Friends, (user, friend) => new { Name = user.Name })" },
                    Configuration =
                    {
                        MaxIndexOutputsPerDocument = 16384
                    }
                };
            }
        }

        [Fact]
        public void ShouldSkipDocumentsIfMaxIndexOutputsPerDocumentIsExceeded()
        {
            var index = new UsersAndFriendsIndex();
            var definition = index.CreateIndexDefinition();
            definition.Configuration.MaxIndexOutputsPerDocument = 2;

            using (var store = GetDocumentStore())
            {
                using (var session = store.OpenSession())
                {
                    var user1 = new User
                    {
                        Name = "user/1",
                        Friends = new List<User>
                        {
                            new User { Name = "friend/1/1" },
                            new User { Name = "friend/1/2" }
                        }
                    };

                    var user2 = new User
                    {
                        Name = "user/2",
                        Friends = new List<User>
                        {
                            new User { Name = "friend/2/1" },
                            new User { Name = "friend/2/2" },
                            new User { Name = "friend/2/3" }
                        }
                    };

                    var user3 = new User
                    {
                        Name = "user/3",
                        Friends = new List<User>
                        {
                            new User { Name = "friend/3/1" }
                        }
                    };

                    session.Store(user1);
                    session.Store(user2);
                    session.Store(user3);

                    session.SaveChanges();
                }

                store.Admin.Send(new PutIndexOperation(index.IndexName, definition));

                WaitForIndexing(store);

                SpinWait.SpinUntil(() => store.Admin.Send(new GetIndexErrorsOperation(new[] { index.IndexName }))[0].Errors.Length > 0, 1000);

                var errors = store.Admin.Send(new GetIndexErrorsOperation(new[] { index.IndexName }))[0];

                Assert.Equal(1, errors.Errors.Length);
                Assert.Contains("Index 'UsersAndFriends' has already produced 3 map results for a source document 'users/2'", errors.Errors[0].Error);

                using (var session = store.OpenSession())
                {
                    var results = session
                        .Query<User, UsersAndFriendsIndex>()
                        .ToList();

                    Assert.Equal(2, results.Count);
                    Assert.True(results.Any(x => x.Name == "user/1"));
                    Assert.True(results.Any(x => x.Name == "user/3"));
                }
            }
        }
    }
}