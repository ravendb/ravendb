using System;
using System.Collections.Generic;
using System.Linq;
using System.Threading.Tasks;
using FastTests;
using Raven.Client.Documents.Indexes;
using Raven.Client.Documents.Session;
using Xunit;

namespace SlowTests.SlowTests.Issues
{
    public class RavenDB_2812 : RavenTestBase
    {
        private class User
        {
            public string Id { get; set; }
            public string Name { get; set; }
            public List<User> Friends { get; set; }
        }

        private class UsersAndFiendsIndex : AbstractIndexCreationTask<User>
        {
            public override string IndexName
            {
                get { return "UsersAndFriends"; }
            }

            public override IndexDefinition CreateIndexDefinition()
            {
                return new IndexDefinition
                {
                    Maps = { @"docs.Users.SelectMany(user => user.Friends, (user, friend) => new {Name = user.Name})" }
                };
            }
        }

        [Fact]
        public async Task ShouldProperlyPageResults()
        {
            var store = GetDocumentStore();

            new UsersAndFiendsIndex().Execute(store);

            using (var bulk = store.BulkInsert())
            {
                for (int i = 0; i < 50; i++)
                {
                    var user = new User()
                    {
                        Id = "users/" + i,
                        Name = "user/" + i,
                        Friends = new List<User>(1000)

                    };

                    var friendsCount = new Random().Next(700, 1000);

                    for (int j = 0; j < friendsCount; j++)
                    {
                        user.Friends.Add(new User()
                        {
                            Name = "friend/" + i + "/" + j
                        });
                    }

                    await bulk.StoreAsync(user);
                }
            }
            WaitForIndexing(store);

            int skippedResults = 0;
            var pagedResults = new List<User>();

            var page = 0;
            const int pageSize = 10;

            using (var session = store.OpenSession())
            {
                for (int i = 0; i < 5; i++)
                {
                    var stats = new QueryStatistics();

                    var results = session
                        .Query<User, UsersAndFiendsIndex>()
                        .Statistics(out stats)
                        .Skip((page * pageSize) + skippedResults)
                        .Take(pageSize)
                        .Distinct()
                        .ToList();

                    skippedResults += stats.SkippedResults;

                    page++;

                    pagedResults.AddRange(results);
                }
            }

            Assert.Equal(50, pagedResults.Select(x => x.Id).Distinct().Count());
            store.Dispose();
        }
    }
}
