// -----------------------------------------------------------------------
//  <copyright file="RavenDB_2812.cs" company="Hibernating Rhinos LTD">
//      Copyright (c) Hibernating Rhinos LTD. All rights reserved.
//  </copyright>
// -----------------------------------------------------------------------

using System;
using System.Collections.Generic;
using System.Linq;
using System.Threading.Tasks;
using FastTests;
using Raven.Client;
using Raven.Client.Indexes;
using Raven.Client.Indexing;
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

        private class UsersAndFriendsIndex : AbstractIndexCreationTask<User>
        {
            public override string IndexName => "UsersAndFriends";

            public override IndexDefinition CreateIndexDefinition()
            {
                return new IndexDefinition
                {
                    Maps = { @"docs.Users.SelectMany(user => user.Friends, (user, friend) => new { Name = user.Name })" },
                    MaxIndexOutputsPerDocument = 16384,
                };
            }
        }

        [Fact]
        public async Task ShouldProperlyPageResults()
        {
            using (var store = await GetDocumentStore())
            {
                new UsersAndFriendsIndex().Execute(store);

                for (var i = 0; i < 50; i++)
                {
                    using (var session = store.OpenSession())
                    {
                        var user = new User
                        {
                            Id = "users/" + i,
                            Name = "user/" + i,
                            Friends = new List<User>(1000)
                        };

                        var friendsCount = new Random().Next(700, 1000);

                        for (var j = 0; j < friendsCount; j++)
                        {
                            user.Friends.Add(new User
                            {
                                Name = "friend/" + i + "/" + j
                            });
                        }

                        session.Store(user);
                        session.SaveChanges();
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
                        var stats = new RavenQueryStatistics();

                        var results = session
                        .Query<User, UsersAndFriendsIndex>()
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
            }
        }
    }
}
