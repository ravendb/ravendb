using System.Collections.Generic;
using FastTests;

using Xunit;
using System.Linq;
using System.Threading;
using System.Threading.Tasks;
using Raven.Client.Documents.Indexes;
using Raven.Client.Documents.Operations.Indexes;
using Raven.Server.NotificationCenter.Notifications;
using Raven.Server.NotificationCenter.Notifications.Details;
using Sparrow.Collections;

namespace SlowTests.Issues
{
    public class RavenDB_6199 : RavenTestBase
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
                    Maps = { @"docs.Users.SelectMany(user => user.Friends, (user, friend) => new { Name = user.Name })" }
                };
            }
        }

        [Fact]
        public async Task Should_create_performance_hint_notification_when_exceeding_max_index_outputs()
        {
            var index = new UsersAndFriendsIndex();
            var definition = index.CreateIndexDefinition();

            using (var store = GetDocumentStore())
            {
                var db = await GetDatabase(store.Database);
                db.Configuration.PerformanceHints.MaxWarnIndexOutputsPerDocument = 2;

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

                var notificationsQueue = new AsyncQueue<Notification>();
                using (db.NotificationCenter.TrackActions(notificationsQueue, null))
                {
                    definition.Name = index.IndexName;
                    store.Admin.Send(new PutIndexesOperation(new[] { definition}));

                    WaitForIndexing(store);

                    Assert.Equal(1, notificationsQueue.Count);

                    var performanceHint = await notificationsQueue.DequeueAsync() as PerformanceHint;

                    Assert.NotNull(performanceHint);
                    Assert.Equal("Index 'UsersAndFriends' has produced more than 2 map results from a single document", performanceHint.Message);
                    Assert.Equal("UsersAndFriends", performanceHint.Source);
                    Assert.Equal(PerformanceHintType.Indexing, performanceHint.HintType);

                    var details = performanceHint.Details as WarnIndexOutputsPerDocument;

                    Assert.NotNull(details);
                    Assert.Equal(1, details.NumberOfExceedingDocuments);
                    Assert.Equal(3, details.MaxNumberOutputsPerDocument);
                    Assert.Equal("users/2", details.SampleDocumentId);
                }

                var indexStats = store.Admin.Send(new GetIndexStatisticsOperation(index.IndexName));

                Assert.Equal(3, indexStats.MaxNumberOfOutputsPerDocument);
            }
        }
    }
}