using System;
using System.Collections.Generic;
using System.Threading.Tasks;
using FastTests;
using Raven.Client.Documents.Indexes;
using Raven.Client.Documents.Operations.Indexes;
using Raven.Server.NotificationCenter.Notifications;
using Raven.Server.NotificationCenter.Notifications.Details;
using Sparrow.Json.Parsing;
using Sparrow.Server.Collections;
using Xunit;

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

                var notificationsQueue = new AsyncQueue<DynamicJsonValue>();
                using (db.NotificationCenter.TrackActions(notificationsQueue, null))
                {
                    definition.Name = index.IndexName;
                    store.Maintenance.Send(new PutIndexesOperation(new[] { definition }));

                    WaitForIndexing(store);

                    // we might have other notifications like StatsChanged
                    Assert.True(notificationsQueue.Count > 0);

                    Tuple<bool, DynamicJsonValue> performanceHint;

                    do
                    {
                        performanceHint = await notificationsQueue.TryDequeueAsync(TimeSpan.Zero);
                    } while (performanceHint.Item2["Type"].ToString() != NotificationType.PerformanceHint.ToString());

                    Assert.NotNull(performanceHint.Item2);
                    Assert.Equal("Index 'UsersAndFriends' has produced more than 2 map results from a single document", performanceHint.Item2[nameof(PerformanceHint.Message)]);
                    Assert.Equal("UsersAndFriends", performanceHint.Item2[nameof(PerformanceHint.Source)]);
                    Assert.Equal(PerformanceHintType.Indexing, performanceHint.Item2[nameof(PerformanceHint.HintType)]);

                    var details = performanceHint.Item2[nameof(PerformanceHint.Details)] as DynamicJsonValue;

                    Assert.NotNull(details);
                    Assert.Equal(1L, details[nameof(WarnIndexOutputsPerDocument.NumberOfExceedingDocuments)]);
                    Assert.Equal(3, details[nameof(WarnIndexOutputsPerDocument.MaxNumberOutputsPerDocument)]);
                    Assert.Equal("users/2-A", details[nameof(WarnIndexOutputsPerDocument.SampleDocumentId)]);
                }

                var indexStats = store.Maintenance.Send(new GetIndexStatisticsOperation(index.IndexName));

                Assert.Equal(3, indexStats.MaxNumberOfOutputsPerDocument);
            }
        }
    }
}
