using System;
using System.Threading.Tasks;
using FastTests;
using FastTests.Server.NotificationCenter;
using Raven.Client;
using Raven.Server.NotificationCenter.Notifications;
using Raven.Server.ServerWide.Context;
using Sparrow.Collections;
using Sparrow.Json.Parsing;
using Xunit;

namespace SlowTests.Server.NotificationCenter
{
    public class NotificationCenterSlowTests : NotificationCenterTests
    {
        [Fact]
        public async Task Should_be_notified_about_changed_database_stats()
        {
            using (var database = CreateDocumentDatabase())
            {
                var actions = new AsyncQueue<Notification>();
                var writer = new TestWebSockerWriter();
                
                using (database.NotificationCenter.TrackActions(actions, writer))
                {
                    var notification = await actions.TryDequeueAsync(TimeSpan.FromSeconds(10));
                    Assert.True(notification.Item1);

                    var databaseStatsChanged = notification.Item2 as DatabaseStatsChanged;

                    Assert.NotNull(databaseStatsChanged); // initial notification

                    Assert.Equal(0, databaseStatsChanged.CountOfDocuments);
                    Assert.Equal(0, databaseStatsChanged.CountOfIndexes);
                    Assert.Equal(0, databaseStatsChanged.CountOfStaleIndexes);
                    Assert.Equal(0, databaseStatsChanged.ModifiedCollections.Count);
                    
                    DocumentsOperationContext context;
                    using (database.DocumentsStorage.ContextPool.AllocateOperationContext(out context))
                    using (var doc = context.ReadObject(new DynamicJsonValue
                    {
                        ["Foo"] = "Bar",
                        [Constants.Documents.Metadata.Key] = new DynamicJsonValue
                        {
                            [Constants.Documents.Metadata.Collection] = "Foos"
                        }
                    }, ""))
                    {
                        using (var tx = context.OpenWriteTransaction())
                        {
                            database.DocumentsStorage.Put(context, "foo/bar", null, doc);
                            tx.Commit();
                        }
                    }

                    notification = await actions.TryDequeueAsync(TimeSpan.FromSeconds(10));
                    Assert.True(notification.Item1);

                    databaseStatsChanged = notification.Item2 as DatabaseStatsChanged;

                    Assert.NotNull(databaseStatsChanged);

                    Assert.Equal(1, databaseStatsChanged.CountOfDocuments);
                    Assert.Equal(0, databaseStatsChanged.CountOfIndexes);
                    Assert.Equal(0, databaseStatsChanged.CountOfStaleIndexes);
                    Assert.Equal(1, databaseStatsChanged.ModifiedCollections.Count);
                    Assert.Equal("Foos", databaseStatsChanged.ModifiedCollections[0].Name);
                }
            }
        }
    }
}