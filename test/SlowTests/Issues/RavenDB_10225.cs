using System;
using System.Threading.Tasks;
using FastTests;
using Raven.Server.NotificationCenter.Notifications;
using Sparrow.Collections;
using Sparrow.Json.Parsing;
using Xunit;

namespace SlowTests.Issues
{
    public class RavenDB_10225 : RavenTestBase
    {
        [Fact]
        public async Task ShouldCreateLowDiskSpaceAlert()
        {
            using (var store = GetDocumentStore(new Options()
            {
                Path = NewDataPath()
            }))
            {
                var database = await GetDatabase(store.Database);

                database.StorageSpaceMonitor.SimulateLowDiskSpace = true;

                var notifications = new AsyncQueue<DynamicJsonValue>();
                using (database.NotificationCenter.TrackActions(notifications, null))
                {
                    database.StorageSpaceMonitor.Run(null);

                    var notification = await notifications.TryDequeueAsync(TimeSpan.FromSeconds(30));

                    Assert.True(notification.Item1);

                    Assert.Equal(AlertType.LowDiskSpace, notification.Item2[nameof(AlertRaised.AlertType)]);
                }
            }
        }
    }
}
