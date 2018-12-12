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
            UseNewLocalServer();

            using (var store = GetDocumentStore(new Options()
            {
                Path = NewDataPath()
            }))
            {
                var database = await GetDatabase(store.Database);

                var serverStore = database.ServerStore;

                serverStore.StorageSpaceMonitor.SimulateLowDiskSpace = true;

                var notifications = new AsyncQueue<DynamicJsonValue>();
                using (serverStore.NotificationCenter.TrackActions(notifications, null))
                {
                    serverStore.StorageSpaceMonitor.Run(null);

                    var notification = await notifications.TryDequeueAsync(TimeSpan.FromSeconds(30));

                    Assert.True(notification.Item1);

                    Assert.Equal(AlertType.LowDiskSpace, notification.Item2[nameof(AlertRaised.AlertType)]);
                }
            }
        }
    }
}
