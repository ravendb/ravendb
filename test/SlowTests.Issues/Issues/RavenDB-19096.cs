using System;
using System.Threading.Tasks;
using FastTests;
using Raven.Client.Documents.Conventions;
using Raven.Server.NotificationCenter.Notifications;
using Raven.Server.NotificationCenter.Notifications.Details;
using Sparrow.Json;
using Sparrow.Json.Parsing;
using Sparrow.Server.Collections;
using Tests.Infrastructure;
using Xunit;
using Xunit.Abstractions;

namespace SlowTests.Issues;

public class RavenDB_19096 : RavenTestBase
{
    public RavenDB_19096(ITestOutputHelper output) : base(output)
    {
    }

    [RavenFact(RavenTestCategory.None)]
    public async Task AssertIndexNamesAreAddedToCpuCreditsExhaustionNotification()
    {
        DoNotReuseServer();
        
        using (var store = GetDocumentStore())
        {
            const string indexName1 = "CoolIndex";
            const string indexName2 = "CoolerIndex";
            
            var db = await GetDatabase(store.Database);

            var notificationsQueue = new AsyncQueue<DynamicJsonValue>();

            using (db.NotificationCenter.TrackActions(notificationsQueue, null))
            {
                db.NotificationCenter.Indexing.AddIndexNameToCpuCreditsExhaustionWarning(indexName1);
                db.NotificationCenter.Indexing.AddIndexNameToCpuCreditsExhaustionWarning(indexName2);
                
                db.NotificationCenter.Indexing.UpdateIndexing(null);
                
                Tuple<bool, DynamicJsonValue> notification;

                do
                {
                    notification = await notificationsQueue.TryDequeueAsync(TimeSpan.FromSeconds(5));
                } while (notification.Item1 && notification.Item2["Type"].ToString() != NotificationType.AlertRaised.ToString());

                var cpuExhaustionWarningDetails = notification.Item2[nameof(AlertRaised.Details)] as DynamicJsonValue;

                using (var ctx = JsonOperationContext.ShortTermSingleUse())
                {
                    var json = ctx.ReadObject(cpuExhaustionWarningDetails, "details");

                    var detailsObject =
                        DocumentConventions.DefaultForServer.Serialization.DefaultConverter.FromBlittable<CpuCreditsExhaustionWarning>(json, "cpu_exhaustion_warning_details");

                    Assert.Equal(2, detailsObject.IndexNames.Count);
                    Assert.Contains(indexName1, detailsObject.IndexNames);
                    Assert.Contains(indexName2, detailsObject.IndexNames);
                }
                
                db.NotificationCenter.Indexing.RemoveIndexNameFromCpuCreditsExhaustionWarning(indexName1);
                db.NotificationCenter.Indexing.RemoveIndexNameFromCpuCreditsExhaustionWarning(indexName2);
                
                db.NotificationCenter.Indexing.UpdateIndexing(null);
                
                do
                {
                    notification = await notificationsQueue.TryDequeueAsync(TimeSpan.FromSeconds(5));
                } while (notification.Item1 && notification.Item2["Type"].ToString() != NotificationType.NotificationUpdated.ToString());

                Assert.Equal("NotificationUpdated/Dismissed/AlertRaised/Throttling_CpuCreditsBalance/Indexing", notification.Item2["Id"]);
            }
        }
    }
}
