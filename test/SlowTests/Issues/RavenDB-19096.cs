using System;
using System.Linq;
using System.Threading.Tasks;
using FastTests;
using Raven.Client.Documents.Conventions;
using Raven.Client.Documents.Indexes;
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
            var db = await GetDatabase(store.Database);

            var notificationsQueue = new AsyncQueue<DynamicJsonValue>();

            using (db.NotificationCenter.TrackActions(notificationsQueue, null))
            {
                var index1 = new DummyIndex();
                var index2 = new OtherIndex();
                
                await index1.ExecuteAsync(store);
                await index2.ExecuteAsync(store);
                
                await Indexes.WaitForIndexingAsync(store);

                db.NotificationCenter.Indexing.AddIndexNameToCpuCreditsExhaustionWarning(index1.IndexName);
                db.NotificationCenter.Indexing.AddIndexNameToCpuCreditsExhaustionWarning(index2.IndexName);
                
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
                    Assert.Contains(index1.IndexName, detailsObject.IndexNames);
                    Assert.Contains(index2.IndexName, detailsObject.IndexNames);
                }
                
                db.NotificationCenter.Indexing.RemoveIndexNameFromCpuCreditsExhaustionWarning(index1.IndexName);
                db.NotificationCenter.Indexing.RemoveIndexNameFromCpuCreditsExhaustionWarning(index2.IndexName);
                
                db.NotificationCenter.Indexing.UpdateIndexing(null);
                
                do
                {
                    notification = await notificationsQueue.TryDequeueAsync(TimeSpan.FromSeconds(5));
                } while (notification.Item1 && notification.Item2["Type"].ToString() != NotificationType.NotificationUpdated.ToString());

                Assert.Equal("NotificationUpdated/Dismissed/AlertRaised/Throttling_CpuCreditsBalance/Indexing", notification.Item2["Id"]);
            }
        }
    }

    private class Dto
    {
        public string Name { get; set; }
    }

    private class DummyIndex : AbstractIndexCreationTask<Dto>
    {
        public DummyIndex()
        {
            Map = dtos => from dto in dtos
                select new
                {
                    Name = dto.Name
                };
        }
    }

    private class OtherIndex : AbstractIndexCreationTask<Dto>
    {
        public OtherIndex()
        {
            Map = dtos => from dto in dtos
                select new
                {
                    Name = dto.Name + "ddd"
                };
        }
    }
}
