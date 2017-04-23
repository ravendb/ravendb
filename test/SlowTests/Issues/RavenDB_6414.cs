using System;
using System.Linq;
using System.Runtime.ExceptionServices;
using System.Threading;
using System.Threading.Tasks;
using FastTests;
using Raven.Server.NotificationCenter.Notifications;
using Raven.Server.ServerWide.Context;
using Raven.Tests.Core.Utils.Entities;
using Sparrow.Collections;
using Sparrow.Json;
using Sparrow.Json.Parsing;
using Xunit;

namespace SlowTests.Issues
{
    public class RavenDB_6414 : RavenTestBase
    {
        [Fact]
        public async Task Should_unload_db_and_send_notification_on_catastrophic_failure()
        {
            UseNewLocalServer();
            using (var store = GetDocumentStore())
            {
                var notifications = new AsyncQueue<Notification>();

                using (Server.ServerStore.NotificationCenter.TrackActions(notifications, null))
                {
                    var database = await GetDatabase(store.DefaultDatabase);

                    Assert.Equal(1, Server.ServerStore.DatabasesLandlord.DatabasesCache.Count());

                    try
                    {
                        throw new Exception("Catastrophy");
                    }
                    catch (Exception e)
                    {
                        database.GetAllStoragesEnvironment().First().Environment.Options.SetCatastrophicFailure(ExceptionDispatchInfo.Capture(e));
                    }

                    var ex = Assert.Throws<Exception>(() =>
                    {
                        using (var context = DocumentsOperationContext.ShortTermSingleUse(database))
                        {
                            using (var tx = context.OpenWriteTransaction())
                            {
                                var dynamicJsonValue = new DynamicJsonValue();
                                using (var doc = context.ReadObject(dynamicJsonValue, "users/1", BlittableJsonDocumentBuilder.UsageMode.ToDisk))
                                {
                                    database.DocumentsStorage.Put(context, "users/1", null, doc);
                                }

                                tx.Commit();
                            }
                        }
                    });

                    Assert.Equal("Catastrophy", ex.Message);

                    // db unloaded
                    Assert.True(SpinWait.SpinUntil(() => Server.ServerStore.DatabasesLandlord.DatabasesCache.Any() == false, TimeSpan.FromMinutes(1)));

                    var alert = await notifications.TryDequeueOfTypeAsync<AlertRaised>(TimeSpan.Zero);

                    Assert.True(alert.Item1);
                    Assert.Equal(AlertType.CatastrophicDatabaseFailure, alert.Item2.AlertType);
                    Assert.Contains(database.Name, alert.Item2.Title);
                }

                using (var session = store.OpenSession())
                {
                    session.Store(new User());

                    session.SaveChanges();
                }

                // db loaded again
                Assert.Equal(1, Server.ServerStore.DatabasesLandlord.DatabasesCache.Count());
            }
            
        }
    }
}