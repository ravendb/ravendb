using System;
using System.Linq;
using System.Runtime.ExceptionServices;
using System.Threading;
using System.Threading.Tasks;
using FastTests;
using Raven.Server.NotificationCenter.Notifications;
using Raven.Server.ServerWide.Context;
using Raven.Tests.Core.Utils.Entities;
using Sparrow.Json;
using Sparrow.Json.Parsing;
using Sparrow.Server.Collections;
using Xunit;
using Xunit.Abstractions;

namespace SlowTests.Issues
{
    public class RavenDB_6414 : RavenTestBase
    {
        public RavenDB_6414(ITestOutputHelper output) : base(output)
        {
        }

        [Fact]
        public async Task Should_unload_db_and_send_notification_on_catastrophic_failure()
        {
            UseNewLocalServer();
            using (var store = GetDocumentStore())
            {
                var notifications = new AsyncQueue<DynamicJsonValue>();

                using (Server.ServerStore.NotificationCenter.TrackActions(notifications, null))
                {
                    var database = await GetDatabase(store.Database);

                    Assert.Equal(1, Server.ServerStore.DatabasesLandlord.DatabasesCache.Count());

                    try
                    {
                        throw new Exception("Catastrophy");
                    }
                    catch (Exception e)
                    {
                        database.GetAllStoragesEnvironment().First().Environment.Options.SetCatastrophicFailure(ExceptionDispatchInfo.Capture(e));
                    }

                    var ex = Assert.ThrowsAny<Exception>(() =>
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

                    Tuple<bool, DynamicJsonValue> alert;
                    do
                    {
                        alert = await notifications.TryDequeueAsync(TimeSpan.Zero);
                    } while (alert.Item2["Type"].ToString() != NotificationType.AlertRaised.ToString());
                 
                    
                    Assert.Equal(AlertType.CatastrophicDatabaseFailure, alert.Item2[nameof(AlertRaised.AlertType)]);
                    Assert.Contains(database.Name, alert.Item2[nameof(AlertRaised.Title)] as string);
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
