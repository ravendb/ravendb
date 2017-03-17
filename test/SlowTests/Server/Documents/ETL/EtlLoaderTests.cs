using System;
using System.Threading.Tasks;
using FastTests;
using Raven.Server.Documents.ETL;
using Raven.Server.Documents.ETL.Providers.Raven;
using Raven.Server.NotificationCenter.Notifications;
using Sparrow.Collections;
using Xunit;

namespace SlowTests.Server.Documents.ETL
{
    public class EtlLoaderTests : RavenTestBase
    {
        [Fact]
        public async Task Raises_alert_if_process_has_invalid_name()
        {
            using (var store = GetDocumentStore())
            {
                var database = await GetDatabase(store.DefaultDatabase);

                var notifications = new AsyncQueue<Notification>();
                using (database.NotificationCenter.TrackActions(notifications, null))
                {
                    using (var session = store.OpenSession())
                    {
                        session.Store(new EtlConfiguration
                        {
                            RavenTargets =
                            {
                                new RavenEtlConfiguration
                                {
                                    Url = "http://127.0.0.1:8080",
                                    Database = "Northwind",
                                    Collection = "Users"
                                }
                            }
                        }, "Raven/ETL");

                        session.SaveChanges();
                    }
                    
                    var alert = await notifications.TryDequeueOfTypeAsync<AlertRaised>(TimeSpan.FromSeconds(30));

                    Assert.True(alert.Item1);

                    Assert.Equal("Invalid ETL configuration for: ''. Reason: Name cannot be empty.", alert.Item2.Message);
                }
            }
        }

        [Fact]
        public async Task Raises_alert_if_processes_have_non_unique_names()
        {
            using (var store = GetDocumentStore())
            {
                var database = await GetDatabase(store.DefaultDatabase);

                var notifications = new AsyncQueue<Notification>();
                using (database.NotificationCenter.TrackActions(notifications, null))
                {
                    using (var session = store.OpenSession())
                    {
                        session.Store(new EtlConfiguration
                        {
                            RavenTargets =
                            {
                                new RavenEtlConfiguration
                                {
                                    Url = "http://127.0.0.1:8080",
                                    Database = "Northwind",
                                    Name = "MyEtl",
                                    Collection = "Users"
                                },
                                new RavenEtlConfiguration
                                {
                                    Url = "http://127.0.0.1:8080",
                                    Database = "Northwind",
                                    Name = "MyEtl",
                                    Collection = "People"
                                }
                            }
                        }, "Raven/ETL");

                        session.SaveChanges();
                    }

                    var alert = await notifications.TryDequeueOfTypeAsync<AlertRaised>(TimeSpan.FromSeconds(30));

                    Assert.True(alert.Item1);

                    Assert.Equal("Invalid ETL configuration for: 'MyEtl'. Reason: 'MyEtl' name is already defined for different ETL process.", alert.Item2.Message);
                }
            }
        }
    }
}