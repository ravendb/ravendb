using System;
using System.Threading.Tasks;
using Raven.Client.Server.ETL;
using Raven.Server.NotificationCenter.Notifications;
using Sparrow.Collections;
using Xunit;

namespace SlowTests.Server.Documents.ETL
{
    public class EtlLoaderTests : EtlTestBase
    {
        [Fact(Skip = "TODO arek - RavenDB-7403")]
        public async Task Raises_alert_if_script_has_invalid_name()
        {
            using (var store = GetDocumentStore())
            {
                var database = await GetDatabase(store.Database);

                var notifications = new AsyncQueue<Notification>();
                using (database.NotificationCenter.TrackActions(notifications, null))
                {
                    AddEtl(store, new RavenEtlConfiguration()
                    {
                        ConnectionStringName = "test",
                        Name = "myEtl",
                        Transforms =
                        {
                            new Transformation()
                            {
                                Collections = {"Users"}
                            }
                        }
                    }, new RavenConnectionString()
                    {
                        Name = "test",
                        Url = "http://127.0.0.1:8080",
                        Database = "Northwind",
                    });

                    var alert = await notifications.TryDequeueOfTypeAsync<AlertRaised>(TimeSpan.FromSeconds(30));

                    Assert.True(alert.Item1);

                    Assert.Equal("Invalid ETL configuration for destination: Northwind@http://127.0.0.1:8080. Reason: Script name cannot be empty.", alert.Item2.Message);
                }
            }
        }

        [Fact(Skip = "TODO arek - RavenDB-7403")]
        public async Task Raises_alert_if_scipts_have_non_unique_names()
        {
            using (var store = GetDocumentStore())
            {
                var database = await GetDatabase(store.Database);

                var notifications = new AsyncQueue<Notification>();
                using (database.NotificationCenter.TrackActions(notifications, null))
                {
                    AddEtl(store, new RavenEtlConfiguration()
                    {
                        ConnectionStringName = "test",
                        Name = "myEtl",
                        Transforms =
                        {
                            new Transformation()
                            {
                                Name = "MyEtl",
                                Collections = { "Users"}
                            },
                            new Transformation()
                            {
                                Name = "MyEtl",
                                Collections = {"People"}
                            }
                        }
                    }, new RavenConnectionString()
                    {
                        Name = "test",
                        Url = "http://127.0.0.1:8080",
                        Database = "Northwind",
                    });

                    var alert = await notifications.TryDequeueOfTypeAsync<AlertRaised>(TimeSpan.FromSeconds(30));

                    Assert.True(alert.Item1);

                    Assert.Equal("Invalid ETL configuration for destination: Northwind@http://127.0.0.1:8080. Reason: Script name 'MyEtl' name is already defined. The script names need to be unique.", alert.Item2.Message);
                }
            }
        }

        [Fact(Skip = "TODO arek - RavenDB-7403")]
        public async Task Raises_alert_if_ETLs_have_the_same_name()
        {
            using (var store = GetDocumentStore())
            {
                var database = await GetDatabase(store.Database);

                var notifications = new AsyncQueue<Notification>();
                using (database.NotificationCenter.TrackActions(notifications, null))
                {

                    AddEtl(store, new RavenEtlConfiguration()
                    {
                        ConnectionStringName = "test",
                        Name = "myEtl",
                        Transforms =
                        {
                            new Transformation()
                            {
                                Collections = {"Users"}
                            }
                        }
                    }, new RavenConnectionString()
                    {
                        Name = "test",
                        Url = "http://127.0.0.1:8080",
                        Database = "Northwind",
                    });


                    AddEtl(store, new RavenEtlConfiguration()
                    {
                        ConnectionStringName = "test",
                        Name = "myEtl",
                        Transforms =
                        {
                            new Transformation()
                            {
                                Name = "TransformOrders",
                                Collections = { "Orders" }
                            }
                        }
                    }, new RavenConnectionString()
                    {
                        Name = "test",
                        Url = "http://127.0.0.1:8080",
                        Database = "Northwind",
                    });

                    var alert = await notifications.TryDequeueOfTypeAsync<AlertRaised>(TimeSpan.FromSeconds(30));

                    Assert.True(alert.Item1);

                    Assert.Equal("Invalid ETL configuration for mydestination: Northwind@http://127.0.0.1:8080. Reason: ETL to this destination is already defined. Please just combine transformation scripts for the same destination.", alert.Item2.Message);
                }
            }
        }
    }
}