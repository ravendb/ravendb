using System;
using System.Threading.Tasks;
using Raven.Client.Documents.Operations.ETL;
using Raven.Client.Exceptions;
using Raven.Client.Exceptions.Cluster;
using Sparrow.Collections;
using Sparrow.Json.Parsing;
using Xunit;

namespace SlowTests.Server.Documents.ETL
{
    public class EtlLoaderTests : EtlTestBase
    {
        [Fact]
        public async Task Raises_alert_if_script_has_invalid_name()
        {
            using (var store = GetDocumentStore())
            {
                var database = await GetDatabase(store.Database);

                var notifications = new AsyncQueue<DynamicJsonValue>();
                using (database.NotificationCenter.TrackActions(notifications, null))
                {
                    var e = Assert.ThrowsAny<Exception>(() => AddEtl(store, new RavenEtlConfiguration
                    {
                        ConnectionStringName = "test",
                        Name = "myEtl",
                        Transforms =
                        {
                            new Transformation
                            {
                                Collections =
                                {
                                    "Users"
                                }
                            }
                        }
                    }, new RavenConnectionString
                    {
                        Name = "test",
                        TopologyDiscoveryUrls = new[] { "http://127.0.0.1:8080" },
                        Database = "Northwind",
                    }));

                    Assert.Contains("Script name cannot be empty", e.Message);
                }
            }
        }

        [Fact]
        public void Raises_alert_if_scipts_have_non_unique_names()
        {
            using (var store = GetDocumentStore())
            {
                var e = Assert.ThrowsAny<Exception>(() => AddEtl(store, new RavenEtlConfiguration
                {
                    ConnectionStringName = "test",
                    Name = "myEtl",
                    Transforms =
                        {
                            new Transformation
                            {
                                Name = "MyEtl",
                                Collections =
                                {
                                    "Users"
                                }
                            },
                            new Transformation
                            {
                                Name = "MyEtl",
                                Collections =
                                {
                                    "People"
                                }
                            }
                        }
                }, new RavenConnectionString
                {
                    Name = "test",
                    TopologyDiscoveryUrls = new[] { "http://127.0.0.1:8080" },
                    Database = "Northwind",
                }));

                Assert.Contains("Script name 'MyEtl' name is already defined. The script names need to be unique", e.Message);
            }
        }

        [Fact]
        public async Task EnsureETLsHaveUniqueNamesAndThrowsIfNotUnique()
        {
            using (var store = GetDocumentStore())
            {
                var database = await GetDatabase(store.Database);

                var notifications = new AsyncQueue<DynamicJsonValue>();
                using (database.NotificationCenter.TrackActions(notifications, null))
                {

                    AddEtl(store, new RavenEtlConfiguration
                    {
                        ConnectionStringName = "test",
                        Name = "myEtl",
                        Transforms =
                        {
                            new Transformation
                            {
                                Name = "TransformUsers",
                                Collections = {"Users"}
                            }
                        }
                    }, new RavenConnectionString
                    {
                        Name = "test",
                        TopologyDiscoveryUrls = new[] { "http://127.0.0.1:8080" },
                        Database = "Northwind",
                    });


                    Assert.Throws<RavenException>(() =>
                    {
                        AddEtl(store, new RavenEtlConfiguration
                        {
                            ConnectionStringName = "test",
                            Name = "myEtl",
                            Transforms =
                            {
                                new Transformation
                                {
                                    Name = "TransformOrders",
                                    Collections =
                                    {
                                        "Orders"
                                    }
                                }
                            }
                        }, new RavenConnectionString
                        {
                            Name = "test",
                            TopologyDiscoveryUrls = new[] { "http://127.0.0.1:8080" },
                            Database = "Northwind",
                        });
                    });
                }
            }
        }

        [Fact]
        public async Task Raises_alert_if_connection_string_was_not_found()
        {
            using (var store = GetDocumentStore())
            {
                var database = await GetDatabase(store.Database);

                var notifications = new AsyncQueue<DynamicJsonValue>();
                using (database.NotificationCenter.TrackActions(notifications, null))
                {
                    var operation = new AddEtlOperation<RavenConnectionString>(new RavenEtlConfiguration
                    {
                        ConnectionStringName = "test",
                        Name = "myEtl",
                        Transforms =
                        {
                            new Transformation
                            {
                                Collections = {"Users"}
                            }
                        }
                    });

                    var e = Assert.ThrowsAny<Exception>(() => store.Maintenance.Send(operation));
                    Assert.Contains("Could not find connection string named", e.Message);
                }
            }
        }
    }
}
