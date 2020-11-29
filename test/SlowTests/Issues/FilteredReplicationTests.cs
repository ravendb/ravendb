using System;
using System.Collections.Generic;
using System.IO;
using System.Security.Cryptography.X509Certificates;
using System.Text;
using System.Threading.Tasks;
using FastTests;
using FastTests.Server.Replication;
using Raven.Client.Documents;
using Raven.Client.Documents.Operations.ConnectionStrings;
using Raven.Client.Documents.Operations.ETL;
using Raven.Client.Documents.Operations.OngoingTasks;
using Raven.Client.Documents.Operations.Replication;
using Raven.Client.Documents.Session;
using Raven.Client.Documents.Session.TimeSeries;
using Raven.Client.Documents.Smuggler;
using Raven.Client.Exceptions;
using Raven.Client.ServerWide;
using Raven.Client.ServerWide.Operations.Certificates;
using Raven.Server.Documents;
using Raven.Server.Documents.Replication;
using Raven.Server.ServerWide;
using Raven.Server.ServerWide.Context;
using Sparrow;
using Xunit;
using Xunit.Abstractions;

namespace SlowTests.Issues
{
    public class FilteredReplicationTests : ReplicationTestBase
    {
        public FilteredReplicationTests(ITestOutputHelper output) : base(output)
        {
        }

        [Fact]
        public async Task Seasame_st()
        {
            var certificates = SetupServerAuthentication();
            using var hooper = GetDocumentStore(new Options
            {
                ClientCertificate = certificates.ServerCertificate.Value,
                AdminCertificate = certificates.ServerCertificate.Value
            });
            using var bert = GetDocumentStore(new Options
            {
                ClientCertificate = certificates.ServerCertificate.Value,
                AdminCertificate = certificates.ServerCertificate.Value
            });
            
            using (var s = hooper.OpenAsyncSession())
            {
                await s.StoreAsync(new { Type = "Eggs" }, "menus/breakfast");
                await s.StoreAsync(new { Name = "Bird Seed Milkshake" }, "recipes/bird-seed-milkshake");
                await s.StoreAsync(new { Name = "3 USD" }, "prices/eastus/2");
                await s.StoreAsync(new { Name = "3 EUR" }, "prices/eu/1");
                await s.SaveChangesAsync();
            }

            using (var s = bert.OpenAsyncSession())
            {
                await s.StoreAsync(new { Name = "Candy" }, "orders/bert/3");
                await s.SaveChangesAsync();
            }

            await hooper.Maintenance.SendAsync(new PutPullReplicationAsHubOperation(new PullReplicationDefinition
            {
                Name = "Franchises",
                Mode = PullReplicationMode.HubToSink | PullReplicationMode.SinkToHub,
                WithFiltering = true,
            }));

            await hooper.Maintenance.SendAsync(new RegisterReplicationHubAccessOperation("Franchises",
                new ReplicationHubAccess
                {
                    Name = "Franchises",
                    CertificateBase64 = Convert.ToBase64String(certificates.ClientCertificate1.Value.Export(X509ContentType.Cert)),
                    AllowedSinkToHubPaths = new[] {"orders/bert/*"},
                    AllowedHubToSinkPaths = new[] {"menus/*", "prices/eastus/*", "recipes/*"}
                }));
            
            
            await bert.Maintenance.SendAsync(new PutConnectionStringOperation<RavenConnectionString>(new RavenConnectionString
            {
                Database = hooper.Database,
                Name = "HopperConStr",
                TopologyDiscoveryUrls = hooper.Urls
            }));
            await bert.Maintenance.SendAsync(new UpdatePullReplicationAsSinkOperation(new PullReplicationAsSink
            {
                ConnectionStringName = "HopperConStr",
                CertificateWithPrivateKey = Convert.ToBase64String(certificates.ClientCertificate1.Value.Export(X509ContentType.Pfx)),
                HubName = "Franchises",
                Mode = PullReplicationMode.HubToSink | PullReplicationMode.SinkToHub
            }));

            Assert.True(WaitForDocument(bert, "menus/breakfast"));
            Assert.True(WaitForDocument(bert, "recipes/bird-seed-milkshake"));
            Assert.True(WaitForDocument(bert, "prices/eastus/2"));
            Assert.True(WaitForDocument(hooper, "orders/bert/3"));

            using (var s = bert.OpenAsyncSession())
            {
                Assert.Null(await s.LoadAsync<object>("prices/eu/1"));
            }

        }

        [Fact]
        public async Task Counters_and_force_revisions()
        {
            using var storeA = GetDocumentStore();
            using var storeB = GetDocumentStore();

            using (var s = storeA.OpenAsyncSession())
            {
                await s.StoreAsync(new { Breed = "German Shepherd" }, "users/ayende/dogs/arava");
                await s.StoreAsync(new { Color = "Gray/White" }, "users/pheobe");
                await s.StoreAsync(new { Name = "Oren" }, "users/ayende");
                s.CountersFor("users/ayende").Increment("test");
                s.CountersFor("users/pheobe").Increment("test");
                s.TimeSeriesFor<HeartRateMeasure>("users/pheobe").Append(DateTime.Today, new HeartRateMeasure
                {
                    HeartRate = 34
                }, "test/things/out");
                s.TimeSeriesFor<HeartRateMeasure>("users/ayende").Append(DateTime.Today, new HeartRateMeasure
                {
                    HeartRate = 55
                }, "test/things/out");
                s.Advanced.Attachments.Store("users/ayende", "test.bin", new MemoryStream(Encoding.UTF8.GetBytes("hello")));
                s.Advanced.Attachments.Store("users/pheobe", "test.bin", new MemoryStream(Encoding.UTF8.GetBytes("hello")));
                s.Advanced.Revisions.ForceRevisionCreationFor("users/ayende", ForceRevisionStrategy.None);
                s.Advanced.Revisions.ForceRevisionCreationFor("users/pheobe", ForceRevisionStrategy.None);
                await s.SaveChangesAsync();
            }

            using (var s = storeA.OpenAsyncSession())
            {
                await s.StoreAsync(new { Color = "Gray/White 2" }, "users/pheobe");
                await s.StoreAsync(new { Name = "Oren 2" }, "users/ayende");

                s.Advanced.Revisions.ForceRevisionCreationFor("users/ayende");
                s.Advanced.Revisions.ForceRevisionCreationFor("users/pheobe");
                await s.SaveChangesAsync();
            }

            await storeA.Maintenance.SendAsync(new PutConnectionStringOperation<RavenConnectionString>(new RavenConnectionString
            {
                Database = storeB.Database,
                Name = storeB.Database + "ConStr",
                TopologyDiscoveryUrls = storeA.Urls
            }));
            await storeA.Maintenance.SendAsync(new UpdateExternalReplicationOperation(new ExternalReplication
            {
                ConnectionStringName = storeB.Database + "ConStr",
                Name = "erpl"
            }));

            Assert.True(WaitForDocument(storeB, "users/ayende"));
            Assert.True(WaitForDocument(storeB, "users/pheobe"));
        }

        [Fact]
        public async Task Can_Setup_Filtered_Replication()
        {
            var certificates = SetupServerAuthentication();
            var dbNameA = GetDatabaseName();
            var adminCert = RegisterClientCertificate(certificates.ServerCertificate.Value, certificates
                .ClientCertificate1.Value, new Dictionary<string, DatabaseAccess>(), SecurityClearance.ClusterAdmin);

            using var storeA = GetDocumentStore(new Options
            {
                AdminCertificate = adminCert,
                ClientCertificate = adminCert,
                ModifyDatabaseName = s => dbNameA
            });

            var pullCert = certificates.ClientCertificate2.Value;
            await storeA.Maintenance.SendAsync(new PutPullReplicationAsHubOperation(new PullReplicationDefinition
            {
                Name = "pull",
                WithFiltering = true
            }));
            await storeA.Maintenance.SendAsync(new RegisterReplicationHubAccessOperation("pull",
                new ReplicationHubAccess
                {
                    Name = "Arava",
                    CertificateBase64 = Convert.ToBase64String(pullCert.Export(X509ContentType.Cert)),
                    AllowedHubToSinkPaths = new[] { "users/ayende", "users/ayende/*" }
                }));
        }

        [Fact]
        public async Task Cannot_setup_partial_filtered_replication()
        {
            var certificates = SetupServerAuthentication();
            var dbNameA = GetDatabaseName();
            var adminCert = RegisterClientCertificate(certificates.ServerCertificate.Value, certificates
                .ClientCertificate1.Value, new Dictionary<string, DatabaseAccess>(), SecurityClearance.ClusterAdmin);

            using var storeA = GetDocumentStore(new Options
            {
                AdminCertificate = adminCert,
                ClientCertificate = adminCert,
                ModifyDatabaseName = s => dbNameA
            });

            var pullCertA = certificates.ClientCertificate2.Value;
            var pullCertB = certificates.ClientCertificate3.Value;

            await storeA.Maintenance.SendAsync(new PutPullReplicationAsHubOperation(new PullReplicationDefinition
            {
                Name = "pull",
#pragma warning disable CS0618 // Type or member is obsolete
                Certificates = new Dictionary<string, string>
#pragma warning restore CS0618 // Type or member is obsolete
                {
                    [pullCertA.Thumbprint] = Convert.ToBase64String(pullCertA.Export(X509ContentType.Cert)),
                    [pullCertB.Thumbprint] = Convert.ToBase64String(pullCertB.Export(X509ContentType.Cert)),
                },
            }));

            await Assert.ThrowsAsync<RavenException>(async () => await storeA.Maintenance.SendAsync(new RegisterReplicationHubAccessOperation("pull",
                new ReplicationHubAccess
                {
                    Name = "Arava",
                    CertificateBase64 = Convert.ToBase64String(pullCertA.Export(X509ContentType.Cert)),
                    AllowedHubToSinkPaths = new[] { "users/ayende", "users/ayende/*" }
                })));
        }

        public class HeartRateMeasure
        {
            [TimeSeriesValue(0)] public double HeartRate;
        }

        [Fact]
        public async Task WhenDeletingHubReplicationWillRemoveAllAccess()
        {
            var certificates = SetupServerAuthentication();
            var dbNameA = GetDatabaseName();
            var adminCert = RegisterClientCertificate(certificates.ServerCertificate.Value, certificates
                .ClientCertificate1.Value, new Dictionary<string, DatabaseAccess>(), SecurityClearance.ClusterAdmin);

            using var storeA = GetDocumentStore(new Options
            {
                AdminCertificate = adminCert,
                ClientCertificate = adminCert,
                ModifyDatabaseName = s => dbNameA
            });
            long[] ids = new long[3];
            var pullCert = new X509Certificate2(File.ReadAllBytes(certificates.ClientCertificate2Path), (string)null,
                X509KeyStorageFlags.Exportable);
            for (int i = 0; i < 3; i++)
            {
                var op = await storeA.Maintenance.SendAsync(new PutPullReplicationAsHubOperation(new PullReplicationDefinition
                {
                    Name = "pull" +i,
                    Mode = PullReplicationMode.SinkToHub | PullReplicationMode.HubToSink,
                    WithFiltering = true
                }));

                ids[i] = op.TaskId;
                
                await storeA.Maintenance.SendAsync(new RegisterReplicationHubAccessOperation("pull" +i, new ReplicationHubAccess
                {
                    Name = "Arava",
                    AllowedHubToSinkPaths = new[]
                    {
                        "users/ayende",
                        "users/ayende/*"
                    },
                    CertificateBase64 = Convert.ToBase64String(pullCert.Export(X509ContentType.Cert)),
                }));
            }

            await storeA.Maintenance.SendAsync(new DeleteOngoingTaskOperation(ids[1], OngoingTaskType.PullReplicationAsHub));
            
            await storeA.Maintenance.SendAsync(new PutPullReplicationAsHubOperation(new PullReplicationDefinition
            {
                Name = "pull1",
                Mode = PullReplicationMode.SinkToHub | PullReplicationMode.HubToSink,
                WithFiltering = true
            }));

            var accesses = await storeA.Maintenance.SendAsync(new GetReplicationHubAccessOperation("pull1"));
            Assert.Empty(accesses);
            
            accesses = await storeA.Maintenance.SendAsync(new GetReplicationHubAccessOperation("pull0"));
            Assert.NotEmpty(accesses);
            accesses = await storeA.Maintenance.SendAsync(new GetReplicationHubAccessOperation("pull2"));
            Assert.NotEmpty(accesses);
        }
        
        [Fact]
        public async Task Can_pull_via_filtered_replication()
        {
            var certificates = SetupServerAuthentication();
            var dbNameA = GetDatabaseName();
            var dbNameB = GetDatabaseName();
            var adminCert = RegisterClientCertificate(certificates.ServerCertificate.Value, certificates
                .ClientCertificate1.Value, new Dictionary<string, DatabaseAccess>(), SecurityClearance.ClusterAdmin);

            using var storeA = GetDocumentStore(new Options
            {
                AdminCertificate = adminCert,
                ClientCertificate = adminCert,
                ModifyDatabaseName = s => dbNameA
            });
            using var storeB = GetDocumentStore(new Options
            {
                AdminCertificate = adminCert,
                ClientCertificate = adminCert,
                ModifyDatabaseName = s => dbNameB
            });

            using (var s = storeA.OpenAsyncSession())
            {
                await s.StoreAsync(new { Breed = "German Shepherd" }, "users/ayende/dogs/arava");
                await s.StoreAsync(new { Color = "Gray/White" }, "users/pheobe");
                await s.StoreAsync(new { Name = "Oren" }, "users/ayende");
                s.CountersFor("users/ayende").Increment("test");
                s.CountersFor("users/pheobe").Increment("test");
                s.TimeSeriesFor<HeartRateMeasure>("users/pheobe").Append(DateTime.Today, new HeartRateMeasure
                {
                    HeartRate = 34
                }, "test/things/out");
                s.TimeSeriesFor<HeartRateMeasure>("users/ayende").Append(DateTime.Today, new HeartRateMeasure
                {
                    HeartRate = 55
                }, "test/things/out");
                s.Advanced.Attachments.Store("users/ayende", "test.bin", new MemoryStream(Encoding.UTF8.GetBytes("hello")));
                s.Advanced.Attachments.Store("users/pheobe", "test.bin", new MemoryStream(Encoding.UTF8.GetBytes("hello")));
                s.Advanced.Revisions.ForceRevisionCreationFor("users/ayende", ForceRevisionStrategy.None);
                s.Advanced.Revisions.ForceRevisionCreationFor("users/pheobe", ForceRevisionStrategy.None);
                await s.SaveChangesAsync();
            }

            using (var s = storeA.OpenAsyncSession())
            {
                await s.LoadAsync<object>("users/pheobe");
                await s.LoadAsync<object>("users/ayende");
            }

            using (var s = storeA.OpenAsyncSession())
            {
                await s.StoreAsync(new { Color = "Gray/White 2" }, "users/pheobe");
                await s.StoreAsync(new { Name = "Oren 2" }, "users/ayende");

                s.Advanced.Revisions.ForceRevisionCreationFor("users/ayende");
                s.Advanced.Revisions.ForceRevisionCreationFor("users/pheobe");
                await s.SaveChangesAsync();
            }

            using (var s = storeA.OpenAsyncSession())
            {
                await s.LoadAsync<object>("users/pheobe");
                await s.LoadAsync<object>("users/ayende");
            }

            var pullCert = new X509Certificate2(File.ReadAllBytes(certificates.ClientCertificate2Path), (string)null,
                X509KeyStorageFlags.Exportable);
            await storeA.Maintenance.SendAsync(new PutPullReplicationAsHubOperation(new PullReplicationDefinition
            {
                Name = "pull",
                Mode = PullReplicationMode.SinkToHub | PullReplicationMode.HubToSink,
                WithFiltering = true
            }));

            await storeA.Maintenance.SendAsync(new RegisterReplicationHubAccessOperation("pull", new ReplicationHubAccess
            {
                Name = "Arava",
                AllowedHubToSinkPaths = new[]
                {
                    "users/ayende",
                    "users/ayende/*"
                },
                CertificateBase64 = Convert.ToBase64String(pullCert.Export(X509ContentType.Cert)),
            }));

            await storeB.Maintenance.SendAsync(new PutConnectionStringOperation<RavenConnectionString>(new RavenConnectionString
            {
                Database = dbNameA,
                Name = dbNameA + "ConStr",
                TopologyDiscoveryUrls = storeA.Urls
            }));
            await storeB.Maintenance.SendAsync(new UpdatePullReplicationAsSinkOperation(new PullReplicationAsSink
            {
                ConnectionStringName = dbNameA + "ConStr",
                CertificateWithPrivateKey = Convert.ToBase64String(pullCert.Export(X509ContentType.Pfx)),
                HubName = "pull"
            }));

            WaitForDocument(storeB, "users/ayende");

            using (var s = storeB.OpenAsyncSession())
            {
                Assert.Null(await s.LoadAsync<object>("users/pheobe"));
                Assert.Null(await s.Advanced.Revisions.GetAsync<object>("users/pheobe", DateTime.Today.AddDays(1)));
                Assert.Null(await s.CountersFor("users/pheobe").GetAsync("test"));
                Assert.Null(await s.TimeSeriesFor<HeartRateMeasure>("users/pheobe").GetAsync());
                Assert.Null(await s.Advanced.Attachments.GetAsync("users/pheobe", "test.bin"));

                WaitForUserToContinueTheTest(storeA);

                Assert.NotNull(await s.LoadAsync<object>("users/ayende/dogs/arava"));
                Assert.NotNull(await s.LoadAsync<object>("users/ayende"));
                Assert.NotNull(await s.Advanced.Revisions.GetAsync<object>("users/ayende", DateTime.Today.AddDays(1)));

                Assert.NotNull(await s.Advanced.Revisions.GetAsync<object>("users/ayende", DateTime.Today.AddDays(1)));
                Assert.NotNull(await s.CountersFor("users/ayende").GetAsync("test"));
                Assert.NotEmpty(await s.TimeSeriesFor<HeartRateMeasure>("users/ayende").GetAsync());
                Assert.NotNull(await s.Advanced.Attachments.GetAsync("users/ayende", "test.bin"));
            }

            using (var s = storeA.OpenAsyncSession())
            {
                s.Delete("users/ayende/dogs/arava");
                await s.SaveChangesAsync();
            }

            WaitForDocumentDeletion(storeB, "users/ayende/dogs/arava");

            using (var s = storeB.OpenAsyncSession())
            {
                Assert.Null(await s.LoadAsync<object>("users/pheobe"));
                Assert.Null(await s.LoadAsync<object>("users/ayende/dogs/arava"));

                Assert.NotNull(await s.LoadAsync<object>("users/ayende"));

                Assert.NotNull(await s.Advanced.Revisions.GetAsync<object>("users/ayende", DateTime.Today.AddDays(1)));
                Assert.NotNull(await s.CountersFor("users/ayende").GetAsync("test"));
                Assert.NotEmpty(await s.TimeSeriesFor<HeartRateMeasure>("users/ayende").GetAsync());
                Assert.NotNull(await s.Advanced.Attachments.GetAsync("users/ayende", "test.bin"));
            }
        }

        [Fact]
        public async Task Can_push_via_filtered_replication()
        {
            var certificates = SetupServerAuthentication();
            var dbNameA = GetDatabaseName();
            var dbNameB = GetDatabaseName();
            var adminCert = RegisterClientCertificate(certificates.ServerCertificate.Value, certificates
                .ClientCertificate1.Value, new Dictionary<string, DatabaseAccess>(), SecurityClearance.ClusterAdmin);

            using var storeA = GetDocumentStore(new Options
            {
                AdminCertificate = adminCert,
                ClientCertificate = adminCert,
                ModifyDatabaseName = s => dbNameA
            });
            using var storeB = GetDocumentStore(new Options
            {
                AdminCertificate = adminCert,
                ClientCertificate = adminCert,
                ModifyDatabaseName = s => dbNameB
            });

            using (var s = storeA.OpenAsyncSession())
            {
                await s.StoreAsync(new { Breed = "German Shepherd" }, "users/ayende/dogs/arava");
                await s.StoreAsync(new { Color = "Gray/White" }, "users/pheobe");
                await s.StoreAsync(new { Name = "Oren" }, "users/ayende");
                s.CountersFor("users/ayende").Increment("test");
                s.CountersFor("users/pheobe").Increment("test");
                s.TimeSeriesFor<HeartRateMeasure>("users/pheobe").Append(DateTime.Today, new HeartRateMeasure
                {
                    HeartRate = 34
                }, "test/things/out");
                s.TimeSeriesFor<HeartRateMeasure>("users/ayende").Append(DateTime.Today, new HeartRateMeasure
                {
                    HeartRate = 55
                }, "test/things/out");
                s.Advanced.Attachments.Store("users/ayende", "test.bin", new MemoryStream(Encoding.UTF8.GetBytes("hello")));
                s.Advanced.Attachments.Store("users/pheobe", "test.bin", new MemoryStream(Encoding.UTF8.GetBytes("hello")));
                s.Advanced.Revisions.ForceRevisionCreationFor("users/ayende", ForceRevisionStrategy.None);
                s.Advanced.Revisions.ForceRevisionCreationFor("users/pheobe", ForceRevisionStrategy.None);
                await s.SaveChangesAsync();
            }

            using (var s = storeA.OpenAsyncSession())
            {
                await s.LoadAsync<object>("users/pheobe");
                await s.LoadAsync<object>("users/ayende");
            }

            using (var s = storeA.OpenAsyncSession())
            {
                await s.StoreAsync(new { Color = "Gray/White 2" }, "users/pheobe");
                await s.StoreAsync(new { Name = "Oren 2" }, "users/ayende");

                s.Advanced.Revisions.ForceRevisionCreationFor("users/ayende");
                s.Advanced.Revisions.ForceRevisionCreationFor("users/pheobe");
                await s.SaveChangesAsync();
            }

            using (var s = storeA.OpenAsyncSession())
            {
                await s.LoadAsync<object>("users/pheobe");
                await s.LoadAsync<object>("users/ayende");
            }

            var pullCert = new X509Certificate2(File.ReadAllBytes(certificates.ClientCertificate2Path), (string)null,
                X509KeyStorageFlags.Exportable);

            await storeB.Maintenance.SendAsync(new PutPullReplicationAsHubOperation(new PullReplicationDefinition
            {
                Name = "push",
                Mode = PullReplicationMode.SinkToHub | PullReplicationMode.HubToSink,
                WithFiltering = true
            }));

            await storeB.Maintenance.SendAsync(new RegisterReplicationHubAccessOperation("push", new ReplicationHubAccess
            {
                Name = "Arava",
                AllowedSinkToHubPaths = new[]
                {
                    "users/ayende",
                    "users/ayende/*"
                },
                CertificateBase64 = Convert.ToBase64String(pullCert.Export(X509ContentType.Cert)),
            }));

            await storeA.Maintenance.SendAsync(new PutConnectionStringOperation<RavenConnectionString>(new RavenConnectionString
            {
                Database = dbNameB,
                Name = dbNameB + "ConStr",
                TopologyDiscoveryUrls = storeA.Urls
            }));
            await storeA.Maintenance.SendAsync(new UpdatePullReplicationAsSinkOperation(new PullReplicationAsSink
            {
                ConnectionStringName = dbNameB + "ConStr",
                Mode = PullReplicationMode.SinkToHub,
                CertificateWithPrivateKey = Convert.ToBase64String(pullCert.Export(X509ContentType.Pfx)),
                HubName = "push"
            }));

            Assert.True(WaitForDocument(storeB, "users/ayende"));
            using (var s = storeB.OpenAsyncSession())
            {
                Assert.Null(await s.LoadAsync<object>("users/pheobe"));
                Assert.Null(await s.Advanced.Revisions.GetAsync<object>("users/pheobe", DateTime.Today.AddDays(1)));
                Assert.Null(await s.CountersFor("users/pheobe").GetAsync("test"));
                Assert.Null(await s.TimeSeriesFor<HeartRateMeasure>("users/pheobe").GetAsync());
                Assert.Null(await s.Advanced.Attachments.GetAsync("users/pheobe", "test.bin"));

                Assert.NotNull(await s.LoadAsync<object>("users/ayende/dogs/arava"));
                Assert.NotNull(await s.LoadAsync<object>("users/ayende"));
                Assert.NotNull(await s.Advanced.Revisions.GetAsync<object>("users/ayende", DateTime.Today.AddDays(1)));

                Assert.NotNull(await s.Advanced.Revisions.GetAsync<object>("users/ayende", DateTime.Today.AddDays(1)));
                Assert.NotNull(await s.CountersFor("users/ayende").GetAsync("test"));
                Assert.NotEmpty(await s.TimeSeriesFor<HeartRateMeasure>("users/ayende").GetAsync());
                Assert.NotNull(await s.Advanced.Attachments.GetAsync("users/ayende", "test.bin"));
            }

            using (var s = storeA.OpenAsyncSession())
            {
                s.Delete("users/ayende/dogs/arava");
                await s.SaveChangesAsync();
            }

            WaitForDocumentDeletion(storeB, "users/ayende/dogs/arava");

            using (var s = storeB.OpenAsyncSession())
            {
                Assert.Null(await s.LoadAsync<object>("users/pheobe"));
                Assert.Null(await s.LoadAsync<object>("users/ayende/dogs/arava"));

                Assert.NotNull(await s.LoadAsync<object>("users/ayende"));

                Assert.NotNull(await s.Advanced.Revisions.GetAsync<object>("users/ayende", DateTime.Today.AddDays(1)));
                Assert.NotNull(await s.CountersFor("users/ayende").GetAsync("test"));
                Assert.NotEmpty(await s.TimeSeriesFor<HeartRateMeasure>("users/ayende").GetAsync());
                Assert.NotNull(await s.Advanced.Attachments.GetAsync("users/ayende", "test.bin"));
            }
        }

        [Fact]
        public async Task Can_pull_and_push_and_filter_at_dest_and_source()
        {
            var certificates = SetupServerAuthentication();
            var dbNameA = GetDatabaseName();
            var dbNameB = GetDatabaseName();
            var adminCert = RegisterClientCertificate(certificates.ServerCertificate.Value, certificates
                .ClientCertificate1.Value, new Dictionary<string, DatabaseAccess>(), SecurityClearance.ClusterAdmin);

            using var storeA = GetDocumentStore(new Options
            {
                AdminCertificate = adminCert,
                ClientCertificate = adminCert,
                ModifyDatabaseName = s => dbNameA
            });
            using var storeB = GetDocumentStore(new Options
            {
                AdminCertificate = adminCert,
                ClientCertificate = adminCert,
                ModifyDatabaseName = s => dbNameB
            });

            using (var s = storeA.OpenAsyncSession())
            {
                await s.StoreAsync(new { Location = "Hadera" }, "users/ayende/office");
                await s.StoreAsync(new { Breed = "German Shepherd" }, "users/ayende/dogs/arava");
                await s.StoreAsync(new { Color = "Gray/White" }, "users/pheobe");
                await s.StoreAsync(new { Name = "Oren" }, "users/ayende");

                await s.SaveChangesAsync();
            }

            using (var s = storeB.OpenAsyncSession())
            {
                await s.StoreAsync(new { Rolling = true }, "users/ayende/chair");
                await s.StoreAsync(new { Color = "Black" }, "users/oscar");
                await s.StoreAsync(new { Secret = "P@$$w0rD" }, "users/ayende/config");

                await s.SaveChangesAsync();
            }

            var pullCert = new X509Certificate2(File.ReadAllBytes(certificates.ClientCertificate2Path), (string)null,
                X509KeyStorageFlags.Exportable);

            await storeA.Maintenance.SendAsync(new PutPullReplicationAsHubOperation(new PullReplicationDefinition
            {
                Name = "both",
                Mode = PullReplicationMode.SinkToHub | PullReplicationMode.HubToSink,
                WithFiltering = true
            }));

            await storeA.Maintenance.SendAsync(new RegisterReplicationHubAccessOperation("both", new ReplicationHubAccess
            {
                Name = "Arava",
                AllowedHubToSinkPaths = new[]
                {
                    "users/ayende",
                    "users/ayende/*"
                },
                AllowedSinkToHubPaths = new[]
                {
                    "users/ayende/config"
                },
                CertificateBase64 = Convert.ToBase64String(pullCert.Export(X509ContentType.Cert)),
            }));

            await storeB.Maintenance.SendAsync(new PutConnectionStringOperation<RavenConnectionString>(new RavenConnectionString
            {
                Database = dbNameA,
                Name = dbNameA + "ConStr",
                TopologyDiscoveryUrls = storeA.Urls
            }));
            await storeB.Maintenance.SendAsync(new UpdatePullReplicationAsSinkOperation(new PullReplicationAsSink
            {
                ConnectionStringName = dbNameA + "ConStr",
                Mode = PullReplicationMode.SinkToHub | PullReplicationMode.HubToSink,
                CertificateWithPrivateKey = Convert.ToBase64String(pullCert.Export(X509ContentType.Pfx)),
                HubName = "both",
                AllowedHubToSinkPaths = new[]
                {
                    "users/ayende",
                    "users/ayende/dogs/*"
                },
                AllowedSinkToHubPaths = new[]
                {
                    "users/ayende/config",
                    "users/ayende/chair"
                }
            }));

            WaitForDocument(storeB, "users/ayende");
            WaitForDocument(storeA, "users/ayende/config");

            WaitForUserToContinueTheTest(storeA);
            using (var s = storeB.OpenAsyncSession())
            {
                Assert.Null(await s.LoadAsync<object>("users/ayende/office"));
                Assert.Null(await s.LoadAsync<object>("users/pheobe"));

                Assert.NotNull(await s.LoadAsync<object>("users/ayende/dogs/arava"));
                Assert.Null(await s.LoadAsync<object>("users/ayende/office"));

                Assert.NotNull(await s.LoadAsync<object>("users/ayende"));
            }

            using (var s = storeA.OpenAsyncSession())
            {
                Assert.Null(await s.LoadAsync<object>("users/ayende/chair"));
                Assert.Null(await s.LoadAsync<object>("users/oscar"));
                Assert.NotNull(await s.LoadAsync<object>("users/ayende/config"));
            }
        }

        public class Propagation
        {
            public bool FromHub;
            public bool FromSink1;
            public bool FromSink2;
            public bool Completed;
            public string Source;
        }

        [Fact]
        public async Task PickupConfigurationChangesOnTheFly()
        {
            var certificates = SetupServerAuthentication();
            var adminCert = RegisterClientCertificate(certificates.ServerCertificate.Value, certificates
                .ClientCertificate1.Value, new Dictionary<string, DatabaseAccess>(), SecurityClearance.ClusterAdmin);

            using var hubStore = GetDocumentStore(new Options
            {
                AdminCertificate = adminCert,
                ClientCertificate = adminCert,
            });
            using var sinkStore1 = GetDocumentStore(new Options
            {
                AdminCertificate = adminCert,
                ClientCertificate = adminCert,
            });

            var pullCert = new X509Certificate2(File.ReadAllBytes(certificates.ClientCertificate2Path), (string)null,
                X509KeyStorageFlags.Exportable);

            var result = await hubStore.Maintenance.SendAsync(new PutPullReplicationAsHubOperation(new PullReplicationDefinition
            {
                Name = "both",
                Mode = PullReplicationMode.HubToSink,
                WithFiltering = true
            }));

            await hubStore.Maintenance.SendAsync(new RegisterReplicationHubAccessOperation("both", new ReplicationHubAccess
            {
                Name = "Arava",
                AllowedSinkToHubPaths = new[]
                {
                    "*",
                },
                AllowedHubToSinkPaths = new[]
                {
                    "*"
                },
                CertificateBase64 = Convert.ToBase64String(pullCert.Export(X509ContentType.Cert)),
            }));

            await sinkStore1.Maintenance.SendAsync(new PutConnectionStringOperation<RavenConnectionString>(new RavenConnectionString
            {
                Database = hubStore.Database, Name = hubStore.Database + "ConStr", TopologyDiscoveryUrls = hubStore.Urls
            }));

            var sinkTask = new PullReplicationAsSink
            {
                ConnectionStringName = hubStore.Database + "ConStr",
                Mode = PullReplicationMode.HubToSink,
                CertificateWithPrivateKey = Convert.ToBase64String(pullCert.Export(X509ContentType.Pfx)),
                HubName = "both",
                AllowedHubToSinkPaths = new[] {"*",},
                AllowedSinkToHubPaths = new[] {"*"}
            };

            var result2 = await sinkStore1.Maintenance.SendAsync(new UpdatePullReplicationAsSinkOperation(sinkTask));

            EnsureReplicating(hubStore, sinkStore1);

            await hubStore.Maintenance.SendAsync(new PutPullReplicationAsHubOperation(new PullReplicationDefinition
            {
                Name = "both",
                Mode = PullReplicationMode.HubToSink | PullReplicationMode.SinkToHub,
                WithFiltering = true,
                TaskId = result.TaskId
            }));
            
            sinkTask.Mode = PullReplicationMode.HubToSink | PullReplicationMode.SinkToHub;
            sinkTask.TaskId = result2.TaskId;

            await sinkStore1.Maintenance.SendAsync(new UpdatePullReplicationAsSinkOperation(sinkTask));

            EnsureReplicating(sinkStore1, hubStore);
        }

        [Fact]
        public async Task Sinks_should_not_update_hubs_change_vector()
        {
            var certificates = SetupServerAuthentication();
            var adminCert = RegisterClientCertificate(certificates.ServerCertificate.Value, certificates
                .ClientCertificate1.Value, new Dictionary<string, DatabaseAccess>(), SecurityClearance.ClusterAdmin);

            using var hubStore = GetDocumentStore(new Options
            {
                AdminCertificate = adminCert,
                ClientCertificate = adminCert,
            });
            using var sinkStore1 = GetDocumentStore(new Options
            {
                AdminCertificate = adminCert,
                ClientCertificate = adminCert,
            });
            using var sinkStore2 = GetDocumentStore(new Options
            {
                AdminCertificate = adminCert,
                ClientCertificate = adminCert,
            });

            var pullCert = new X509Certificate2(File.ReadAllBytes(certificates.ClientCertificate2Path), (string)null,
                X509KeyStorageFlags.Exportable);

            await hubStore.Maintenance.SendAsync(new PutPullReplicationAsHubOperation(new PullReplicationDefinition
            {
                Name = "both",
                Mode = PullReplicationMode.SinkToHub | PullReplicationMode.HubToSink,
                WithFiltering = true
            }));

            await hubStore.Maintenance.SendAsync(new RegisterReplicationHubAccessOperation("both", new ReplicationHubAccess
            {
                Name = "Arava",
                AllowedSinkToHubPaths = new[]
                {
                    "*",
                },
                AllowedHubToSinkPaths = new[]
                {
                    "*"
                },
                CertificateBase64 = Convert.ToBase64String(pullCert.Export(X509ContentType.Cert)),
            }));

            await SetupSink(sinkStore1);
            await SetupSink(sinkStore2);

            EnsureReplicating(hubStore, sinkStore1);
            EnsureReplicating(sinkStore1,hubStore);
            
            EnsureReplicating(hubStore, sinkStore2);
            EnsureReplicating(sinkStore2,hubStore);

            EnsureReplicating(sinkStore1, sinkStore2);
            EnsureReplicating(sinkStore2,sinkStore1);

            using (var s = hubStore.OpenAsyncSession())
            {
                await s.StoreAsync(new Propagation
                {
                    FromHub = true
                }, "common");
                await s.SaveChangesAsync();
            }

            Assert.True(WaitForDocument(sinkStore1, "common"));
            Assert.True(WaitForDocument(sinkStore2, "common"));

            using (var s = sinkStore1.OpenAsyncSession())
            {
                var common = await s.LoadAsync<Propagation>("common");
                common.FromSink1 = true;
                await s.SaveChangesAsync();
            }

            Assert.True(WaitForDocument<Propagation>(hubStore, "common", x => x.FromSink1 == true));
            Assert.True(WaitForDocument<Propagation>(sinkStore2, "common", x => x.FromSink1 == true));

            using (var s = sinkStore2.OpenAsyncSession())
            {
                var common = await s.LoadAsync<Propagation>("common");
                common.FromSink2 = true;
                await s.SaveChangesAsync();
            }

            Assert.True(WaitForDocument<Propagation>(hubStore, "common", x => x.FromSink2 == true));
            Assert.True(WaitForDocument<Propagation>(sinkStore1, "common", x => x.FromSink2 == true));

            WaitForUserToContinueTheTest(hubStore, clientCert: adminCert);

            using (var s = hubStore.OpenAsyncSession())
            {
                var common = await s.LoadAsync<Propagation>("common");
                common.Completed = true;
                await s.SaveChangesAsync();
            }

            Assert.True(WaitForDocument<Propagation>(sinkStore2, "common", x => x.Completed == true));
            Assert.True(WaitForDocument<Propagation>(sinkStore1, "common", x => x.Completed == true));

            var hubDb = await GetDocumentDatabaseInstanceFor(hubStore);
            var sink1Db = await GetDocumentDatabaseInstanceFor(sinkStore1);
            var sink2Db = await GetDocumentDatabaseInstanceFor(sinkStore2);

            using (hubDb.DocumentsStorage.ContextPool.AllocateOperationContext(out DocumentsOperationContext ctx))
            using (ctx.OpenReadTransaction())
            {
                var hubGlobalCv = DocumentsStorage.GetDatabaseChangeVector(ctx);
                Assert.Equal(1, hubGlobalCv.ToChangeVector().Length);
            }

            using (sink1Db.DocumentsStorage.ContextPool.AllocateOperationContext(out DocumentsOperationContext ctx))
            using (ctx.OpenReadTransaction())
            {
                var sink1GlobalCv = DocumentsStorage.GetDatabaseChangeVector(ctx);
                Assert.Equal(3, sink1GlobalCv.ToChangeVector().Length);
            }

            using (sink2Db.DocumentsStorage.ContextPool.AllocateOperationContext(out DocumentsOperationContext ctx))
            using (ctx.OpenReadTransaction())
            {
                var sink2GlobalCv = DocumentsStorage.GetDatabaseChangeVector(ctx);
                Assert.Equal(3, sink2GlobalCv.ToChangeVector().Length);
            }

            async Task SetupSink(DocumentStore sinkStore)
            {
                await sinkStore.Maintenance.SendAsync(new PutConnectionStringOperation<RavenConnectionString>(new RavenConnectionString
                {
                    Database = hubStore.Database, Name = hubStore.Database + "ConStr", TopologyDiscoveryUrls = hubStore.Urls
                }));
                await sinkStore.Maintenance.SendAsync(new UpdatePullReplicationAsSinkOperation(new PullReplicationAsSink
                {
                    ConnectionStringName = hubStore.Database + "ConStr",
                    Mode = PullReplicationMode.SinkToHub | PullReplicationMode.HubToSink,
                    CertificateWithPrivateKey = Convert.ToBase64String(pullCert.Export(X509ContentType.Pfx)),
                    HubName = "both",
                    AllowedHubToSinkPaths = new[] {"*",},
                    AllowedSinkToHubPaths = new[] {"*"}
                }));
            }
        }

        [Fact]
        public async Task Sinks_should_not_update_hubs_change_vector2()
        {
            var certificates = SetupServerAuthentication();
            var adminCert = RegisterClientCertificate(certificates.ServerCertificate.Value, certificates
                .ClientCertificate1.Value, new Dictionary<string, DatabaseAccess>(), SecurityClearance.ClusterAdmin);

            using var hubStore = GetDocumentStore(new Options
            {
                AdminCertificate = adminCert,
                ClientCertificate = adminCert,
            });
            using var sinkStore1 = GetDocumentStore(new Options
            {
                AdminCertificate = adminCert,
                ClientCertificate = adminCert,
            });

            var pullCert = new X509Certificate2(File.ReadAllBytes(certificates.ClientCertificate2Path), (string)null,
                X509KeyStorageFlags.Exportable);

            await hubStore.Maintenance.SendAsync(new PutPullReplicationAsHubOperation(new PullReplicationDefinition
            {
                Name = "both",
                Mode = PullReplicationMode.SinkToHub | PullReplicationMode.HubToSink,
                WithFiltering = true
            }));

            await hubStore.Maintenance.SendAsync(new RegisterReplicationHubAccessOperation("both", new ReplicationHubAccess
            {
                Name = "Arava",
                AllowedSinkToHubPaths = new[]
                {
                    "*",
                },
                AllowedHubToSinkPaths = new[]
                {
                    "*"
                },
                CertificateBase64 = Convert.ToBase64String(pullCert.Export(X509ContentType.Cert)),
            }));

            await SetupSink(sinkStore1);

            EnsureReplicating(hubStore, sinkStore1);
            EnsureReplicating(sinkStore1,hubStore);
            
            using (var s = hubStore.OpenAsyncSession())
            {
                await s.StoreAsync(new Propagation
                {
                    FromHub = true
                }, "common");
                await s.SaveChangesAsync();
            }

            Assert.True(WaitForDocument(sinkStore1, "common"));

            using (var s = sinkStore1.OpenAsyncSession())
            {
                var common = await s.LoadAsync<Propagation>("common");
                common.FromSink1 = true;
                await s.SaveChangesAsync();
            }

            Assert.True(WaitForDocument<Propagation>(hubStore, "common", x => x.FromSink1 == true));

            using (var s = hubStore.OpenAsyncSession())
            {
                var common = await s.LoadAsync<Propagation>("common");
                common.Completed = true;
                await s.SaveChangesAsync();
            }

            Assert.True(WaitForDocument<Propagation>(sinkStore1, "common", x => x.Completed == true));

            using (var s = sinkStore1.OpenAsyncSession())
            {
                s.TimeSeriesFor("common", "test").Append(DateTime.Today, 12);
                await s.SaveChangesAsync();
            }

            using (var s = sinkStore1.OpenAsyncSession())
            {
                s.CountersFor("common").Increment("test");
                await s.SaveChangesAsync();
            }

            using (var s = sinkStore1.OpenAsyncSession())
            {
                await using (var ms = new MemoryStream(new byte[]{1,2,3,4,5}))
                {
                    s.Advanced.Attachments.Store("common", "test", ms);
                    await s.SaveChangesAsync();
                }
            }

            using (var s = sinkStore1.OpenAsyncSession())
            {
                var common = await s.LoadAsync<Propagation>("common");
                common.FromSink2 = true;
                await s.SaveChangesAsync();
            }

            Assert.True(WaitForDocument<Propagation>(hubStore, "common", x => x.FromSink2 == true));


            var hubDb = await GetDocumentDatabaseInstanceFor(hubStore);
            var sink1Db = await GetDocumentDatabaseInstanceFor(sinkStore1);

            using (hubDb.DocumentsStorage.ContextPool.AllocateOperationContext(out DocumentsOperationContext ctx))
            using (ctx.OpenReadTransaction())
            {
                var hubGlobalCv = DocumentsStorage.GetDatabaseChangeVector(ctx);
                Assert.Equal(1, hubGlobalCv.ToChangeVector().Length);
            }

            using (sink1Db.DocumentsStorage.ContextPool.AllocateOperationContext(out DocumentsOperationContext ctx))
            using (ctx.OpenReadTransaction())
            {
                var sink1GlobalCv = DocumentsStorage.GetDatabaseChangeVector(ctx);
                Assert.Equal(2, sink1GlobalCv.ToChangeVector().Length);
            }

            using (var s = hubStore.OpenAsyncSession())
            {
                var common = await s.LoadAsync<Propagation>("common");
                var cv = s.Advanced.GetChangeVectorFor(common);
                var r = await s.Advanced.Revisions.GetForAsync<Propagation>("common");

                Assert.Equal(2, cv.ToChangeVectorList().Count);
                Assert.Contains("SINK", cv);
                Assert.Equal(0, r.Count);
            }

            using (var s = sinkStore1.OpenAsyncSession())
            {
                var common = await s.LoadAsync<Propagation>("common");
                var cv = s.Advanced.GetChangeVectorFor(common);
                var r = await s.Advanced.Revisions.GetForAsync<Propagation>("common");
                Assert.Equal(2, cv.ToChangeVectorList().Count);
                Assert.DoesNotContain("SINK", cv);
                Assert.Equal(0, r.Count);
            }

            async Task SetupSink(DocumentStore sinkStore)
            {
                await sinkStore.Maintenance.SendAsync(new PutConnectionStringOperation<RavenConnectionString>(new RavenConnectionString
                {
                    Database = hubStore.Database, Name = hubStore.Database + "ConStr", TopologyDiscoveryUrls = hubStore.Urls
                }));
                await sinkStore.Maintenance.SendAsync(new UpdatePullReplicationAsSinkOperation(new PullReplicationAsSink
                {
                    ConnectionStringName = hubStore.Database + "ConStr",
                    Mode = PullReplicationMode.SinkToHub | PullReplicationMode.HubToSink,
                    CertificateWithPrivateKey = Convert.ToBase64String(pullCert.Export(X509ContentType.Pfx)),
                    HubName = "both",
                    AllowedHubToSinkPaths = new[] {"*",},
                    AllowedSinkToHubPaths = new[] {"*"}
                }));
            }
        }

        [Fact]
        public async Task Sinks_should_not_update_hubs_change_vector3()
        {
            var certificates = SetupServerAuthentication();
            var adminCert = RegisterClientCertificate(certificates.ServerCertificate.Value, certificates
                .ClientCertificate1.Value, new Dictionary<string, DatabaseAccess>(), SecurityClearance.ClusterAdmin);

            using var hubStore = GetDocumentStore(new Options
            {
                AdminCertificate = adminCert,
                ClientCertificate = adminCert,
            });
            using var sinkStore1 = GetDocumentStore(new Options
            {
                AdminCertificate = adminCert,
                ClientCertificate = adminCert,
            });
            using var sinkStore2 = GetDocumentStore(new Options
            {
                AdminCertificate = adminCert,
                ClientCertificate = adminCert,
            });

            var fooCert = new X509Certificate2(File.ReadAllBytes(certificates.ClientCertificate2Path), (string)null,
                X509KeyStorageFlags.Exportable);

            var barCert = new X509Certificate2(File.ReadAllBytes(certificates.ClientCertificate3Path), (string)null,
                X509KeyStorageFlags.Exportable);

            await hubStore.Maintenance.SendAsync(new PutPullReplicationAsHubOperation(new PullReplicationDefinition
            {
                Name = "both",
                Mode = PullReplicationMode.SinkToHub | PullReplicationMode.HubToSink,
                WithFiltering = true
            }));

            var access1 = new ReplicationHubAccess
            {
                Name = "both",
                AllowedSinkToHubPaths = new[] {"foo"},
                AllowedHubToSinkPaths = new[] {"foo"},
                CertificateBase64 = Convert.ToBase64String(fooCert.Export(X509ContentType.Cert)),
            };

            var access2 = new ReplicationHubAccess
            {
                Name = "both",
                AllowedSinkToHubPaths = new[] {"bar"},
                AllowedHubToSinkPaths = new[] {"bar"},
                CertificateBase64 = Convert.ToBase64String(barCert.Export(X509ContentType.Cert)),
            };

            await hubStore.Maintenance.SendAsync(new RegisterReplicationHubAccessOperation("both", access1));
            await hubStore.Maintenance.SendAsync(new RegisterReplicationHubAccessOperation("both", access2));

            await SetupSink(sinkStore1, access1, fooCert);
            await SetupSink(sinkStore2, access2, barCert);

            using (var s = hubStore.OpenAsyncSession())
            {
                await s.StoreAsync(new Propagation
                {
                    FromHub = true
                }, "foo");
                await s.StoreAsync(new Propagation
                {
                    FromHub = true
                }, "bar");
                await s.SaveChangesAsync();
            }

            using (var s = hubStore.OpenAsyncSession())
            {
                var baseline = DateTime.Today;
                for (int i = 0; i < 150; i++)
                {
                    s.TimeSeriesFor("foo","test").Append(baseline.AddHours(i), 1);
                    s.TimeSeriesFor("bar","test").Append(baseline.AddHours(i), 1);
                }
                await s.SaveChangesAsync();
            }

            Assert.True(WaitForDocument(sinkStore1, "foo"));
            Assert.True(WaitForDocument(sinkStore2, "bar"));

            await sinkStore1.TimeSeries.SetPolicyAsync<Propagation>("By3Hours", TimeValue.FromHours(3), TimeValue.FromDays(3));
            await sinkStore2.TimeSeries.SetPolicyAsync<Propagation>("By3Hours", TimeValue.FromHours(3), TimeValue.FromDays(3));

            var hubDb = await GetDocumentDatabaseInstanceFor(hubStore);
            var sink1Db = await GetDocumentDatabaseInstanceFor(sinkStore1);
            var sink2Db = await GetDocumentDatabaseInstanceFor(sinkStore2);

            await DoRollup(sink1Db);
            await DoRollup(sink2Db);

            using (var s = sinkStore1.OpenAsyncSession())
            {
                var common = await s.LoadAsync<Propagation>("foo");
                common.FromSink1 = true;
                await s.SaveChangesAsync();
            }

            using (var s = sinkStore2.OpenAsyncSession())
            {
                var common = await s.LoadAsync<Propagation>("bar");
                common.FromSink2 = true;
                await s.SaveChangesAsync();
            }

            Assert.True(WaitForDocument<Propagation>(hubStore, "foo", x => x.FromSink1 == true));
            Assert.True(WaitForDocument<Propagation>(hubStore, "bar", x => x.FromSink2 == true));

            using (var s = hubStore.OpenAsyncSession())
            {
                var foo = await s.LoadAsync<Propagation>("foo");
                foo.Completed = true;
                var bar = await s.LoadAsync<Propagation>("bar");
                bar.Completed = true;

                s.Advanced.Revisions.ForceRevisionCreationFor("foo");
                s.Advanced.Revisions.ForceRevisionCreationFor("bar");
                await s.SaveChangesAsync();
            }

            Assert.True(WaitForDocument<Propagation>(sinkStore2, "bar", x => x.Completed == true));
            Assert.True(WaitForDocument<Propagation>(sinkStore1, "foo", x => x.Completed == true));

            using (var token = new OperationCancelToken(hubDb.Configuration.Databases.OperationTimeout.AsTimeSpan, hubDb.DatabaseShutdown))
                await hubDb.DocumentsStorage.RevisionsStorage.EnforceConfiguration(_ => { }, token);

            using (var s = hubStore.OpenAsyncSession())
            {
                var foo = await s.LoadAsync<Propagation>("foo");
                foo.Source = "after-enforce-revision";
                var bar = await s.LoadAsync<Propagation>("bar");
                bar.Source = "after-enforce-revision";
                await s.SaveChangesAsync();
            }

            Assert.True(WaitForDocument<Propagation>(sinkStore2, "bar", x => x.Source == "after-enforce-revision"));
            Assert.True(WaitForDocument<Propagation>(sinkStore1, "foo", x => x.Source == "after-enforce-revision"));

            await VerifyNoRevisions(hubStore, "foo");
            await VerifyNoRevisions(hubStore, "bar");

            await VerifyNoRevisions(sinkStore1, "foo");
            await VerifyNoRevisions(sinkStore2, "bar");

            using (hubDb.DocumentsStorage.ContextPool.AllocateOperationContext(out DocumentsOperationContext ctx))
            using (ctx.OpenReadTransaction())
            {
                var hubGlobalCv = DocumentsStorage.GetDatabaseChangeVector(ctx);
                Assert.Equal(1, hubGlobalCv.ToChangeVector().Length);
            }

            using (sink1Db.DocumentsStorage.ContextPool.AllocateOperationContext(out DocumentsOperationContext ctx))
            using (ctx.OpenReadTransaction())
            {
                var sink1GlobalCv = DocumentsStorage.GetDatabaseChangeVector(ctx);
                Assert.Equal(2, sink1GlobalCv.ToChangeVector().Length);
            }

            using (sink2Db.DocumentsStorage.ContextPool.AllocateOperationContext(out DocumentsOperationContext ctx))
            using (ctx.OpenReadTransaction())
            {
                var sink2GlobalCv = DocumentsStorage.GetDatabaseChangeVector(ctx);
                Assert.Equal(2, sink2GlobalCv.ToChangeVector().Length);
            }

            using (var s = hubStore.OpenAsyncSession())
            {
                await AssertOnHub(s, "foo");
                await AssertOnHub(s, "bar");
            }

            using (var s = sinkStore1.OpenAsyncSession())
            {
                await AssertOnSink(s, "foo");
            }

            using (var s = sinkStore2.OpenAsyncSession())
            {
                await AssertOnSink(s, "bar");
            }

            async Task SetupSink(DocumentStore sinkStore, ReplicationHubAccess access, X509Certificate2 cert)
            {
                await sinkStore.Maintenance.SendAsync(new PutConnectionStringOperation<RavenConnectionString>(new RavenConnectionString
                {
                    Database = hubStore.Database, Name = hubStore.Database + "ConStr", TopologyDiscoveryUrls = hubStore.Urls
                }));
                await sinkStore.Maintenance.SendAsync(new UpdatePullReplicationAsSinkOperation(new PullReplicationAsSink
                {
                    ConnectionStringName = hubStore.Database + "ConStr",
                    Mode = PullReplicationMode.SinkToHub | PullReplicationMode.HubToSink,
                    CertificateWithPrivateKey = Convert.ToBase64String(cert.Export(X509ContentType.Pfx)),
                    HubName = access.Name,
                    AllowedHubToSinkPaths = access.AllowedHubToSinkPaths,
                    AllowedSinkToHubPaths = access.AllowedSinkToHubPaths
                }));
            }

            async Task DoRollup(DocumentDatabase database)
            {
                await database.TimeSeriesPolicyRunner.HandleChanges();
                await database.TimeSeriesPolicyRunner.RunRollups();
                await database.TimeSeriesPolicyRunner.DoRetention();
            }
        }

        private static async Task VerifyNoRevisions(DocumentStore hubStore, string id)
        {
            using (var s = hubStore.OpenAsyncSession())
            {
                var rev = await s.Advanced.Revisions.GetForAsync<Propagation>(id);
                Assert.Equal(0, rev?.Count ?? 0);
            }
        }

        private static async Task AssertOnSink(IAsyncDocumentSession s, string id)
        {
            var doc = await s.LoadAsync<Propagation>(id);
            var cv = s.Advanced.GetChangeVectorFor(doc);
            var r = await s.Advanced.Revisions.GetForAsync<Propagation>(id);
            Assert.Equal(2, cv.ToChangeVectorList().Count);
            Assert.DoesNotContain("SINK", cv);
            Assert.Equal(0, r.Count);
        }

        private static async Task AssertOnHub(IAsyncDocumentSession s, string id)
        {
            var doc = await s.LoadAsync<Propagation>(id);
            var cv = s.Advanced.GetChangeVectorFor(doc);
            var r = await s.Advanced.Revisions.GetForAsync<Propagation>(id);

            Assert.Equal(2, cv.ToChangeVectorList().Count);
            Assert.Contains("SINK", cv);
            Assert.Equal(0, r.Count);
        }

        [Fact]
        public async Task Sinks_should_not_update_hubs_change_vector_with_conflicts()
        {
            var certificates = SetupServerAuthentication();
            var adminCert = RegisterClientCertificate(certificates.ServerCertificate.Value, certificates
                .ClientCertificate1.Value, new Dictionary<string, DatabaseAccess>(), SecurityClearance.ClusterAdmin);

            using var hubStore = GetDocumentStore(new Options
            {
                AdminCertificate = adminCert,
                ClientCertificate = adminCert,
            });
            using var sinkStore1 = GetDocumentStore(new Options
            {
                AdminCertificate = adminCert,
                ClientCertificate = adminCert,
            });
            using var sinkStore2 = GetDocumentStore(new Options
            {
                AdminCertificate = adminCert,
                ClientCertificate = adminCert,
            });

            using (var s = sinkStore1.OpenAsyncSession())
            {
                await s.StoreAsync(new Propagation
                {
                    Source = "Sink1"
                }, "common");
                await s.SaveChangesAsync();
            }

            using (var s = sinkStore2.OpenAsyncSession())
            {
                await s.StoreAsync(new Propagation
                {
                    Source = "Sink2"
                }, "common");
                await s.SaveChangesAsync();
            }

            using (var s = hubStore.OpenAsyncSession())
            {
                await s.StoreAsync(new Propagation
                {
                    Source = "Hub"
                }, "common");
                await s.SaveChangesAsync();
            }

            var pullCert = new X509Certificate2(File.ReadAllBytes(certificates.ClientCertificate2Path), (string)null,
                X509KeyStorageFlags.Exportable);

            await hubStore.Maintenance.SendAsync(new PutPullReplicationAsHubOperation(new PullReplicationDefinition
            {
                Name = "both",
                Mode = PullReplicationMode.SinkToHub | PullReplicationMode.HubToSink,
                WithFiltering = true
            }));

            await hubStore.Maintenance.SendAsync(new RegisterReplicationHubAccessOperation("both", new ReplicationHubAccess
            {
                Name = "Arava",
                AllowedSinkToHubPaths = new[]
                {
                    "*",
                },
                AllowedHubToSinkPaths = new[]
                {
                    "*"
                },
                CertificateBase64 = Convert.ToBase64String(pullCert.Export(X509ContentType.Cert)),
            }));

            await SetupSink(sinkStore1);
            await SetupSink(sinkStore2);

            EnsureReplicating(hubStore, sinkStore1);
            EnsureReplicating(sinkStore1,hubStore);
            
            EnsureReplicating(hubStore, sinkStore2);
            EnsureReplicating(sinkStore2,hubStore);

            EnsureReplicating(sinkStore1, sinkStore2);
            EnsureReplicating(sinkStore2,sinkStore1);
            
            Assert.True(WaitForDocument<Propagation>(hubStore, "common", x => x.Source == "Hub"));
            Assert.True(WaitForDocument<Propagation>(sinkStore1, "common", x => x.Source  == "Hub"));
            Assert.True(WaitForDocument<Propagation>(sinkStore2, "common", x => x.Source  == "Hub"));

            var hubDb = await GetDocumentDatabaseInstanceFor(hubStore);
            var sink1Db = await GetDocumentDatabaseInstanceFor(sinkStore1);
            var sink2Db = await GetDocumentDatabaseInstanceFor(sinkStore2);

            using (hubDb.DocumentsStorage.ContextPool.AllocateOperationContext(out DocumentsOperationContext ctx))
            using (ctx.OpenReadTransaction())
            {
                var hubGlobalCv = DocumentsStorage.GetDatabaseChangeVector(ctx);
                Assert.Equal(1, hubGlobalCv.ToChangeVector().Length);
            }

            using (sink1Db.DocumentsStorage.ContextPool.AllocateOperationContext(out DocumentsOperationContext ctx))
            using (ctx.OpenReadTransaction())
            {
                var sink1GlobalCv = DocumentsStorage.GetDatabaseChangeVector(ctx);
                Assert.Equal(3, sink1GlobalCv.ToChangeVector().Length);
            }

            using (sink2Db.DocumentsStorage.ContextPool.AllocateOperationContext(out DocumentsOperationContext ctx))
            using (ctx.OpenReadTransaction())
            {
                var sink2GlobalCv = DocumentsStorage.GetDatabaseChangeVector(ctx);
                Assert.Equal(3, sink2GlobalCv.ToChangeVector().Length);
            }

            await EnsureNoReplicationLoop(Server, hubStore.Database);
            await EnsureNoReplicationLoop(Server, sinkStore1.Database);
            await EnsureNoReplicationLoop(Server, sinkStore2.Database);

            async Task SetupSink(DocumentStore sinkStore)
            {
                await sinkStore.Maintenance.SendAsync(new PutConnectionStringOperation<RavenConnectionString>(new RavenConnectionString
                {
                    Database = hubStore.Database, Name = hubStore.Database + "ConStr", TopologyDiscoveryUrls = hubStore.Urls
                }));
                await sinkStore.Maintenance.SendAsync(new UpdatePullReplicationAsSinkOperation(new PullReplicationAsSink
                {
                    ConnectionStringName = hubStore.Database + "ConStr",
                    Mode = PullReplicationMode.SinkToHub | PullReplicationMode.HubToSink,
                    CertificateWithPrivateKey = Convert.ToBase64String(pullCert.Export(X509ContentType.Pfx)),
                    HubName = "both",
                    AllowedHubToSinkPaths = new[] {"*",},
                    AllowedSinkToHubPaths = new[] {"*"}
                }));
            }
        }

        [Fact]
        public async Task Sinks_should_not_update_hubs_change_vector_with_conflicts2()
        {
            var certificates = SetupServerAuthentication();
            var adminCert = RegisterClientCertificate(certificates.ServerCertificate.Value, certificates
                .ClientCertificate1.Value, new Dictionary<string, DatabaseAccess>(), SecurityClearance.ClusterAdmin);

            using var hubStore = GetDocumentStore(new Options
            {
                AdminCertificate = adminCert,
                ClientCertificate = adminCert,
                ModifyDatabaseRecord = r => r.ConflictSolverConfig = new ConflictSolver
                {
                    ResolveToLatest = false
                }
            });
            using var sinkStore1 = GetDocumentStore(new Options
            {
                AdminCertificate = adminCert,
                ClientCertificate = adminCert,
                ModifyDatabaseRecord = r => r.ConflictSolverConfig = new ConflictSolver
                {
                    ResolveToLatest = false
                }
            });
            using var sinkStore2 = GetDocumentStore(new Options
            {
                AdminCertificate = adminCert,
                ClientCertificate = adminCert,
                ModifyDatabaseRecord = r => r.ConflictSolverConfig = new ConflictSolver
                {
                    ResolveToLatest = false
                }
            });

            using (var s = sinkStore1.OpenAsyncSession())
            {
                await s.StoreAsync(new Propagation
                {
                    Source = "Sink1"
                }, "common");
                await s.SaveChangesAsync();
            }

            using (var s = sinkStore2.OpenAsyncSession())
            {
                await s.StoreAsync(new Propagation
                {
                    Source = "Sink2"
                }, "common");
                await s.SaveChangesAsync();
            }

            using (var s = hubStore.OpenAsyncSession())
            {
                await s.StoreAsync(new Propagation
                {
                    Source = "Hub"
                }, "common");
                await s.SaveChangesAsync();
            }

            var pullCert = new X509Certificate2(File.ReadAllBytes(certificates.ClientCertificate2Path), (string)null,
                X509KeyStorageFlags.Exportable);

            await hubStore.Maintenance.SendAsync(new PutPullReplicationAsHubOperation(new PullReplicationDefinition
            {
                Name = "both",
                Mode = PullReplicationMode.SinkToHub | PullReplicationMode.HubToSink,
                WithFiltering = true
            }));

            await hubStore.Maintenance.SendAsync(new RegisterReplicationHubAccessOperation("both", new ReplicationHubAccess
            {
                Name = "Arava",
                AllowedSinkToHubPaths = new[]
                {
                    "*",
                },
                AllowedHubToSinkPaths = new[]
                {
                    "*"
                },
                CertificateBase64 = Convert.ToBase64String(pullCert.Export(X509ContentType.Cert)),
            }));

            await SetupSink(sinkStore1);
            await SetupSink(sinkStore2);

            EnsureReplicating(hubStore, sinkStore1);
            EnsureReplicating(sinkStore1,hubStore);
            
            EnsureReplicating(hubStore, sinkStore2);
            EnsureReplicating(sinkStore2,hubStore);

            EnsureReplicating(sinkStore1, sinkStore2);
            EnsureReplicating(sinkStore2,sinkStore1);

            await WaitForConflict(hubStore, "common");
            await WaitForConflict(sinkStore1, "common");
            await WaitForConflict(sinkStore2, "common");

            await UpdateConflictResolver(hubStore, resolveToLatest: true);

            Assert.True(WaitForDocument<Propagation>(hubStore, "common", x => x.Source == "Hub"));
            Assert.True(WaitForDocument<Propagation>(sinkStore1, "common", x => x.Source  == "Hub"));
            Assert.True(WaitForDocument<Propagation>(sinkStore2, "common", x => x.Source  == "Hub"));

            var hubDb = await GetDocumentDatabaseInstanceFor(hubStore);
            var sink1Db = await GetDocumentDatabaseInstanceFor(sinkStore1);
            var sink2Db = await GetDocumentDatabaseInstanceFor(sinkStore2);

            using (hubDb.DocumentsStorage.ContextPool.AllocateOperationContext(out DocumentsOperationContext ctx))
            using (ctx.OpenReadTransaction())
            {
                var hubGlobalCv = DocumentsStorage.GetDatabaseChangeVector(ctx);
                Assert.Equal(1, hubGlobalCv.ToChangeVector().Length);
            }

            using (sink1Db.DocumentsStorage.ContextPool.AllocateOperationContext(out DocumentsOperationContext ctx))
            using (ctx.OpenReadTransaction())
            {
                var sink1GlobalCv = DocumentsStorage.GetDatabaseChangeVector(ctx);
                Assert.Equal(3, sink1GlobalCv.ToChangeVector().Length);
            }

            using (sink2Db.DocumentsStorage.ContextPool.AllocateOperationContext(out DocumentsOperationContext ctx))
            using (ctx.OpenReadTransaction())
            {
                var sink2GlobalCv = DocumentsStorage.GetDatabaseChangeVector(ctx);
                Assert.Equal(3, sink2GlobalCv.ToChangeVector().Length);
            }

            await EnsureNoReplicationLoop(Server, hubStore.Database);
            await EnsureNoReplicationLoop(Server, sinkStore1.Database);
            await EnsureNoReplicationLoop(Server, sinkStore2.Database);

            async Task SetupSink(DocumentStore sinkStore)
            {
                await sinkStore.Maintenance.SendAsync(new PutConnectionStringOperation<RavenConnectionString>(new RavenConnectionString
                {
                    Database = hubStore.Database, Name = hubStore.Database + "ConStr", TopologyDiscoveryUrls = hubStore.Urls
                }));
                await sinkStore.Maintenance.SendAsync(new UpdatePullReplicationAsSinkOperation(new PullReplicationAsSink
                {
                    ConnectionStringName = hubStore.Database + "ConStr",
                    Mode = PullReplicationMode.SinkToHub | PullReplicationMode.HubToSink,
                    CertificateWithPrivateKey = Convert.ToBase64String(pullCert.Export(X509ContentType.Pfx)),
                    HubName = "both",
                    AllowedHubToSinkPaths = new[] {"*",},
                    AllowedSinkToHubPaths = new[] {"*"}
                }));
            }
        }

        [Fact]
        public async Task Can_import_export_replication_certs()
        {
            var certificates = SetupServerAuthentication();
            var dbNameA = GetDatabaseName();
            var dbNameB = GetDatabaseName();
            var adminCert = RegisterClientCertificate(certificates.ServerCertificate.Value, certificates
                .ClientCertificate1.Value, new Dictionary<string, DatabaseAccess>(), SecurityClearance.ClusterAdmin);

            using var storeA = GetDocumentStore(new Options
            {
                AdminCertificate = adminCert,
                ClientCertificate = adminCert,
                ModifyDatabaseName = s => dbNameA
            });
            using var storeB = GetDocumentStore(new Options
            {
                AdminCertificate = adminCert,
                ClientCertificate = adminCert,
                ModifyDatabaseName = s => dbNameB
            });

            using (var s = storeA.OpenAsyncSession())
            {
                await s.StoreAsync(new { Location = "Hadera" }, "users/ayende/office");
                await s.StoreAsync(new { Breed = "German Shepherd" }, "users/ayende/dogs/arava");
                await s.StoreAsync(new { Color = "Gray/White" }, "users/pheobe");
                await s.StoreAsync(new { Name = "Oren" }, "users/ayende");

                await s.SaveChangesAsync();
            }

            var pullCert = new X509Certificate2(File.ReadAllBytes(certificates.ClientCertificate2Path), (string)null,
                X509KeyStorageFlags.Exportable);

            await storeA.Maintenance.SendAsync(new PutPullReplicationAsHubOperation(new PullReplicationDefinition
            {
                Name = "both",
                Mode = PullReplicationMode.SinkToHub | PullReplicationMode.HubToSink,
                WithFiltering = true
            }));

            await storeA.Maintenance.SendAsync(new RegisterReplicationHubAccessOperation("both", new ReplicationHubAccess
            {
                Name = "Arava",
                AllowedSinkToHubPaths = new[]
                {
                    "users/ayende",
                    "users/ayende/*"
                },
                AllowedHubToSinkPaths = new[]
                {
                    "users/ayende/config"
                },
                CertificateBase64 = Convert.ToBase64String(pullCert.Export(X509ContentType.Cert)),
            }));

            var file = GetTempFileName();
            await storeA.Smuggler.ExportAsync(new DatabaseSmugglerExportOptions(), file);

            var accessResults = await storeB.Maintenance.SendAsync(new GetReplicationHubAccessOperation("both"));
            Assert.Empty(accessResults);

            var op = await storeB.Smuggler.ImportAsync(new DatabaseSmugglerImportOptions(), file);
            await op.WaitForCompletionAsync();

            accessResults = await storeB.Maintenance.SendAsync(new GetReplicationHubAccessOperation("both"));
            Assert.NotEmpty(accessResults);
            Assert.Equal("Arava", accessResults[0].Name);
        }
        
        [Fact]
        public async Task Cannot_use_access_paths_if_filtering_is_not_set()
        {
            var certificates = SetupServerAuthentication();
            var dbNameA = GetDatabaseName();
            var adminCert = RegisterClientCertificate(certificates.ServerCertificate.Value, certificates
                .ClientCertificate1.Value, new Dictionary<string, DatabaseAccess>(), SecurityClearance.ClusterAdmin);

            using var storeA = GetDocumentStore(new Options
            {
                AdminCertificate = adminCert,
                ClientCertificate = adminCert,
                ModifyDatabaseName = s => dbNameA
            });
            
            var pullCert = certificates.ClientCertificate2.Value;
            await storeA.Maintenance.SendAsync(new PutPullReplicationAsHubOperation(new PullReplicationDefinition
            {
                Name = "pull",
                WithFiltering = false
            }));
            
            var ex = await Assert.ThrowsAsync<RavenException>( async () => 
                await storeA.Maintenance.SendAsync(new RegisterReplicationHubAccessOperation("pull",
                    new ReplicationHubAccess
                    {
                        Name = "Arava",
                        CertificateBase64 = Convert.ToBase64String(pullCert.Export(X509ContentType.Cert)),
                        AllowedHubToSinkPaths = new[] {"users/ayende", "users/ayende/*"}
                    })
            ));
            
            Assert.Contains("Filtering replication is not set for this Replication Hub task. AllowedSinkToHubPaths and AllowedHubToSinkPaths cannot have a value.",  ex.InnerException.Message);
        }
        
        [Fact]
        public async Task Must_use_access_paths_if_filtering_is_set()
        {
            var certificates = SetupServerAuthentication();
            var dbNameA = GetDatabaseName();
            var adminCert = RegisterClientCertificate(certificates.ServerCertificate.Value, certificates
                .ClientCertificate1.Value, new Dictionary<string, DatabaseAccess>(), SecurityClearance.ClusterAdmin);

            using var storeA = GetDocumentStore(new Options
            {
                AdminCertificate = adminCert,
                ClientCertificate = adminCert,
                ModifyDatabaseName = s => dbNameA
            });
            
            var pullCert = certificates.ClientCertificate2.Value;
            await storeA.Maintenance.SendAsync(new PutPullReplicationAsHubOperation(new PullReplicationDefinition
            {
                Name = "pull",
                WithFiltering = true
            }));
            
            var ex = await Assert.ThrowsAsync<RavenException>( async () => 
                await storeA.Maintenance.SendAsync(new RegisterReplicationHubAccessOperation("pull",
                    new ReplicationHubAccess
                    {
                        Name = "Arava",
                        CertificateBase64 = Convert.ToBase64String(pullCert.Export(X509ContentType.Cert))
                    })
                ));
            
            Assert.Contains("Either AllowedSinkToHubPaths or AllowedHubToSinkPaths must have a value, but both were null or empty",  ex.InnerException.Message);
        }
    }
}
