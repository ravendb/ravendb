using System;
using System.Collections.Generic;
using System.IO;
using System.Linq;
using System.Security.Cryptography.X509Certificates;
using System.Text;
using System.Threading;
using System.Threading.Tasks;
using Raven.Client.Documents;
using Raven.Client.Documents.Operations;
using Raven.Client.Documents.Operations.ConnectionStrings;
using Raven.Client.Documents.Operations.ETL;
using Raven.Client.Documents.Operations.OngoingTasks;
using Raven.Client.Documents.Operations.Replication;
using Raven.Client.Documents.Session;
using Raven.Client.Documents.Session.TimeSeries;
using Raven.Client.Documents.Smuggler;
using Raven.Client.Exceptions;
using Raven.Client.ServerWide.Operations;
using Raven.Client.ServerWide.Operations.Certificates;
using Raven.Client.Util;
using Raven.Server.Documents;
using Raven.Server.Documents.Replication;
using Raven.Server.ServerWide;
using Raven.Server.ServerWide.Context;
using Raven.Tests.Core.Utils.Entities;
using Sparrow;
using Tests.Infrastructure;
using Xunit;
using Xunit.Abstractions;

namespace SlowTests.Issues
{
    public class FilteredReplicationTests : ReplicationTestBase
    {
        public FilteredReplicationTests(ITestOutputHelper output) : base(output)
        {
        }

        [RavenFact(RavenTestCategory.Replication | RavenTestCategory.Certificates)]
        public async Task Seasame_st()
        {
            var certificates = Certificates.SetupServerAuthentication();
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
                    AllowedSinkToHubPaths = new[] { "orders/bert/*" },
                    AllowedHubToSinkPaths = new[] { "menus/*", "prices/eastus/*", "recipes/*" }
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

        [RavenTheory(RavenTestCategory.Replication | RavenTestCategory.Revisions | RavenTestCategory.Counters | RavenTestCategory.TimeSeries)]
        [RavenData(DatabaseMode = RavenDatabaseMode.All)]
        public async Task Counters_and_force_revisions(Options options)
        {
            using var storeA = GetDocumentStore(options);
            using var storeB = GetDocumentStore(options);

            using (var s = storeA.OpenAsyncSession())
            {
                await s.StoreAsync(new { Breed = "German Shepherd" }, "users/ayende/dogs/arava");
                await s.StoreAsync(new { Color = "Gray/White" }, "users/pheobe");
                await s.StoreAsync(new { Name = "Oren" }, "users/ayende");
                s.CountersFor("users/ayende").Increment("test");
                s.CountersFor("users/pheobe").Increment("test");
                s.TimeSeriesFor<HeartRateMeasure>("users/pheobe").Append(RavenTestHelper.UtcToday, new HeartRateMeasure
                {
                    HeartRate = 34
                }, "test/things/out");
                s.TimeSeriesFor<HeartRateMeasure>("users/ayende").Append(RavenTestHelper.UtcToday, new HeartRateMeasure
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

        [RavenFact(RavenTestCategory.Replication | RavenTestCategory.Certificates)]
        public async Task Can_Setup_Filtered_Replication()
        {
            var certificates = Certificates.SetupServerAuthentication();
            var dbNameA = GetDatabaseName();
            var adminCert = Certificates.RegisterClientCertificate(certificates.ServerCertificate.Value, certificates
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

        [RavenFact(RavenTestCategory.Replication | RavenTestCategory.Certificates)]
        public async Task Cannot_setup_partial_filtered_replication()
        {
            var certificates = Certificates.SetupServerAuthentication();
            var dbNameA = GetDatabaseName();
            var adminCert = Certificates.RegisterClientCertificate(certificates.ServerCertificate.Value, certificates
                .ClientCertificate1.Value, new Dictionary<string, DatabaseAccess>(), SecurityClearance.ClusterAdmin);

            using var storeA = GetDocumentStore(new Options
            {
                AdminCertificate = adminCert,
                ClientCertificate = adminCert,
                ModifyDatabaseName = s => dbNameA
            });

            var pullCertA = certificates.ClientCertificate2.Value;
            var pullCertB = certificates.ClientCertificate3.Value;

            await storeA.Maintenance.SendAsync(new PutPullReplicationAsHubOperation(new PullReplicationDefinition { Name = "pull" }));

            await storeA.Maintenance.SendAsync(new RegisterReplicationHubAccessOperation("pull", new ReplicationHubAccess
            {
                Name = "pull1",
                CertificateBase64 = Convert.ToBase64String(pullCertA.Export(X509ContentType.Cert))
            }));

            await storeA.Maintenance.SendAsync(new RegisterReplicationHubAccessOperation("pull", new ReplicationHubAccess
            {
                Name = "pull2",
                CertificateBase64 = Convert.ToBase64String(pullCertB.Export(X509ContentType.Cert))
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

        [RavenFact(RavenTestCategory.Replication | RavenTestCategory.Certificates)]
        public async Task WhenDeletingHubReplicationWillRemoveAllAccess()
        {
            var certificates = Certificates.SetupServerAuthentication();
            var dbNameA = GetDatabaseName();
            var adminCert = Certificates.RegisterClientCertificate(certificates.ServerCertificate.Value, certificates
                .ClientCertificate1.Value, new Dictionary<string, DatabaseAccess>(), SecurityClearance.ClusterAdmin);

            using var storeA = GetDocumentStore(new Options
            {
                AdminCertificate = adminCert,
                ClientCertificate = adminCert,
                ModifyDatabaseName = s => dbNameA
            });
            long[] ids = new long[3];
            var pullCert = CertificateHelper.CreateCertificateFromPfx(File.ReadAllBytes(certificates.ClientCertificate2Path), (string)null,
                X509KeyStorageFlags.Exportable);
            for (int i = 0; i < 3; i++)
            {
                var op = await storeA.Maintenance.SendAsync(new PutPullReplicationAsHubOperation(new PullReplicationDefinition
                {
                    Name = "pull" + i,
                    Mode = PullReplicationMode.SinkToHub | PullReplicationMode.HubToSink,
                    WithFiltering = true
                }));

                ids[i] = op.TaskId;

                await storeA.Maintenance.SendAsync(new RegisterReplicationHubAccessOperation("pull" + i, new ReplicationHubAccess
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

        [RavenFact(RavenTestCategory.Replication | RavenTestCategory.Certificates)]
        public async Task Can_pull_via_filtered_replication()
        {
            var certificates = Certificates.SetupServerAuthentication();
            var dbNameA = GetDatabaseName();
            var dbNameB = GetDatabaseName();
            var adminCert = Certificates.RegisterClientCertificate(certificates.ServerCertificate.Value, certificates
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
                s.TimeSeriesFor<HeartRateMeasure>("users/pheobe").Append(RavenTestHelper.UtcToday, new HeartRateMeasure
                {
                    HeartRate = 34
                }, "test/things/out");
                s.TimeSeriesFor<HeartRateMeasure>("users/ayende").Append(RavenTestHelper.UtcToday, new HeartRateMeasure
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

            var pullCert = CertificateHelper.CreateCertificateFromPfx(File.ReadAllBytes(certificates.ClientCertificate2Path), (string)null,
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
                Assert.Null(await s.Advanced.Revisions.GetAsync<object>("users/pheobe", RavenTestHelper.UtcToday.AddDays(1)));
                Assert.Null(await s.CountersFor("users/pheobe").GetAsync("test"));
                Assert.Null(await s.TimeSeriesFor<HeartRateMeasure>("users/pheobe").GetAsync());
                using (var attachment = await s.Advanced.Attachments.GetAsync("users/pheobe", "test.bin"))
                {
                    Assert.Null(attachment);
                }

                Assert.NotNull(await s.LoadAsync<object>("users/ayende/dogs/arava"));
                Assert.NotNull(await s.LoadAsync<object>("users/ayende"));
                Assert.NotNull(await s.Advanced.Revisions.GetAsync<object>("users/ayende", RavenTestHelper.UtcToday.AddDays(1)));

                Assert.NotNull(await s.Advanced.Revisions.GetAsync<object>("users/ayende", RavenTestHelper.UtcToday.AddDays(1)));
                Assert.NotNull(await s.CountersFor("users/ayende").GetAsync("test"));
                Assert.NotEmpty(await s.TimeSeriesFor<HeartRateMeasure>("users/ayende").GetAsync());
                using (var attachment = await s.Advanced.Attachments.GetAsync("users/ayende", "test.bin"))
                {
                    Assert.NotNull(attachment);
                }
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

                Assert.NotNull(await s.Advanced.Revisions.GetAsync<object>("users/ayende", RavenTestHelper.UtcToday.AddDays(1)));
                Assert.NotNull(await s.CountersFor("users/ayende").GetAsync("test"));
                Assert.NotEmpty(await s.TimeSeriesFor<HeartRateMeasure>("users/ayende").GetAsync());
                using (var attachment = await s.Advanced.Attachments.GetAsync("users/ayende", "test.bin"))
                {
                    Assert.NotNull(attachment);
                }
            }
        }

        [RavenFact(RavenTestCategory.Replication | RavenTestCategory.Certificates)]
        public async Task Can_push_via_filtered_replication()
        {
            var certificates = Certificates.SetupServerAuthentication();
            var dbNameA = GetDatabaseName();
            var dbNameB = GetDatabaseName();
            var adminCert = Certificates.RegisterClientCertificate(certificates.ServerCertificate.Value, certificates
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
                s.TimeSeriesFor<HeartRateMeasure>("users/pheobe").Append(RavenTestHelper.UtcToday, new HeartRateMeasure
                {
                    HeartRate = 34
                }, "test/things/out");
                s.TimeSeriesFor<HeartRateMeasure>("users/ayende").Append(RavenTestHelper.UtcToday, new HeartRateMeasure
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

            var pullCert = CertificateHelper.CreateCertificateFromPfx(File.ReadAllBytes(certificates.ClientCertificate2Path), (string)null,
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
                Assert.Null(await s.Advanced.Revisions.GetAsync<object>("users/pheobe", RavenTestHelper.UtcToday.AddDays(1)));
                Assert.Null(await s.CountersFor("users/pheobe").GetAsync("test"));
                Assert.Null(await s.TimeSeriesFor<HeartRateMeasure>("users/pheobe").GetAsync());
                using (var attachment = await s.Advanced.Attachments.GetAsync("users/pheobe", "test.bin"))
                {
                    Assert.Null(attachment);
                }

                Assert.NotNull(await s.LoadAsync<object>("users/ayende/dogs/arava"));
                Assert.NotNull(await s.LoadAsync<object>("users/ayende"));
                Assert.NotNull(await s.Advanced.Revisions.GetAsync<object>("users/ayende", RavenTestHelper.UtcToday.AddDays(1)));

                Assert.NotNull(await s.Advanced.Revisions.GetAsync<object>("users/ayende", RavenTestHelper.UtcToday.AddDays(1)));
                Assert.NotNull(await s.CountersFor("users/ayende").GetAsync("test"));
                Assert.NotEmpty(await s.TimeSeriesFor<HeartRateMeasure>("users/ayende").GetAsync());
                using (var attachment = await s.Advanced.Attachments.GetAsync("users/ayende", "test.bin"))
                {
                    Assert.NotNull(attachment);
                }
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

                Assert.NotNull(await s.Advanced.Revisions.GetAsync<object>("users/ayende", RavenTestHelper.UtcToday.AddDays(1)));
                Assert.NotNull(await s.CountersFor("users/ayende").GetAsync("test"));
                Assert.NotEmpty(await s.TimeSeriesFor<HeartRateMeasure>("users/ayende").GetAsync());
                using (var attachment = await s.Advanced.Attachments.GetAsync("users/ayende", "test.bin"))
                {
                    Assert.NotNull(attachment);
                }
            }
        }

        [RavenFact(RavenTestCategory.Replication | RavenTestCategory.Certificates)]
        public async Task Can_pull_and_push_and_filter_at_dest_and_source()
        {
            var certificates = Certificates.SetupServerAuthentication();
            var dbNameA = GetDatabaseName();
            var dbNameB = GetDatabaseName();
            var adminCert = Certificates.RegisterClientCertificate(certificates.ServerCertificate.Value, certificates
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

            var pullCert = CertificateHelper.CreateCertificateFromPfx(File.ReadAllBytes(certificates.ClientCertificate2Path), (string)null,
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

        [RavenFact(RavenTestCategory.Replication | RavenTestCategory.Certificates)]
        public async Task PickupConfigurationChangesOnTheFly()
        {
            var certificates = Certificates.SetupServerAuthentication();
            var adminCert = Certificates.RegisterClientCertificate(certificates.ServerCertificate.Value, certificates
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

            var pullCert = CertificateHelper.CreateCertificateFromPfx(File.ReadAllBytes(certificates.ClientCertificate2Path), (string)null,
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
                Database = hubStore.Database,
                Name = hubStore.Database + "ConStr",
                TopologyDiscoveryUrls = hubStore.Urls
            }));

            var sinkTask = new PullReplicationAsSink
            {
                ConnectionStringName = hubStore.Database + "ConStr",
                Mode = PullReplicationMode.HubToSink,
                CertificateWithPrivateKey = Convert.ToBase64String(pullCert.Export(X509ContentType.Pfx)),
                HubName = "both",
                AllowedHubToSinkPaths = new[] { "*", },
                AllowedSinkToHubPaths = new[] { "*" }
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

        [RavenFact(RavenTestCategory.Replication | RavenTestCategory.Certificates)]
        public async Task Sinks_should_not_update_hubs_change_vector()
        {
            var certificates = Certificates.SetupServerAuthentication();
            var adminCert = Certificates.RegisterClientCertificate(certificates.ServerCertificate.Value, certificates
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

            var pullCert = CertificateHelper.CreateCertificateFromPfx(File.ReadAllBytes(certificates.ClientCertificate2Path), (string)null,
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
            EnsureReplicating(sinkStore1, hubStore);

            EnsureReplicating(hubStore, sinkStore2);
            EnsureReplicating(sinkStore2, hubStore);

            EnsureReplicating(sinkStore1, sinkStore2);
            EnsureReplicating(sinkStore2, sinkStore1);

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

            var hubDb = await Databases.GetDocumentDatabaseInstanceFor(hubStore);
            var sink1Db = await Databases.GetDocumentDatabaseInstanceFor(sinkStore1);
            var sink2Db = await Databases.GetDocumentDatabaseInstanceFor(sinkStore2);

            using (hubDb.DocumentsStorage.ContextPool.AllocateOperationContext(out DocumentsOperationContext ctx))
            using (ctx.OpenReadTransaction())
            {
                var hubGlobalCv = DocumentsStorage.GetDatabaseChangeVector(ctx).AsString();
                Assert.Equal(1, hubGlobalCv.ToChangeVector().Length);
            }

            using (sink1Db.DocumentsStorage.ContextPool.AllocateOperationContext(out DocumentsOperationContext ctx))
            using (ctx.OpenReadTransaction())
            {
                var sink1GlobalCv = DocumentsStorage.GetDatabaseChangeVector(ctx).AsString();
                Assert.Equal(2, sink1GlobalCv.ToChangeVector().Length);
            }

            using (sink2Db.DocumentsStorage.ContextPool.AllocateOperationContext(out DocumentsOperationContext ctx))
            using (ctx.OpenReadTransaction())
            {
                var sink2GlobalCv = DocumentsStorage.GetDatabaseChangeVector(ctx).AsString();
                Assert.Equal(2, sink2GlobalCv.ToChangeVector().Length);
            }

            async Task SetupSink(DocumentStore sinkStore)
            {
                await sinkStore.Maintenance.SendAsync(new PutConnectionStringOperation<RavenConnectionString>(new RavenConnectionString
                {
                    Database = hubStore.Database,
                    Name = hubStore.Database + "ConStr",
                    TopologyDiscoveryUrls = hubStore.Urls
                }));
                await sinkStore.Maintenance.SendAsync(new UpdatePullReplicationAsSinkOperation(new PullReplicationAsSink
                {
                    ConnectionStringName = hubStore.Database + "ConStr",
                    Mode = PullReplicationMode.SinkToHub | PullReplicationMode.HubToSink,
                    CertificateWithPrivateKey = Convert.ToBase64String(pullCert.Export(X509ContentType.Pfx)),
                    HubName = "both",
                    AllowedHubToSinkPaths = new[] { "*", },
                    AllowedSinkToHubPaths = new[] { "*" }
                }));
            }
        }

        [RavenFact(RavenTestCategory.Replication | RavenTestCategory.Certificates)]
        public async Task Sinks_should_not_update_hubs_change_vector2()
        {
            var certificates = Certificates.SetupServerAuthentication();
            var adminCert = Certificates.RegisterClientCertificate(certificates.ServerCertificate.Value, certificates
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

            var pullCert = CertificateHelper.CreateCertificateFromPfx(File.ReadAllBytes(certificates.ClientCertificate2Path), (string)null,
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
            EnsureReplicating(sinkStore1, hubStore);

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
                s.TimeSeriesFor("common", "test").Append(RavenTestHelper.UtcToday, 12);
                await s.SaveChangesAsync();
            }

            using (var s = sinkStore1.OpenAsyncSession())
            {
                s.CountersFor("common").Increment("test");
                await s.SaveChangesAsync();
            }

            using (var s = sinkStore1.OpenAsyncSession())
            {
                await using (var ms = new MemoryStream(new byte[] { 1, 2, 3, 4, 5 }))
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


            var hubDb = await Databases.GetDocumentDatabaseInstanceFor(hubStore);
            var sink1Db = await Databases.GetDocumentDatabaseInstanceFor(sinkStore1);

            using (hubDb.DocumentsStorage.ContextPool.AllocateOperationContext(out DocumentsOperationContext ctx))
            using (ctx.OpenReadTransaction())
            {
                var hubGlobalCv = DocumentsStorage.GetDatabaseChangeVector(ctx).AsString();
                Assert.Equal(1, hubGlobalCv.ToChangeVector().Length);
            }

            using (sink1Db.DocumentsStorage.ContextPool.AllocateOperationContext(out DocumentsOperationContext ctx))
            using (ctx.OpenReadTransaction())
            {
                var sink1GlobalCv = DocumentsStorage.GetDatabaseChangeVector(ctx).AsString();
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
                    Database = hubStore.Database,
                    Name = hubStore.Database + "ConStr",
                    TopologyDiscoveryUrls = hubStore.Urls
                }));
                await sinkStore.Maintenance.SendAsync(new UpdatePullReplicationAsSinkOperation(new PullReplicationAsSink
                {
                    ConnectionStringName = hubStore.Database + "ConStr",
                    Mode = PullReplicationMode.SinkToHub | PullReplicationMode.HubToSink,
                    CertificateWithPrivateKey = Convert.ToBase64String(pullCert.Export(X509ContentType.Pfx)),
                    HubName = "both",
                    AllowedHubToSinkPaths = new[] { "*", },
                    AllowedSinkToHubPaths = new[] { "*" }
                }));
            }
        }

        [RavenFact(RavenTestCategory.Replication | RavenTestCategory.Certificates)]
        public async Task Sinks_should_not_update_hubs_change_vector3()
        {
            var certificates = Certificates.SetupServerAuthentication();
            var adminCert = Certificates.RegisterClientCertificate(certificates.ServerCertificate.Value, certificates
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

            var fooCert = CertificateHelper.CreateCertificateFromPfx(File.ReadAllBytes(certificates.ClientCertificate2Path), (string)null,
                X509KeyStorageFlags.Exportable);

            var barCert = CertificateHelper.CreateCertificateFromPfx(File.ReadAllBytes(certificates.ClientCertificate3Path), (string)null,
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
                AllowedSinkToHubPaths = new[] { "foo" },
                AllowedHubToSinkPaths = new[] { "foo" },
                CertificateBase64 = Convert.ToBase64String(fooCert.Export(X509ContentType.Cert)),
            };

            var access2 = new ReplicationHubAccess
            {
                Name = "both",
                AllowedSinkToHubPaths = new[] { "bar" },
                AllowedHubToSinkPaths = new[] { "bar" },
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
                var baseline = RavenTestHelper.UtcToday;
                for (int i = 0; i < 150; i++)
                {
                    s.TimeSeriesFor("foo", "test").Append(baseline.AddHours(i), 1);
                    s.TimeSeriesFor("bar", "test").Append(baseline.AddHours(i), 1);
                }
                await s.SaveChangesAsync();
            }

            Assert.True(WaitForDocument(sinkStore1, "foo"));
            Assert.True(WaitForDocument(sinkStore2, "bar"));

            await sinkStore1.TimeSeries.SetPolicyAsync<Propagation>("By3Hours", TimeValue.FromHours(3), TimeValue.FromDays(3));
            await sinkStore2.TimeSeries.SetPolicyAsync<Propagation>("By3Hours", TimeValue.FromHours(3), TimeValue.FromDays(3));

            var hubDb = await Databases.GetDocumentDatabaseInstanceFor(hubStore);
            var sink1Db = await Databases.GetDocumentDatabaseInstanceFor(sinkStore1);
            var sink2Db = await Databases.GetDocumentDatabaseInstanceFor(sinkStore2);

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

            using (var token = new OperationCancelToken(hubDb.Configuration.Databases.OperationTimeout.AsTimeSpan, hubDb.DatabaseShutdown, CancellationToken.None))
                await hubDb.DocumentsStorage.RevisionsStorage.EnforceConfigurationAsync(_ => { }, token);

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
                var hubGlobalCv = DocumentsStorage.GetDatabaseChangeVector(ctx).AsString();
                Assert.Equal(1, hubGlobalCv.ToChangeVector().Length);
            }

            using (sink1Db.DocumentsStorage.ContextPool.AllocateOperationContext(out DocumentsOperationContext ctx))
            using (ctx.OpenReadTransaction())
            {
                var sink1GlobalCv = DocumentsStorage.GetDatabaseChangeVector(ctx).AsString();
                Assert.Equal(2, sink1GlobalCv.ToChangeVector().Length);
            }

            using (sink2Db.DocumentsStorage.ContextPool.AllocateOperationContext(out DocumentsOperationContext ctx))
            using (ctx.OpenReadTransaction())
            {
                var sink2GlobalCv = DocumentsStorage.GetDatabaseChangeVector(ctx).AsString();
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
                    Database = hubStore.Database,
                    Name = hubStore.Database + "ConStr",
                    TopologyDiscoveryUrls = hubStore.Urls
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

        [RavenFact(RavenTestCategory.Replication | RavenTestCategory.Certificates | RavenTestCategory.BackupExportImport)]
        public async Task Can_import_export_replication_certs()
        {
            var certificates = Certificates.SetupServerAuthentication();
            var dbNameA = GetDatabaseName();
            var dbNameB = GetDatabaseName();
            var adminCert = Certificates.RegisterClientCertificate(certificates.ServerCertificate.Value, certificates
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

            var pullCert = CertificateHelper.CreateCertificateFromPfx(File.ReadAllBytes(certificates.ClientCertificate2Path), (string)null,
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
            var op = await storeA.Smuggler.ExportAsync(new DatabaseSmugglerExportOptions(), file);
            await op.WaitForCompletionAsync(TimeSpan.FromMinutes(5));

            var accessResults = await storeB.Maintenance.SendAsync(new GetReplicationHubAccessOperation("both"));
            Assert.Empty(accessResults);

            op = await storeB.Smuggler.ImportAsync(new DatabaseSmugglerImportOptions(), file);
            await op.WaitForCompletionAsync(TimeSpan.FromMinutes(5));

            accessResults = await storeB.Maintenance.SendAsync(new GetReplicationHubAccessOperation("both"));
            Assert.NotEmpty(accessResults);
            Assert.Equal("Arava", accessResults[0].Name);
        }

        [RavenFact(RavenTestCategory.Replication | RavenTestCategory.Certificates)]
        public async Task Cannot_use_access_paths_if_filtering_is_not_set()
        {
            var certificates = Certificates.SetupServerAuthentication();
            var dbNameA = GetDatabaseName();
            var adminCert = Certificates.RegisterClientCertificate(certificates.ServerCertificate.Value, certificates
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

            var ex = await Assert.ThrowsAsync<RavenException>(async () =>
               await storeA.Maintenance.SendAsync(new RegisterReplicationHubAccessOperation("pull",
                   new ReplicationHubAccess
                   {
                       Name = "Arava",
                       CertificateBase64 = Convert.ToBase64String(pullCert.Export(X509ContentType.Cert)),
                       AllowedHubToSinkPaths = new[] { "users/ayende", "users/ayende/*" }
                   })
           ));

            Assert.Contains("Filtering replication is not set for this Replication Hub task. AllowedSinkToHubPaths and AllowedHubToSinkPaths cannot have a value.", ex.InnerException.Message);
        }

        [RavenFact(RavenTestCategory.Replication | RavenTestCategory.Certificates)]
        public async Task Must_use_access_paths_if_filtering_is_set()
        {
            var certificates = Certificates.SetupServerAuthentication();
            var dbNameA = GetDatabaseName();
            var adminCert = Certificates.RegisterClientCertificate(certificates.ServerCertificate.Value, certificates
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

            var ex = await Assert.ThrowsAsync<RavenException>(async () =>
               await storeA.Maintenance.SendAsync(new RegisterReplicationHubAccessOperation("pull",
                   new ReplicationHubAccess
                   {
                       Name = "Arava",
                       CertificateBase64 = Convert.ToBase64String(pullCert.Export(X509ContentType.Cert))
                   })
               ));

            Assert.Contains("Either AllowedSinkToHubPaths or AllowedHubToSinkPaths must have a value, but both were null or empty", ex.InnerException.Message);
        }

        [RavenFact(RavenTestCategory.Replication)]
        public async Task Can_pull_and_push_with_first_transaction_on_sink()
        {
            var dbNameA = GetDatabaseName();
            var dbNameB = GetDatabaseName();

            var (hubNodes, hubLeader, hubCertificates) = await CreateRaftClusterWithSsl(2, watcherCluster: true);
            using var hub = GetDocumentStore(new Options
            {
                Server = hubLeader,
                ReplicationFactor = 2,
                AdminCertificate = hubCertificates.ServerCertificate.Value,
                ClientCertificate = hubCertificates.ServerCertificate.Value,
                ModifyDatabaseName = s => dbNameA,
                CreateDatabase = true
            });

            using var sink = GetDocumentStore(new Options
            {
                AdminCertificate = hubCertificates.ServerCertificate.Value,
                ClientCertificate = hubCertificates.ServerCertificate.Value,
                ModifyDatabaseName = s => dbNameB
            });

            var sinkDatabaseId = (await GetDatabase(Server, sink.Database)).DbBase64Id;
            var hubDatabaseIds = await Task.WhenAll(hubNodes.Select(async node => (await GetDatabase(node, hub.Database)).DbBase64Id));

            const string usersDocId1 = "users/1";

            using (var session = sink.OpenAsyncSession())
            {
                var user = new User { Name = "Grisha" };
                await session.StoreAsync(user, usersDocId1);
                await session.SaveChangesAsync();

                var changeVector = session.Advanced.GetChangeVectorFor(user);
                Assert.True(changeVector.Contains(sinkDatabaseId));
            }

            var pullCert = CertificateHelper.CreateCertificateFromPfx(File.ReadAllBytes(hubCertificates.ClientCertificate2Path), (string)null,
                X509KeyStorageFlags.Exportable);

            await hub.Maintenance.SendAsync(new PutPullReplicationAsHubOperation(new PullReplicationDefinition
            {
                Name = "both",
                Mode = PullReplicationMode.SinkToHub | PullReplicationMode.HubToSink,
                WithFiltering = true
            }));

            await hub.Maintenance.SendAsync(new RegisterReplicationHubAccessOperation("both", new ReplicationHubAccess
            {
                Name = "Arava",
                CertificateBase64 = Convert.ToBase64String(pullCert.Export(X509ContentType.Cert)),
                AllowedHubToSinkPaths = new[] { "*" },
                AllowedSinkToHubPaths = new[] { "*" }
            }));

            await sink.Maintenance.SendAsync(new PutConnectionStringOperation<RavenConnectionString>(new RavenConnectionString
            {
                Database = dbNameA,
                Name = dbNameA + "ConStr",
                TopologyDiscoveryUrls = hub.Urls
            }));
            await sink.Maintenance.SendAsync(new UpdatePullReplicationAsSinkOperation(new PullReplicationAsSink
            {
                ConnectionStringName = dbNameA + "ConStr",
                Mode = PullReplicationMode.SinkToHub | PullReplicationMode.HubToSink,
                CertificateWithPrivateKey = Convert.ToBase64String(pullCert.Export(X509ContentType.Pfx)),
                HubName = "both",
                AllowedHubToSinkPaths = new[] { "*" },
                AllowedSinkToHubPaths = new[] { "*" }
            }));

            WaitForDocument(hub, usersDocId1);

            const int age = 38;
            using (var session = hub.OpenAsyncSession())
            {
                var user = await session.LoadAsync<User>(usersDocId1);
                Assert.NotNull(user);

                var changeVector = session.Advanced.GetChangeVectorFor(user);
                Assert.True(changeVector.Contains(ChangeVectorParser.SinkTag));
                Assert.True(changeVector.Contains(sinkDatabaseId));

                var stats = await hub.Maintenance.SendAsync(new GetStatisticsOperation());
                Assert.False(stats.DatabaseChangeVector.Contains(ChangeVectorParser.SinkTag));

                user.Age = age;
                await session.SaveChangesAsync();
                changeVector = session.Advanced.GetChangeVectorFor(user);
                Assert.True(changeVector.Contains(ChangeVectorParser.SinkTag));
                Assert.True(changeVector.Contains(sinkDatabaseId));
                Assert.True(hubDatabaseIds.Any(id => changeVector.Contains(id)));

                stats = await hub.Maintenance.SendAsync(new GetStatisticsOperation());
                Assert.False(stats.DatabaseChangeVector.Contains(ChangeVectorParser.SinkTag));
            }

            var ageVal = await WaitForValueAsync(async () =>
            {
                using (var session = sink.OpenAsyncSession())
                {
                    var user = await session.LoadAsync<User>(usersDocId1);
                    return user.Age;
                }
            }, age);
            Assert.Equal(age, ageVal);

            using (var session = sink.OpenAsyncSession())
            {
                var user = await session.LoadAsync<User>(usersDocId1);

                var changeVector = session.Advanced.GetChangeVectorFor(user);
                Assert.False(changeVector.Contains(ChangeVectorParser.SinkTag));
                Assert.True(changeVector.Contains(sinkDatabaseId));
                Assert.True(hubDatabaseIds.Any(id => changeVector.Contains(id)));

                var stats = await sink.Maintenance.SendAsync(new GetStatisticsOperation());
                Assert.False(stats.DatabaseChangeVector.Contains(ChangeVectorParser.SinkTag));
            }
        }

        [RavenFact(RavenTestCategory.ClusterTransactions | RavenTestCategory.Replication)]
        public async Task Can_pull_and_push_with_first_cluster_transactions_on_sink()
        {
            var dbNameA = GetDatabaseName();
            var dbNameB = GetDatabaseName();

            var (hubNodes, hubLeader, hubCertificates) = await CreateRaftClusterWithSsl(2);
            using var hub = GetDocumentStore(new Options
            {
                Server = hubLeader,
                ReplicationFactor = 2,
                AdminCertificate = hubCertificates.ServerCertificate.Value,
                ClientCertificate = hubCertificates.ServerCertificate.Value,
                ModifyDatabaseName = s => dbNameA,
                CreateDatabase = true
            });

            using var sink = GetDocumentStore(new Options
            {
                AdminCertificate = hubCertificates.ServerCertificate.Value,
                ClientCertificate = hubCertificates.ServerCertificate.Value,
                ModifyDatabaseName = s => dbNameB
            });

            var sinkRecord = await sink.Maintenance.Server.SendAsync(new GetDatabaseRecordOperation(sink.Database));
            var sinkClusterId = sinkRecord.Topology.ClusterTransactionIdBase64;
            var hubDatabaseIds = await Task.WhenAll(hubNodes.Select(async node => (await GetDatabase(node, hub.Database)).DbBase64Id));

            const string usersDocId1 = "users/1";

            using (var session = sink.OpenAsyncSession())
            {
                var user = new User { Name = "Grisha" };
                session.Advanced.SetTransactionMode(TransactionMode.ClusterWide);
                await session.StoreAsync(user, usersDocId1);
                await session.SaveChangesAsync();

                var changeVector = session.Advanced.GetChangeVectorFor(user);
                Assert.True(changeVector.Contains(ChangeVectorParser.TrxnTag));
                Assert.True(changeVector.Contains(sinkClusterId));
            }

            var pullCert = CertificateHelper.CreateCertificateFromPfx(File.ReadAllBytes(hubCertificates.ClientCertificate2Path), (string)null,
                X509KeyStorageFlags.Exportable);

            await hub.Maintenance.SendAsync(new PutPullReplicationAsHubOperation(new PullReplicationDefinition
            {
                Name = "both",
                Mode = PullReplicationMode.SinkToHub | PullReplicationMode.HubToSink,
                WithFiltering = true
            }));

            await hub.Maintenance.SendAsync(new RegisterReplicationHubAccessOperation("both", new ReplicationHubAccess
            {
                Name = "Arava",
                CertificateBase64 = Convert.ToBase64String(pullCert.Export(X509ContentType.Cert)),
                AllowedHubToSinkPaths = new[] { "*" },
                AllowedSinkToHubPaths = new[] { "*" }
            }));

            await sink.Maintenance.SendAsync(new PutConnectionStringOperation<RavenConnectionString>(new RavenConnectionString
            {
                Database = dbNameA,
                Name = dbNameA + "ConStr",
                TopologyDiscoveryUrls = hub.Urls
            }));
            await sink.Maintenance.SendAsync(new UpdatePullReplicationAsSinkOperation(new PullReplicationAsSink
            {
                ConnectionStringName = dbNameA + "ConStr",
                Mode = PullReplicationMode.SinkToHub | PullReplicationMode.HubToSink,
                CertificateWithPrivateKey = Convert.ToBase64String(pullCert.Export(X509ContentType.Pfx)),
                HubName = "both",
                AllowedHubToSinkPaths = new[] { "*" },
                AllowedSinkToHubPaths = new[] { "*" }
            }));

            Assert.True(await WaitForDocumentInClusterAsync<User>
                (hubNodes, dbNameA, usersDocId1, u => u.Name == "Grisha",
                    timeout: TimeSpan.FromSeconds(30), certificate: hubCertificates.ServerCertificate.Value));

            const int age = 38;
            using (var session = hub.OpenAsyncSession())
            {
                var user = await session.LoadAsync<User>(usersDocId1);
                Assert.NotNull(user);

                var changeVector = session.Advanced.GetChangeVectorFor(user);
                Assert.True(changeVector.Contains(ChangeVectorParser.SinkTag));
                Assert.True(changeVector.Contains(sinkClusterId));

                await VerifyDatabaseChangeVector(hub);

                user.Age = age;
                await session.SaveChangesAsync();
                changeVector = session.Advanced.GetChangeVectorFor(user);
                Assert.True(changeVector.Contains(ChangeVectorParser.SinkTag));
                Assert.True(changeVector.Contains(sinkClusterId));
                Assert.True(hubDatabaseIds.Any(id => changeVector.Contains(id)));

                await VerifyDatabaseChangeVector(hub);
            }

            var ageVal = await WaitForValueAsync(async () =>
            {
                using (var session = sink.OpenAsyncSession())
                {
                    var user = await session.LoadAsync<User>(usersDocId1);
                    return user.Age;
                }
            }, age);
            Assert.Equal(age, ageVal);

            using (var session = sink.OpenAsyncSession())
            {
                var user = await session.LoadAsync<User>(usersDocId1);

                var changeVector = session.Advanced.GetChangeVectorFor(user);
                Assert.True(changeVector.Contains(ChangeVectorParser.TrxnTag));
                Assert.True(changeVector.Contains(ChangeVectorParser.RaftTag));
                Assert.False(changeVector.Contains(ChangeVectorParser.SinkTag));
                Assert.True(changeVector.Contains(sinkClusterId));
                Assert.True(hubDatabaseIds.Any(id => changeVector.Contains(id)));

                await VerifyDatabaseChangeVector(sink);
            }

            using (var session = sink.OpenAsyncSession())
            {
                const string anotherUserId = "users/2";
                var user = new User();
                await session.StoreAsync(user, anotherUserId);
                await session.SaveChangesAsync();

                user.Name = "Grisha";
                await session.SaveChangesAsync();
            }

            async Task VerifyDatabaseChangeVector(DocumentStore store)
            {
                var stats = await store.Maintenance.SendAsync(new GetStatisticsOperation());
                Assert.False(stats.DatabaseChangeVector.Contains(ChangeVectorParser.TrxnTag));
                Assert.False(stats.DatabaseChangeVector.Contains(ChangeVectorParser.SinkTag));
                Assert.False(stats.DatabaseChangeVector.Contains(sinkClusterId));
            }
        }

        [RavenFact(RavenTestCategory.Replication)]
        public async Task Can_pull_and_push_with_first_transactions_on_hub()
        {
            var dbNameA = GetDatabaseName();
            var dbNameB = GetDatabaseName();

            var (hubNodes, hubLeader, hubCertificates) = await CreateRaftClusterWithSsl(2, watcherCluster: true);
            using var hub = GetDocumentStore(new Options
            {
                Server = hubLeader,
                ReplicationFactor = 2,
                AdminCertificate = hubCertificates.ServerCertificate.Value,
                ClientCertificate = hubCertificates.ServerCertificate.Value,
                ModifyDatabaseName = s => dbNameA,
                CreateDatabase = true
            });

            using var sink = GetDocumentStore(new Options
            {
                AdminCertificate = hubCertificates.ServerCertificate.Value,
                ClientCertificate = hubCertificates.ServerCertificate.Value,
                ModifyDatabaseName = s => dbNameB
            });

            var sinkDatabaseId = (await GetDatabase(Server, sink.Database)).DbBase64Id;
            var hubDatabaseIds = await Task.WhenAll(hubNodes.Select(async node => (await GetDatabase(node, hub.Database)).DbBase64Id));

            const string usersDocId1 = "users/1";

            using (var session = hub.OpenAsyncSession())
            {
                var user = new User { Name = "Grisha" };
                await session.StoreAsync(user, usersDocId1);
                await session.SaveChangesAsync();

                var changeVector = session.Advanced.GetChangeVectorFor(user);
                Assert.True(hubDatabaseIds.Any(id => changeVector.Contains(id)));
            }

            var pullCert = CertificateHelper.CreateCertificateFromPfx(File.ReadAllBytes(hubCertificates.ClientCertificate2Path), (string)null,
                X509KeyStorageFlags.Exportable);

            await hub.Maintenance.SendAsync(new PutPullReplicationAsHubOperation(new PullReplicationDefinition
            {
                Name = "both",
                Mode = PullReplicationMode.SinkToHub | PullReplicationMode.HubToSink,
                WithFiltering = true
            }));

            await hub.Maintenance.SendAsync(new RegisterReplicationHubAccessOperation("both", new ReplicationHubAccess
            {
                Name = "Arava",
                CertificateBase64 = Convert.ToBase64String(pullCert.Export(X509ContentType.Cert)),
                AllowedHubToSinkPaths = new[] { "*" },
                AllowedSinkToHubPaths = new[] { "*" }
            }));

            await sink.Maintenance.SendAsync(new PutConnectionStringOperation<RavenConnectionString>(new RavenConnectionString
            {
                Database = dbNameA,
                Name = dbNameA + "ConStr",
                TopologyDiscoveryUrls = hub.Urls
            }));
            await sink.Maintenance.SendAsync(new UpdatePullReplicationAsSinkOperation(new PullReplicationAsSink
            {
                ConnectionStringName = dbNameA + "ConStr",
                Mode = PullReplicationMode.SinkToHub | PullReplicationMode.HubToSink,
                CertificateWithPrivateKey = Convert.ToBase64String(pullCert.Export(X509ContentType.Pfx)),
                HubName = "both",
                AllowedHubToSinkPaths = new[] { "*" },
                AllowedSinkToHubPaths = new[] { "*" }
            }));

            WaitForDocument(sink, usersDocId1);

            const int age = 38;
            using (var session = sink.OpenAsyncSession())
            {
                var user = await session.LoadAsync<User>(usersDocId1);
                Assert.NotNull(user);

                var changeVector = session.Advanced.GetChangeVectorFor(user);
                Assert.False(changeVector.Contains(ChangeVectorParser.SinkTag));
                Assert.True(hubDatabaseIds.Any(id => changeVector.Contains(id)));

                var stats = await hub.Maintenance.SendAsync(new GetStatisticsOperation());
                Assert.False(stats.DatabaseChangeVector.Contains(ChangeVectorParser.SinkTag));

                user.Age = age;
                await session.SaveChangesAsync();
                changeVector = session.Advanced.GetChangeVectorFor(user);
                Assert.False(changeVector.Contains(ChangeVectorParser.SinkTag));
                Assert.True(hubDatabaseIds.Any(id => changeVector.Contains(id)));
                Assert.True(changeVector.Contains(sinkDatabaseId));

                stats = await hub.Maintenance.SendAsync(new GetStatisticsOperation());
                Assert.False(stats.DatabaseChangeVector.Contains(ChangeVectorParser.SinkTag));
            }

            var ageVal = await WaitForValueAsync(async () =>
            {
                using (var session = hub.OpenAsyncSession())
                {
                    var user = await session.LoadAsync<User>(usersDocId1);
                    return user.Age;
                }
            }, age);
            Assert.Equal(age, ageVal);

            using (var session = hub.OpenAsyncSession())
            {
                var user = await session.LoadAsync<User>(usersDocId1);

                var changeVector = session.Advanced.GetChangeVectorFor(user);
                Assert.True(changeVector.Contains(ChangeVectorParser.SinkTag));
                Assert.True(hubDatabaseIds.Any(id => changeVector.Contains(id)));
                Assert.True(changeVector.Contains(sinkDatabaseId));

                var stats = await hub.Maintenance.SendAsync(new GetStatisticsOperation());
                Assert.False(stats.DatabaseChangeVector.Contains(ChangeVectorParser.SinkTag));
            }
        }

        [RavenFact(RavenTestCategory.ClusterTransactions | RavenTestCategory.Replication)]
        public async Task Can_pull_and_push_with_first_cluster_transactions_on_hub()
        {
            var dbNameA = GetDatabaseName();
            var dbNameB = GetDatabaseName();

            var (_, hubLeader, hubCertificates) = await CreateRaftClusterWithSsl(2, watcherCluster: true);
            using var hub = GetDocumentStore(new Options
            {
                Server = hubLeader,
                ReplicationFactor = 2,
                AdminCertificate = hubCertificates.ServerCertificate.Value,
                ClientCertificate = hubCertificates.ServerCertificate.Value,
                ModifyDatabaseName = s => dbNameA,
                CreateDatabase = true
            });

            using var sink = GetDocumentStore(new Options
            {
                AdminCertificate = hubCertificates.ServerCertificate.Value,
                ClientCertificate = hubCertificates.ServerCertificate.Value,
                ModifyDatabaseName = s => dbNameB
            });

            var hubRecord = await hub.Maintenance.Server.SendAsync(new GetDatabaseRecordOperation(hub.Database));
            var hubClusterId = hubRecord.Topology.ClusterTransactionIdBase64;
            var sinkDatabaseId = (await GetDatabase(Server, sink.Database)).DbBase64Id;

            const string usersDocId1 = "users/1";

            using (var session = hub.OpenAsyncSession())
            {
                var user = new User { Name = "Grisha" };
                session.Advanced.SetTransactionMode(TransactionMode.ClusterWide);
                await session.StoreAsync(user, usersDocId1);
                await session.SaveChangesAsync();

                var changeVector = session.Advanced.GetChangeVectorFor(user);
                Assert.True(changeVector.Contains(ChangeVectorParser.TrxnTag));
                Assert.True(changeVector.Contains(hubClusterId));
            }

            var pullCert = CertificateHelper.CreateCertificateFromPfx(File.ReadAllBytes(hubCertificates.ClientCertificate2Path), (string)null,
                X509KeyStorageFlags.Exportable);

            await hub.Maintenance.SendAsync(new PutPullReplicationAsHubOperation(new PullReplicationDefinition
            {
                Name = "both",
                Mode = PullReplicationMode.SinkToHub | PullReplicationMode.HubToSink,
                WithFiltering = true
            }));

            await hub.Maintenance.SendAsync(new RegisterReplicationHubAccessOperation("both", new ReplicationHubAccess
            {
                Name = "Arava",
                CertificateBase64 = Convert.ToBase64String(pullCert.Export(X509ContentType.Cert)),
                AllowedHubToSinkPaths = new[] { "*" },
                AllowedSinkToHubPaths = new[] { "*" }
            }));

            await sink.Maintenance.SendAsync(new PutConnectionStringOperation<RavenConnectionString>(new RavenConnectionString
            {
                Database = dbNameA,
                Name = dbNameA + "ConStr",
                TopologyDiscoveryUrls = hub.Urls
            }));
            await sink.Maintenance.SendAsync(new UpdatePullReplicationAsSinkOperation(new PullReplicationAsSink
            {
                ConnectionStringName = dbNameA + "ConStr",
                Mode = PullReplicationMode.SinkToHub | PullReplicationMode.HubToSink,
                CertificateWithPrivateKey = Convert.ToBase64String(pullCert.Export(X509ContentType.Pfx)),
                HubName = "both",
                AllowedHubToSinkPaths = new[] { "*" },
                AllowedSinkToHubPaths = new[] { "*" }
            }));

            WaitForDocument(sink, usersDocId1);

            const int age = 38;
            using (var session = sink.OpenAsyncSession())
            {
                var user = await session.LoadAsync<User>(usersDocId1);
                Assert.NotNull(user);

                var changeVector = session.Advanced.GetChangeVectorFor(user);
                Assert.True(changeVector.Contains(ChangeVectorParser.TrxnTag));
                Assert.True(changeVector.Contains(hubClusterId));

                await VerifyDatabaseChangeVector(sink);

                user.Age = age;
                await session.SaveChangesAsync();
                changeVector = session.Advanced.GetChangeVectorFor(user);
                Assert.True(changeVector.Contains(ChangeVectorParser.TrxnTag));
                Assert.True(changeVector.Contains(hubClusterId));
                Assert.True(changeVector.Contains(sinkDatabaseId));

                await VerifyDatabaseChangeVector(sink);
            }

            var ageVal = await WaitForValueAsync(async () =>
            {
                using (var session = hub.OpenAsyncSession())
                {
                    var user = await session.LoadAsync<User>(usersDocId1);
                    return user.Age;
                }
            }, age);
            Assert.Equal(age, ageVal);

            using (var session = hub.OpenAsyncSession())
            {
                var user = await session.LoadAsync<User>(usersDocId1);

                var changeVector = session.Advanced.GetChangeVectorFor(user);
                Assert.True(changeVector.Contains(ChangeVectorParser.TrxnTag));
                Assert.True(changeVector.Contains(ChangeVectorParser.RaftTag));
                Assert.True(changeVector.Contains(ChangeVectorParser.SinkTag));
                Assert.True(changeVector.Contains(hubClusterId));
                Assert.True(changeVector.Contains(sinkDatabaseId));

                await VerifyDatabaseChangeVector(hub);
            }

            using (var session = hub.OpenAsyncSession())
            {
                const string anotherUserId = "users/2";
                var user = new User();
                await session.StoreAsync(user, anotherUserId);
                await session.SaveChangesAsync();

                user.Name = "Grisha";
                await session.SaveChangesAsync();
            }

            async Task VerifyDatabaseChangeVector(DocumentStore store)
            {
                var stats = await store.Maintenance.SendAsync(new GetStatisticsOperation());
                Assert.False(stats.DatabaseChangeVector.Contains(ChangeVectorParser.TrxnTag));
                Assert.False(stats.DatabaseChangeVector.Contains(ChangeVectorParser.SinkTag));
                Assert.False(stats.DatabaseChangeVector.Contains(hubClusterId));
            }
        }
    }
}
