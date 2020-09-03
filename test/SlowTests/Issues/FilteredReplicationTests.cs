using System;
using System.Collections.Generic;
using System.IO;
using System.Security.Cryptography.X509Certificates;
using System.Text;
using System.Threading.Tasks;
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
using Raven.Server.ServerWide.Context;
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

            WaitForUserToContinueTheTest(storeA, clientCert: certificates.ServerCertificate.Value);

            WaitForDocument(storeB, "users/ayende");
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
