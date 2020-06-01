using System;
using System.Collections.Generic;
using System.IO;
using System.Security.Cryptography.X509Certificates;
using System.Text;
using System.Threading.Tasks;
using Raven.Client.Documents.Operations.ConnectionStrings;
using Raven.Client.Documents.Operations.ETL;
using Raven.Client.Documents.Operations.Replication;
using Raven.Client.Documents.Session;
using Raven.Client.Documents.Session.TimeSeries;
using Raven.Client.Exceptions;
using Raven.Client.ServerWide.Operations.Certificates;
using Tests.Infrastructure;
using Xunit;
using Xunit.Abstractions;

namespace SlowTests.Issues
{
    public class FilteredReplicationTests : ClusterTestBase
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
                await s.StoreAsync(new {Breed = "German Shepherd"}, "users/ayende/dogs/arava");
                await s.StoreAsync(new {Color = "Gray/White"}, "users/pheobe");
                await s.StoreAsync(new {Name = "Oren"}, "users/ayende");
                s.CountersFor("users/ayende").Increment("test");
                s.CountersFor("users/pheobe").Increment("test");
                s.TimeSeriesFor<HeartRateMeasure>("users/pheobe").Append(DateTime.Today, new HeartRateMeasure
                {
                    HeartRate = 34
                },"test/things/out");
                s.TimeSeriesFor<HeartRateMeasure>("users/ayende").Append(DateTime.Today,  new HeartRateMeasure
                {
                    HeartRate = 55
                },"test/things/out");
                s.Advanced.Attachments.Store("users/ayende", "test.bin", new MemoryStream(Encoding.UTF8.GetBytes("hello")));
                s.Advanced.Attachments.Store("users/pheobe", "test.bin", new MemoryStream(Encoding.UTF8.GetBytes("hello")));
                s.Advanced.Revisions.ForceRevisionCreationFor("users/ayende",ForceRevisionStrategy.None);
                s.Advanced.Revisions.ForceRevisionCreationFor("users/pheobe",ForceRevisionStrategy.None);
                await s.SaveChangesAsync();
            }
      
            using (var s = storeA.OpenAsyncSession())
            {
                await s.StoreAsync(new {Color = "Gray/White 2"}, "users/pheobe");
                await s.StoreAsync(new {Name = "Oren 2"}, "users/ayende");
              
                s.Advanced.Revisions.ForceRevisionCreationFor("users/ayende");
                s.Advanced.Revisions.ForceRevisionCreationFor("users/pheobe");
                await s.SaveChangesAsync();
            }
            
            await storeA.Maintenance.SendAsync(new PutConnectionStringOperation<RavenConnectionString>(new RavenConnectionString
            {
                Database = storeB.Database,
                Name = storeB.Database + "ConStr",
                TopologyDiscoveryUrls =  storeA.Urls
            }));
            await storeA.Maintenance.SendAsync(new UpdateExternalReplicationOperation(new ExternalReplication
            {
                ConnectionStringName =  storeB.Database + "ConStr",
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
            }));
            await storeA.Maintenance.SendAsync(new RegisterReplicationHubAccessOperation("pull",
                new ReplicationHubAccess
                {
                    Name = "Arava",
                    CertificateBas64 = Convert.ToBase64String(pullCert.Export(X509ContentType.Cert)),
                    AllowedReadPaths = new[] {"users/ayende", "users/ayende/*"}
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
            var hubOperation = new PutPullReplicationAsHubOperation(new PullReplicationDefinition
            {
                Name = "pull",
                Certificates = new Dictionary<string, string>
                {
                    [pullCertA.Thumbprint] = Convert.ToBase64String(pullCertA.Export(X509ContentType.Cert)),
                    [pullCertB.Thumbprint] = Convert.ToBase64String(pullCertB.Export(X509ContentType.Cert)),
                },
            });
            await Assert.ThrowsAsync<RavenException>(async () => await storeA.Maintenance.SendAsync(new RegisterReplicationHubAccessOperation("pull",
                new ReplicationHubAccess
                {
                    Name = "Arava",
                    CertificateBas64 = Convert.ToBase64String(pullCertA.Export(X509ContentType.Cert)),
                    AllowedReadPaths = new[] {"users/ayende", "users/ayende/*"}
                })));
        }

        public class HeartRateMeasure
        {
            [TimeSeriesValue(0)] public double HeartRate;
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
                await s.StoreAsync(new {Breed = "German Shepherd"}, "users/ayende/dogs/arava");
                await s.StoreAsync(new {Color = "Gray/White"}, "users/pheobe");
                await s.StoreAsync(new {Name = "Oren"}, "users/ayende");
                s.CountersFor("users/ayende").Increment("test");
                s.CountersFor("users/pheobe").Increment("test");
                s.TimeSeriesFor<HeartRateMeasure>("users/pheobe").Append(DateTime.Today, new HeartRateMeasure
                {
                    HeartRate = 34
                },"test/things/out");
                s.TimeSeriesFor<HeartRateMeasure>("users/ayende").Append(DateTime.Today,  new HeartRateMeasure
                {
                    HeartRate = 55
                },"test/things/out");
                s.Advanced.Attachments.Store("users/ayende", "test.bin", new MemoryStream(Encoding.UTF8.GetBytes("hello")));
                s.Advanced.Attachments.Store("users/pheobe", "test.bin", new MemoryStream(Encoding.UTF8.GetBytes("hello")));
                s.Advanced.Revisions.ForceRevisionCreationFor("users/ayende",ForceRevisionStrategy.None);
                s.Advanced.Revisions.ForceRevisionCreationFor("users/pheobe",ForceRevisionStrategy.None);
                await s.SaveChangesAsync();
            }

            using (var s = storeA.OpenAsyncSession())
            {
                await s.LoadAsync<object>("users/pheobe");
                await s.LoadAsync<object>("users/ayende");
            }
            
            using (var s = storeA.OpenAsyncSession())
            {
                await s.StoreAsync(new {Color = "Gray/White 2"}, "users/pheobe");
                await s.StoreAsync(new {Name = "Oren 2"}, "users/ayende");
              
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
                Mode = PullReplicationMode.Incoming | PullReplicationMode.Outgoing,
            }));
            
            await storeB.Maintenance.SendAsync(new RegisterReplicationHubAccessOperation("pull", new ReplicationHubAccess
            {
                Name = "Arava",
                AllowedReadPaths =  new[]
                {
                    "users/ayende",
                    "users/ayende/*"
                },
                CertificateBas64 =  Convert.ToBase64String(pullCert.Export(X509ContentType.Cert)),
            }));
            
            await storeB.Maintenance.SendAsync(new PutConnectionStringOperation<RavenConnectionString>(new RavenConnectionString
            {
                Database = dbNameA,
                Name = dbNameA + "ConStr",
                TopologyDiscoveryUrls =  storeA.Urls
            }));
            await storeB.Maintenance.SendAsync(new UpdatePullReplicationAsSinkOperation(new PullReplicationAsSink
            {
                ConnectionStringName = dbNameA + "ConStr",
                CertificateWithPrivateKey = Convert.ToBase64String(pullCert.Export(X509ContentType.Pfx)),
                HubDefinitionName = "pull"
            }));

            WaitForDocument(storeB, "users/ayende");

            using (var s = storeB.OpenAsyncSession())
            {
                Assert.Null(await s.LoadAsync<object>("users/pheobe")); 
                Assert.Null(await s.Advanced.Revisions.GetAsync<object>("users/pheobe",DateTime.Today.AddDays(1)));
                Assert.Null(await s.CountersFor("users/pheobe").GetAsync("test"));
                Assert.Null(await s.TimeSeriesFor<HeartRateMeasure>("users/pheobe").GetAsync());
                Assert.Null(await s.Advanced.Attachments.GetAsync("users/pheobe", "test.bin"));
                
                WaitForUserToContinueTheTest(storeA);
                
                Assert.NotNull(await s.LoadAsync<object>("users/ayende/dogs/arava"));
                Assert.NotNull(await s.LoadAsync<object>("users/ayende"));
                Assert.NotNull(await s.Advanced.Revisions.GetAsync<object>("users/ayende",DateTime.Today.AddDays(1)));
                
                Assert.NotNull(await s.Advanced.Revisions.GetAsync<object>("users/ayende",DateTime.Today.AddDays(1)));
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
                
                Assert.NotNull(await s.Advanced.Revisions.GetAsync<object>("users/ayende",DateTime.Today.AddDays(1)));
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
                await s.StoreAsync(new {Breed = "German Shepherd"}, "users/ayende/dogs/arava");
                await s.StoreAsync(new {Color = "Gray/White"}, "users/pheobe");
                await s.StoreAsync(new {Name = "Oren"}, "users/ayende");
                s.CountersFor("users/ayende").Increment("test");
                s.CountersFor("users/pheobe").Increment("test");
                s.TimeSeriesFor<HeartRateMeasure>("users/pheobe").Append(DateTime.Today, new HeartRateMeasure
                {
                    HeartRate = 34
                },"test/things/out");
                s.TimeSeriesFor<HeartRateMeasure>("users/ayende").Append(DateTime.Today,  new HeartRateMeasure
                {
                    HeartRate = 55
                },"test/things/out");
                s.Advanced.Attachments.Store("users/ayende", "test.bin", new MemoryStream(Encoding.UTF8.GetBytes("hello")));
                s.Advanced.Attachments.Store("users/pheobe", "test.bin", new MemoryStream(Encoding.UTF8.GetBytes("hello")));
                s.Advanced.Revisions.ForceRevisionCreationFor("users/ayende",ForceRevisionStrategy.None);
                s.Advanced.Revisions.ForceRevisionCreationFor("users/pheobe",ForceRevisionStrategy.None);
                await s.SaveChangesAsync();
            }

            using (var s = storeA.OpenAsyncSession())
            {
                await s.LoadAsync<object>("users/pheobe");
                await s.LoadAsync<object>("users/ayende");
            }
            
            using (var s = storeA.OpenAsyncSession())
            {
                await s.StoreAsync(new {Color = "Gray/White 2"}, "users/pheobe");
                await s.StoreAsync(new {Name = "Oren 2"}, "users/ayende");
              
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
                Name = "push",
                Mode = PullReplicationMode.Incoming | PullReplicationMode.Outgoing,
            }));
            
            await storeB.Maintenance.SendAsync(new RegisterReplicationHubAccessOperation("push", new ReplicationHubAccess
            {
                Name = "Arava",
                AllowedWritePaths =  new[]
                {
                    "users/ayende",
                    "users/ayende/*"
                },
                CertificateBas64 =  Convert.ToBase64String(pullCert.Export(X509ContentType.Cert)),
            }));
            
            await storeA.Maintenance.SendAsync(new PutConnectionStringOperation<RavenConnectionString>(new RavenConnectionString
            {
                Database = dbNameB,
                Name = dbNameB + "ConStr",
                TopologyDiscoveryUrls =  storeA.Urls
            }));
            await storeA.Maintenance.SendAsync(new UpdatePullReplicationAsSinkOperation(new PullReplicationAsSink
            {
                ConnectionStringName = dbNameB + "ConStr",
                Mode = PullReplicationMode.Incoming,
                CertificateWithPrivateKey = Convert.ToBase64String(pullCert.Export(X509ContentType.Pfx)),
                HubDefinitionName = "push"
            }));

            WaitForDocument(storeB, "users/ayende", timeout: 10000);

            using (var s = storeB.OpenAsyncSession())
            {
                Assert.Null(await s.LoadAsync<object>("users/pheobe")); 
                Assert.Null(await s.Advanced.Revisions.GetAsync<object>("users/pheobe",DateTime.Today.AddDays(1)));
                Assert.Null(await s.CountersFor("users/pheobe").GetAsync("test"));
                Assert.Null(await s.TimeSeriesFor<HeartRateMeasure>("users/pheobe").GetAsync());
                Assert.Null(await s.Advanced.Attachments.GetAsync("users/pheobe", "test.bin"));
                
                Assert.NotNull(await s.LoadAsync<object>("users/ayende/dogs/arava"));
                Assert.NotNull(await s.LoadAsync<object>("users/ayende"));
                Assert.NotNull(await s.Advanced.Revisions.GetAsync<object>("users/ayende",DateTime.Today.AddDays(1)));
                
                Assert.NotNull(await s.Advanced.Revisions.GetAsync<object>("users/ayende",DateTime.Today.AddDays(1)));
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
                
                Assert.NotNull(await s.Advanced.Revisions.GetAsync<object>("users/ayende",DateTime.Today.AddDays(1)));
                Assert.NotNull(await s.CountersFor("users/ayende").GetAsync("test"));
                Assert.NotEmpty(await s.TimeSeriesFor<HeartRateMeasure>("users/ayende").GetAsync());
                Assert.NotNull(await s.Advanced.Attachments.GetAsync("users/ayende", "test.bin"));
            }
        }
    }
}
