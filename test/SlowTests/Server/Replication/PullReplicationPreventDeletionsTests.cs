using System;
using System.Collections.Generic;
using System.IO;
using System.Linq;
using System.Security.Cryptography.X509Certificates;
using System.Threading.Tasks;
using FastTests;
using FastTests.Utils;
using Raven.Client;
using Raven.Client.Documents;
using Raven.Client.Documents.Indexes;
using Raven.Client.Documents.Operations.ConnectionStrings;
using Raven.Client.Documents.Operations.ETL;
using Raven.Client.Documents.Operations.Expiration;
using Raven.Client.Documents.Operations.Replication;
using Raven.Client.Documents.Session;
using Raven.Client.ServerWide.Operations.Certificates;
using Raven.Client.Util;
using Raven.Server.Config;
using Raven.Server.Documents.Replication.Incoming;
using Tests.Infrastructure;
using Xunit;
using Xunit.Abstractions;

namespace SlowTests.Server.Replication
{
    public class PullReplicationPreventDeletionsTests : ClusterTestBase
    {
        public PullReplicationPreventDeletionsTests(ITestOutputHelper output) : base(output)
        {
        }

        [RavenFact(RavenTestCategory.Replication)]
        public async Task PreventDeletionsOnHubForAttachments()
        {
            var certificates = Certificates.SetupServerAuthentication();
            var adminCert = Certificates.RegisterClientCertificate(certificates.ServerCertificate.Value, certificates
                .ClientCertificate1.Value, new Dictionary<string, DatabaseAccess>(), SecurityClearance.ClusterAdmin);

            var hubDatabase = GetDatabaseName("HUB");
            var sinkDatabase = GetDatabaseName("SINK");

            using var hubStore = GetDocumentStore(new RavenTestBase.Options
            {
                AdminCertificate = adminCert,
                ClientCertificate = adminCert,
                ModifyDatabaseName = x => hubDatabase
            });

            using var sinkStore = GetDocumentStore(new RavenTestBase.Options
            {
                AdminCertificate = adminCert,
                ClientCertificate = adminCert,
                ModifyDatabaseName = x => sinkDatabase
            });

            //setup expiration
            await SetupExpiration(sinkStore);

            var pullCert = CertificateHelper.CreateCertificate(File.ReadAllBytes(certificates.ClientCertificate2Path), (string)null,
                X509KeyStorageFlags.Exportable);

            await hubStore.Maintenance.SendAsync(new PutPullReplicationAsHubOperation(new PullReplicationDefinition
            {
                Name = "pullRepHub",
                Mode = PullReplicationMode.SinkToHub | PullReplicationMode.HubToSink,
                PreventDeletionsMode = PreventDeletionsMode.PreventSinkToHubDeletions
            }));

            await hubStore.Maintenance.SendAsync(new RegisterReplicationHubAccessOperation("pullRepHub",
                new ReplicationHubAccess { Name = "hubAccess", CertificateBase64 = Convert.ToBase64String(pullCert.Export(X509ContentType.Cert)) }));

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
                HubName = "pullRepHub"
            }));

            var sinkDatabaseInstance = await Databases.GetDocumentDatabaseInstanceFor(sinkStore);
            sinkDatabaseInstance.ForTestingPurposes = null;

            using (var s = sinkStore.OpenAsyncSession())
            {
                dynamic user1 = new { Source = "Sink" };
                dynamic user3 = new { Source = "Sink" };
                using (var attachment = new MemoryStream(new byte[]{1,2,3,4}))
                using (var attachment2 = new MemoryStream(new byte[]{2,32,3,4}))
                {
                    await s.StoreAsync(user1, "users/insink/1");
                    s.Advanced.Attachments.Store(user1, "foo", attachment);
                    s.Advanced.GetMetadataFor(user1)[Constants.Documents.Metadata.Expires] = DateTime.UtcNow.AddMinutes(10);

                    dynamic user2 = new { Source = "Sink" };
                    await s.StoreAsync(user2, "users/insink/2");
                    s.Advanced.GetMetadataFor(user2)[Constants.Documents.Metadata.Expires] = DateTime.UtcNow.AddMinutes(10);

                    await s.StoreAsync(user3, "users/insink/3");
                    s.Advanced.Attachments.Store(user3, "bar", attachment2);

                    await s.SaveChangesAsync();
                }
            }

            using (var s = hubStore.OpenAsyncSession())
            {
                await s.StoreAsync(new { Source = "Hub" }, "users/inhub/1");
                await s.SaveChangesAsync();
            }

            Assert.True(WaitForDocument(sinkStore, "users/inhub/1"));
            
            await EnsureReplicatingAsync(sinkStore, hubStore);

            using (var s = sinkStore.OpenAsyncSession())
            {
                s.Delete("users/insink/3");
                await s.SaveChangesAsync();
            }

            //make sure hub got both docs and expires gets deleted
            using (var h = hubStore.OpenAsyncSession())
            {
                //check hub got both docs
                var doc1 = await h.LoadAsync<dynamic>("users/insink/1");
                Assert.NotNull(doc1);

                var doc2 = await h.LoadAsync<dynamic>("users/insink/2");
                Assert.NotNull(doc2);

                //check expired does not exist in users/insink/1
                IMetadataDictionary metadata = h.Advanced.GetMetadataFor(doc1);
                Assert.False(metadata?.ContainsKey(Constants.Documents.Metadata.Expires));

                //check expired does not exist in users/insink/2
                metadata = h.Advanced.GetMetadataFor(doc2);
                Assert.False(metadata?.ContainsKey(Constants.Documents.Metadata.Expires));
            }

            //delete doc from sink
            using (var s = sinkStore.OpenAsyncSession())
            {
                s.Delete("users/insink/1");
                await s.SaveChangesAsync();
            }

            EnsureReplicating(hubStore, sinkStore);
            EnsureReplicating(sinkStore, hubStore);

            //make sure doc is deleted from sink
            Assert.True(WaitForDocumentDeletion(sinkStore, "users/insink/1"));

            //make sure doc not deleted from hub and still doesn't contain expires
            using (var h = hubStore.OpenAsyncSession())
            {
                //check hub got doc
                var doc1 = await h.LoadAsync<dynamic>("users/insink/1");
                Assert.NotNull(doc1);

                //check expires does not exist in users/insink/1
                IMetadataDictionary metadata = h.Advanced.GetMetadataFor(doc1);
                Assert.False(metadata?.ContainsKey(Constants.Documents.Metadata.Expires));
            }
        }

        [RavenFact(RavenTestCategory.Replication)]
        public async Task PreventDeletionOnHubSinkCompromised()
        {
            var certificates = Certificates.SetupServerAuthentication();
            var adminCert = Certificates.RegisterClientCertificate(certificates.ServerCertificate.Value, certificates
                .ClientCertificate1.Value, new Dictionary<string, DatabaseAccess>(), SecurityClearance.ClusterAdmin);

            var hubDatabase = GetDatabaseName("HUB");
            var sinkDatabase = GetDatabaseName("SINK");

            using var hubStore = GetDocumentStore(new RavenTestBase.Options
            {
                AdminCertificate = adminCert,
                ClientCertificate = adminCert,
                ModifyDatabaseName = x => hubDatabase
            });

            using var sinkStore = GetDocumentStore(new RavenTestBase.Options
            {
                AdminCertificate = adminCert,
                ClientCertificate = adminCert,
                ModifyDatabaseName = x => sinkDatabase
            });

            var pullCert = CertificateHelper.CreateCertificate(File.ReadAllBytes(certificates.ClientCertificate2Path), (string)null,
                X509KeyStorageFlags.Exportable);

            await hubStore.Maintenance.SendAsync(new PutPullReplicationAsHubOperation(new PullReplicationDefinition
            {
                Name = "pullRepHub",
                Mode = PullReplicationMode.SinkToHub | PullReplicationMode.HubToSink,
                PreventDeletionsMode = PreventDeletionsMode.PreventSinkToHubDeletions
            }));

            await hubStore.Maintenance.SendAsync(new RegisterReplicationHubAccessOperation("pullRepHub",
                new ReplicationHubAccess { Name = "hubAccess1", CertificateBase64 = Convert.ToBase64String(pullCert.Export(X509ContentType.Cert)) }));

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
                HubName = "pullRepHub"
            }));

            using (var s = sinkStore.OpenAsyncSession())
            {

                dynamic user1 = new { Source = "Sink" };
                await s.StoreAsync(user1, "users/insink/1");

                dynamic user2 = new { Source = "Sink" };
                await s.StoreAsync(user2, "users/insink/2");

                await s.SaveChangesAsync();
            }

            using (var s = hubStore.OpenAsyncSession())
            {
                await s.StoreAsync(new { Source = "Hub" }, "users/inhub/1");
                await s.SaveChangesAsync();
            }

            var sinkDatabaseInstance = await Databases.GetDocumentDatabaseInstanceFor(sinkStore);
            sinkDatabaseInstance.ForTestingPurposesOnly().ForceSendTombstones = true;

            await EnsureReplicatingAsync(hubStore, sinkStore);
            await EnsureReplicatingAsync(sinkStore, hubStore);

            using (var h = hubStore.OpenAsyncSession())
            {
                //check hub got both docs
                var doc1 = await h.LoadAsync<dynamic>("users/insink/1");
                Assert.NotNull(doc1);

                var doc2 = await h.LoadAsync<dynamic>("users/insink/2");
                Assert.NotNull(doc2);
            }

            Assert.True(WaitForDocument(sinkStore, "users/inhub/1"));

            var hubDatabaseInstance = await Databases.GetDocumentDatabaseInstanceFor(hubStore);
            bool expectedError = false;
            string lastError = null;

            Exception lastException = null;
            var incomingHandlers = hubDatabaseInstance.ReplicationLoader.IncomingHandlers.Cast<IncomingPullReplicationHandler>().ToArray();
            var incomingStr = string.Join(",", incomingHandlers.Select(x => x.GetType()));
            Assert.True(incomingHandlers.Length == 1, $"hub should have 1 incoming handler but has {incomingHandlers.Length}, {incomingStr}");

            hubDatabaseInstance.ReplicationLoader.ForTestingPurposesOnly().OnIncomingReplicationHandlerFailure += (exception) =>
            {
                if (exception.Message.Contains("This hub does not allow for tombstone replication via pull replication"))
                {
                    expectedError = true;
                }

                lastError = exception.Message;
                lastException = exception;
            };

            //delete doc from sink
            using (var s = sinkStore.OpenAsyncSession())
            {
                s.Delete("users/insink/1");
                await s.SaveChangesAsync();
            }

            //make sure doc deleted from sink
            Assert.True(WaitForDocumentDeletion(sinkStore, "users/insink/1"));

            //make sure doc not deleted from hub
            using (var h = hubStore.OpenAsyncSession())
            {
                //check hub got doc
                var doc = await h.LoadAsync<dynamic>("users/insink/1");
                Assert.NotNull(doc);
            }

            //make sure hub threw error
            var result = await WaitForValueAsync(() => Task.FromResult(expectedError), true);
            if (result == false)
            {
                Assert.NotNull(lastException);
                Assert.Fail($"The actual exception: {lastException}");
            }
        }

        [RavenFact(RavenTestCategory.Replication)]
        public async Task DeleteWhenAcceptSinkDeletionsFlagOff()
        {
            var certificates = Certificates.SetupServerAuthentication();
            var adminCert = Certificates.RegisterClientCertificate(certificates.ServerCertificate.Value, certificates
                .ClientCertificate1.Value, new Dictionary<string, DatabaseAccess>(), SecurityClearance.ClusterAdmin);

            var hubDatabase = GetDatabaseName("HUB");
            var sinkDatabase = GetDatabaseName("SINK");

            using var hubStore = GetDocumentStore(new RavenTestBase.Options
            {
                AdminCertificate = adminCert,
                ClientCertificate = adminCert,
                ModifyDatabaseName = x => hubDatabase
            });

            using var sinkStore = GetDocumentStore(new RavenTestBase.Options
            {
                AdminCertificate = adminCert,
                ClientCertificate = adminCert,
                ModifyDatabaseName = x => sinkDatabase
            });

            //setup expiration
            await SetupExpiration(sinkStore);

            var pullCert = CertificateHelper.CreateCertificate(File.ReadAllBytes(certificates.ClientCertificate2Path), (string)null,
                X509KeyStorageFlags.Exportable);

            await hubStore.Maintenance.SendAsync(new PutPullReplicationAsHubOperation(new PullReplicationDefinition
            {
                Name = "pullRepHub",
                Mode = PullReplicationMode.SinkToHub | PullReplicationMode.HubToSink
            }));

            await hubStore.Maintenance.SendAsync(new RegisterReplicationHubAccessOperation("pullRepHub",
                new ReplicationHubAccess { Name = "hubAccess", CertificateBase64 = Convert.ToBase64String(pullCert.Export(X509ContentType.Cert)) }));

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
                HubName = "pullRepHub"
            }));

            using (var s = sinkStore.OpenAsyncSession())
            {
                dynamic user1 = new { Source = "Sink" };
                await s.StoreAsync(user1, "users/insink/1");
                s.Advanced.GetMetadataFor(user1)[Constants.Documents.Metadata.Expires] = DateTime.UtcNow.AddMinutes(10);

                dynamic user2 = new { Source = "Sink" };
                await s.StoreAsync(user2, "users/insink/2");
                s.Advanced.GetMetadataFor(user2)[Constants.Documents.Metadata.Expires] = DateTime.UtcNow.AddMinutes(10);

                await s.SaveChangesAsync();
            }

            using (var s = hubStore.OpenAsyncSession())
            {
                await s.StoreAsync(new { Source = "Hub" }, "users/inhub/1");
                await s.SaveChangesAsync();
            }

            Assert.True(WaitForDocument(sinkStore, "users/inhub/1"));

            //make sure hub got both docs and expires doesn't get deleted
            using (var h = hubStore.OpenAsyncSession())
            {
                //check hub got both docs
                var doc1 = await h.LoadAsync<dynamic>("users/insink/1");
                Assert.NotNull(doc1);

                var doc2 = await h.LoadAsync<dynamic>("users/insink/2");
                Assert.NotNull(doc2);

                //check expired exists in users/insink/1
                var metadata = h.Advanced.GetMetadataFor(doc1);
                var expires = metadata[Constants.Documents.Metadata.Expires];
                Assert.NotNull(expires);

                //check expired exists in users/insink/2
                metadata = h.Advanced.GetMetadataFor(doc2);
                expires = metadata[Constants.Documents.Metadata.Expires];
                Assert.NotNull(expires);
            }

            //delete doc from sink
            using (var s = sinkStore.OpenAsyncSession())
            {
                s.Delete("users/insink/1");
                await s.SaveChangesAsync();
            }

            EnsureReplicating(hubStore, sinkStore);
            EnsureReplicating(sinkStore, hubStore);

            //make sure doc is deleted from hub and sink both
            Assert.True(WaitForDocumentDeletion(hubStore, "users/insink/1"));
            Assert.True(WaitForDocumentDeletion(sinkStore, "users/insink/1"));
        }

        [RavenFact(RavenTestCategory.Replication)]
        public async Task PreventDeletionsOnHub()
        {
            var certificates = Certificates.SetupServerAuthentication();
            var adminCert = Certificates.RegisterClientCertificate(certificates.ServerCertificate.Value, certificates
                .ClientCertificate1.Value, new Dictionary<string, DatabaseAccess>(), SecurityClearance.ClusterAdmin);

            var hubDatabase = GetDatabaseName("HUB");
            var sinkDatabase = GetDatabaseName("SINK");

            using var hubStore = GetDocumentStore(new RavenTestBase.Options
            {
                AdminCertificate = adminCert,
                ClientCertificate = adminCert,
                ModifyDatabaseName = x => hubDatabase
            });

            using var sinkStore = GetDocumentStore(new RavenTestBase.Options
            {
                AdminCertificate = adminCert,
                ClientCertificate = adminCert,
                ModifyDatabaseName = x => sinkDatabase
            });

            //setup expiration
            await SetupExpiration(sinkStore);

            var pullCert = CertificateHelper.CreateCertificate(File.ReadAllBytes(certificates.ClientCertificate2Path), (string)null,
                X509KeyStorageFlags.Exportable);

            await hubStore.Maintenance.SendAsync(new PutPullReplicationAsHubOperation(new PullReplicationDefinition
            {
                Name = "pullRepHub",
                Mode = PullReplicationMode.SinkToHub | PullReplicationMode.HubToSink,
                PreventDeletionsMode = PreventDeletionsMode.PreventSinkToHubDeletions
            }));

            await hubStore.Maintenance.SendAsync(new RegisterReplicationHubAccessOperation("pullRepHub",
                new ReplicationHubAccess { Name = "hubAccess", CertificateBase64 = Convert.ToBase64String(pullCert.Export(X509ContentType.Cert)) }));

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
                HubName = "pullRepHub"
            }));

            using (var s = sinkStore.OpenAsyncSession())
            {
                dynamic user1 = new { Source = "Sink" };
                await s.StoreAsync(user1, "users/insink/1");
                s.Advanced.GetMetadataFor(user1)[Constants.Documents.Metadata.Expires] = DateTime.UtcNow.AddMinutes(10);

                dynamic user2 = new { Source = "Sink" };
                await s.StoreAsync(user2, "users/insink/2");
                s.Advanced.GetMetadataFor(user2)[Constants.Documents.Metadata.Expires] = DateTime.UtcNow.AddMinutes(10);

                await s.SaveChangesAsync();
            }

            using (var s = hubStore.OpenAsyncSession())
            {
                await s.StoreAsync(new { Source = "Hub" }, "users/inhub/1");
                await s.SaveChangesAsync();
            }

            Assert.True(WaitForDocument(sinkStore, "users/inhub/1"));

            //make sure hub got both docs and expires gets deleted
            using (var h = hubStore.OpenAsyncSession())
            {
                //check hub got both docs
                var doc1 = await h.LoadAsync<dynamic>("users/insink/1");
                Assert.NotNull(doc1);

                var doc2 = await h.LoadAsync<dynamic>("users/insink/2");
                Assert.NotNull(doc2);

                //check expired does not exist in users/insink/1
                IMetadataDictionary metadata = h.Advanced.GetMetadataFor(doc1);
                Assert.False(metadata?.ContainsKey(Constants.Documents.Metadata.Expires));

                //check expired does not exist in users/insink/2
                metadata = h.Advanced.GetMetadataFor(doc2);
                Assert.False(metadata?.ContainsKey(Constants.Documents.Metadata.Expires));
            }

            //delete doc from sink
            using (var s = sinkStore.OpenAsyncSession())
            {
                s.Delete("users/insink/1");
                await s.SaveChangesAsync();
            }

            EnsureReplicating(hubStore, sinkStore);
            EnsureReplicating(sinkStore, hubStore);

            //make sure doc is deleted from sink
            Assert.True(WaitForDocumentDeletion(sinkStore, "users/insink/1"));

            //make sure doc not deleted from hub and still doesn't contain expires
            using (var h = hubStore.OpenAsyncSession())
            {
                //check hub got doc
                var doc1 = await h.LoadAsync<dynamic>("users/insink/1");
                Assert.NotNull(doc1);

                //check expires does not exist in users/insink/1
                IMetadataDictionary metadata = h.Advanced.GetMetadataFor(doc1);
                Assert.False(metadata?.ContainsKey(Constants.Documents.Metadata.Expires));
            }
        }

        [RavenTheory(RavenTestCategory.Replication)]
        [RavenExplicitData(SearchEngineMode = RavenSearchEngineMode.Lucene)]
        public async Task EnsureArtificialDocsAreSkippedOnReplication_17795(RavenTestParameters config)
        {
            var certificates = Certificates.SetupServerAuthentication();
            var adminCert = Certificates.RegisterClientCertificate(certificates.ServerCertificate.Value, certificates
                .ClientCertificate1.Value, new Dictionary<string, DatabaseAccess>(), SecurityClearance.ClusterAdmin);

            var hubDatabase = GetDatabaseName("HUB");
            var sinkDatabase = GetDatabaseName("SINK");

            using var hubStore = GetDocumentStore(new RavenTestBase.Options
            {
                AdminCertificate = adminCert,
                ClientCertificate = adminCert,
                ModifyDatabaseName = x => hubDatabase,
                ModifyDatabaseRecord = record =>
                {
                    record.Settings[RavenConfiguration.GetKey(x => x.Indexing.AutoIndexingEngineType)] = config.SearchEngine.ToString();
                    record.Settings[RavenConfiguration.GetKey(x => x.Indexing.StaticIndexingEngineType)] = config.SearchEngine.ToString();
                }
            });

            using var sinkStore = GetDocumentStore(new RavenTestBase.Options
            {
                AdminCertificate = adminCert,
                ClientCertificate = adminCert,
                ModifyDatabaseName = x => sinkDatabase,
                ModifyDatabaseRecord = record =>
                {
                    record.Settings[RavenConfiguration.GetKey(x => x.Indexing.AutoIndexingEngineType)] = config.SearchEngine.ToString();
                    record.Settings[RavenConfiguration.GetKey(x => x.Indexing.StaticIndexingEngineType)] = config.SearchEngine.ToString();
                }
            });

            //create artificial doc in sink
            var artificialId = "";

            await new Users_ByName_Count().ExecuteAsync(sinkStore);

            using (var s = sinkStore.OpenAsyncSession())
            {
                User user1 = new() { Name = "stav", Source = "Sink" };
                await s.StoreAsync(user1, "users/insink/1");

                User user2 = new() { Name = "stav", Source = "Sink" };
                await s.StoreAsync(user2, "users/insink/2");

                await s.SaveChangesAsync();
            }

            Indexes.WaitForIndexing(sinkStore);

            using (var s = sinkStore.OpenAsyncSession())
            {
                var artificialDoc = await s.Advanced.AsyncRawQuery<dynamic>("from UserNames").FirstAsync();
                artificialId = s.Advanced.GetMetadataFor(artificialDoc)[Constants.Documents.Metadata.Id];
            }

            //setup pull replication
            var pullCert = CertificateHelper.CreateCertificate(File.ReadAllBytes(certificates.ClientCertificate2Path), (string)null,
                X509KeyStorageFlags.Exportable);

            await hubStore.Maintenance.SendAsync(new PutPullReplicationAsHubOperation(new PullReplicationDefinition
            {
                Name = "pullRepHub",
                Mode = PullReplicationMode.SinkToHub | PullReplicationMode.HubToSink,
                PreventDeletionsMode = PreventDeletionsMode.PreventSinkToHubDeletions
            }));

            await hubStore.Maintenance.SendAsync(new RegisterReplicationHubAccessOperation("pullRepHub",
                new ReplicationHubAccess { Name = "hubAccess", CertificateBase64 = Convert.ToBase64String(pullCert.Export(X509ContentType.Cert)) }));

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
                HubName = "pullRepHub"
            }));

            //make sure hub got both docs
            Assert.True(WaitForDocument(hubStore, "users/insink/1"));
            Assert.True(WaitForDocument(hubStore, "users/insink/2"));

            EnsureReplicating(hubStore, sinkStore);

            //make sure artificial wasn't replicated to hub
            using (var h = hubStore.OpenAsyncSession())
            {
                var artificial = await h.LoadAsync<dynamic>(artificialId);
                Assert.Null(artificial);
            }
        }

        private class Users_ByName_Count : AbstractIndexCreationTask<User, Users_ByName_Count.Result>
        {
            public class Result
            {
                public string Name { get; set; }

                public int Count { get; set; }
            }

            public Users_ByName_Count()
            {
                Map = users => from u in users select new { u.Name, Count = 1 };
                Reduce = results => from result in results group result by result.Name into g select new { Name = g.Key, Count = g.Sum(x => x.Count) };
                OutputReduceToCollection = "UserNames";
                PatternForOutputReduceToCollectionReferences = x => $"UserNames/{x.Name}";
            }
        }

        [RavenFact(RavenTestCategory.Replication)]
        public async Task MakeSureDeletionsRevisionsDontReplicate()
        {
            var certificates = Certificates.SetupServerAuthentication();
            var adminCert = Certificates.RegisterClientCertificate(certificates.ServerCertificate.Value, certificates
                .ClientCertificate1.Value, new Dictionary<string, DatabaseAccess>(), SecurityClearance.ClusterAdmin);

            var hubDatabase = GetDatabaseName("HUB");
            var sinkDatabase = GetDatabaseName("SINK");

            using var hubStore = GetDocumentStore(new RavenTestBase.Options
            {
                AdminCertificate = adminCert,
                ClientCertificate = adminCert,
                ModifyDatabaseName = x => hubDatabase
            });

            using var sinkStore = GetDocumentStore(new RavenTestBase.Options
            {
                AdminCertificate = adminCert,
                ClientCertificate = adminCert,
                ModifyDatabaseName = x => sinkDatabase
            });

            //setup expiration
            await SetupExpiration(sinkStore);

            var pullCert = CertificateHelper.CreateCertificate(File.ReadAllBytes(certificates.ClientCertificate2Path), (string)null,
                X509KeyStorageFlags.Exportable);

            await hubStore.Maintenance.SendAsync(new PutPullReplicationAsHubOperation(new PullReplicationDefinition
            {
                Name = "pullRepHub",
                Mode = PullReplicationMode.SinkToHub | PullReplicationMode.HubToSink,
                PreventDeletionsMode = PreventDeletionsMode.PreventSinkToHubDeletions
            }));

            await hubStore.Maintenance.SendAsync(new RegisterReplicationHubAccessOperation("pullRepHub",
                new ReplicationHubAccess { Name = "hubAccess", CertificateBase64 = Convert.ToBase64String(pullCert.Export(X509ContentType.Cert)) }));

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
                HubName = "pullRepHub"
            }));

            //enable revisions
            await RevisionsHelper.SetupRevisionsAsync(sinkStore, modifyConfiguration: r => r.Collections["Users"].PurgeOnDelete = false);

            //create doc in sink
            using (var s = sinkStore.OpenAsyncSession())
            {
                dynamic user1 = new User { Source = "Sink" };
                await s.StoreAsync(user1, "users/insink/1");
                s.Advanced.GetMetadataFor(user1)[Constants.Documents.Metadata.Expires] = DateTime.UtcNow.AddMinutes(10);

                await s.SaveChangesAsync();
            }

            //create revision
            using (var s = sinkStore.OpenAsyncSession())
            {
                var user1 = await s.LoadAsync<User>("users/insink/1");
                user1.Source = "SinkAfterChange";
                await s.SaveChangesAsync();
            }

            //create doc in hub
            using (var s = hubStore.OpenAsyncSession())
            {
                await s.StoreAsync(new { Source = "Hub" }, "users/inhub/1");
                await s.SaveChangesAsync();
            }

            Assert.True(WaitForDocument(sinkStore, "users/inhub/1"));

            //make sure hub got both docs and expires gets deleted
            using (var h = hubStore.OpenAsyncSession())
            {
                //check hub got both docs
                var doc1 = await h.LoadAsync<dynamic>("users/insink/1");
                Assert.NotNull(doc1);

                //check expired does not exist in users/insink/1
                IMetadataDictionary metadata = h.Advanced.GetMetadataFor(doc1);
                Assert.False(metadata?.ContainsKey(Constants.Documents.Metadata.Expires));
            }

            //delete doc from sink
            using (var s = sinkStore.OpenAsyncSession())
            {
                s.Delete("users/insink/1");
                await s.SaveChangesAsync();
            }

            EnsureReplicating(hubStore, sinkStore);
            EnsureReplicating(sinkStore, hubStore);

            //make sure doc is deleted from sink
            Assert.True(WaitForDocumentDeletion(sinkStore, "users/insink/1"));

            //make sure doc not deleted from hub and still doesn't contain expires
            using (var h = hubStore.OpenAsyncSession())
            {
                //check hub got doc
                var doc1 = await h.LoadAsync<dynamic>("users/insink/1");
                Assert.NotNull(doc1);

                //check expires does not exist in users/insink/1
                IMetadataDictionary metadata = h.Advanced.GetMetadataFor(doc1);
                Assert.False(metadata?.ContainsKey(Constants.Documents.Metadata.Expires));
            }
        }

        private async Task SetupExpiration(DocumentStore store)
        {
            var config = new ExpirationConfiguration
            {
                Disabled = false,
                DeleteFrequencyInSec = 200,
            };

            await ExpirationHelper.SetupExpiration(store, Server.ServerStore, config);
        }

        private class User
        {
#pragma warning restore CS0649
            public string Name;
            public string Source;
        }
    }
}
