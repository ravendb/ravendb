using System;
using System.Collections.Generic;
using System.IO;
using System.Linq;
using System.Security.Cryptography.X509Certificates;
using System.Threading.Tasks;
using Raven.Client.Documents.Operations.Attachments;
using Raven.Client.Documents.Operations.Replication;
using Raven.Client.Documents.Operations.Revisions;
using Raven.Client.ServerWide.Operations.Certificates;
using Raven.Tests.Core.Utils.Entities;
using Tests.Infrastructure;
using Xunit;
using Xunit.Abstractions;

namespace SlowTests.Server.Replication
{
    public class ReplicationBasicTestsSlow : ReplicationTestBase
    {
        public ReplicationBasicTestsSlow(ITestOutputHelper output) : base(output)
        {
        }

        public readonly string DbName = "TestDB" + Guid.NewGuid();

        [RavenTheory(RavenTestCategory.Replication)]
        [RavenData(false, DatabaseMode = RavenDatabaseMode.All)]
        [RavenData(true, DatabaseMode = RavenDatabaseMode.All)]
        public async Task Master_master_replication_from_etag_zero_without_conflict_should_work(Options options, bool useSsl)
        {
            var dbName1 = DbName + "-1";
            var dbName2 = DbName + "-2";

            X509Certificate2 clientCertificate = null;
            X509Certificate2 adminCertificate = null;
            if (useSsl)
            {
                var certificates = Certificates.SetupServerAuthentication();
                adminCertificate = Certificates.RegisterClientCertificate(certificates.ServerCertificate.Value, certificates.ClientCertificate1.Value, new Dictionary<string, DatabaseAccess>(), SecurityClearance.ClusterAdmin);
                clientCertificate = Certificates.RegisterClientCertificate(certificates.ServerCertificate.Value, certificates.ClientCertificate2.Value, new Dictionary<string, DatabaseAccess>(), SecurityClearance.Operator);
            }

            using (var store1 = GetDocumentStore(new Options(options)
            {
                AdminCertificate = adminCertificate,
                ClientCertificate = clientCertificate,
                ModifyDatabaseName = s => dbName1
            }))
            using (var store2 = GetDocumentStore(new Options(options)
            {
                AdminCertificate = adminCertificate,
                ClientCertificate = clientCertificate,
                ModifyDatabaseName = s => dbName2
            }))
            {
                await SetupReplicationAsync(store1, store2);
                await SetupReplicationAsync(store2, store1);
                using (var session = store1.OpenSession())
                {
                    session.Store(new User
                    {
                        Name = "John Dow",
                        Age = 30
                    }, "users/1");
                    session.SaveChanges();
                }
                using (var session = store2.OpenSession())
                {
                    session.Store(new User
                    {
                        Name = "Jane Dow",
                        Age = 31
                    }, "users/2");

                    session.SaveChanges();
                }

                var replicated1 = WaitForDocumentToReplicate<User>(store1, "users/1", 10000);

                Assert.NotNull(replicated1);
                Assert.Equal("John Dow", replicated1.Name);
                Assert.Equal(30, replicated1.Age);

                var replicated2 = WaitForDocumentToReplicate<User>(store1, "users/2", 10000);
                Assert.NotNull(replicated2);
                Assert.Equal("Jane Dow", replicated2.Name);
                Assert.Equal(31, replicated2.Age);

                replicated1 = WaitForDocumentToReplicate<User>(store2, "users/1", 10000);

                Assert.NotNull(replicated1);
                Assert.Equal("John Dow", replicated1.Name);
                Assert.Equal(30, replicated1.Age);

                replicated2 = WaitForDocumentToReplicate<User>(store2, "users/2", 10000);
                Assert.NotNull(replicated2);
                Assert.Equal("Jane Dow", replicated2.Name);
                Assert.Equal(31, replicated2.Age);
            }
        }

        [RavenTheory(RavenTestCategory.Replication)]
        [RavenData(DatabaseMode = RavenDatabaseMode.All)]
        public async Task DisableExternalReplication(Options options)
        {
            using (var store1 = GetDocumentStore(options))
            using (var store2 = GetDocumentStore(options))
            {
                var externalList = await SetupReplicationAsync(store1, store2);
                using (var session = store1.OpenSession())
                {
                    session.Store(new User
                    {
                        Name = "John Dow",
                        Age = 30
                    }, "users/1");
                    session.SaveChanges();
                }

                var replicated1 = WaitForDocumentToReplicate<User>(store2, "users/1", 10000);
                Assert.NotNull(replicated1);
                Assert.Equal("John Dow", replicated1.Name);
                Assert.Equal(30, replicated1.Age);

                var db1 = await Server.ServerStore.DatabasesLandlord.TryGetOrCreateResourceStore(options.DatabaseMode == RavenDatabaseMode.Single
                  ? store1.Database
                  : await Sharding.GetShardDatabaseNameForDocAsync(store1, "users/1"));
                var replicationConnection = db1.ReplicationLoader.OutgoingHandlers.First();

                var external = new ExternalReplication(store1.Database, $"ConnectionString-{store2.Identifier}")
                {
                    TaskId = externalList.First().TaskId,
                    Disabled = true
                };

                var res = await store1.Maintenance.SendAsync(new UpdateExternalReplicationOperation(external));
                Assert.Equal(externalList.First().TaskId, res.TaskId);

                //make sure the command is processed
                await Server.ServerStore.Cluster.WaitForIndexNotification(res.RaftCommandIndex);

                var connectionDropped = await WaitForValueAsync(() => replicationConnection.IsConnectionDisposed, true);
                Assert.True(connectionDropped);

                using (var session = store1.OpenSession())
                {
                    session.Store(new User
                    {
                        Name = "John Dow",
                        Age = 30
                    }, "users/2");
                    session.SaveChanges();
                }
                var replicated2 = WaitForDocumentToReplicate<User>(store2, "users/2", 10000);
                Assert.Null(replicated2);
            }
        }

        [RavenTheory(RavenTestCategory.Replication)]
        [RavenData(DatabaseMode = RavenDatabaseMode.All)]
        public async Task Update_LastWork_With_Replication(Options options)
        {
            using (var store1 = GetDocumentStore(options))
            using (var store2 = GetDocumentStore(options))
            {
                var db2 = await Server.ServerStore.DatabasesLandlord.TryGetOrCreateResourceStore(options.DatabaseMode == RavenDatabaseMode.Single
                    ? store2.Database
                    : await Sharding.GetShardDatabaseNameForDocAsync(store2, "users/1"));

                await SetupReplicationAsync(store1, store2);

                using (var session = store1.OpenSession())
                {
                    session.Store(new User(), "users/2$users/1");
                    session.SaveChanges();
                }

                Assert.True(WaitForDocument(store2, "users/2$users/1"));

                var lastAccessTime = db2.LastAccessTime;

                using (var session = store1.OpenSession())
                {
                    session.Store(new User
                    {
                        Name = "John Dow",
                        Age = 30
                    }, "users/1");
                    session.SaveChanges();
                }

                Assert.Equal(2, await WaitForValueAsync(() => db2.DocumentsStorage.GetNumberOfDocuments(), 2));

                var lastAccessTimeAfterLoad = db2.LastAccessTime;

                using (var session = store2.OpenSession())
                {
                    Assert.NotNull(session.Load<User>("users/1"));
                }

                Assert.NotEqual(lastAccessTimeAfterLoad, lastAccessTime);
            }
        }

        // RavenDB-15081
        [RavenTheory(RavenTestCategory.Replication | RavenTestCategory.Attachments | RavenTestCategory.Revisions)]
        [RavenData(DatabaseMode = RavenDatabaseMode.All)]
        public async Task CanReplicateExpiredRevisionsWithAttachment(Options options)
        {
            var rnd = new Random();
            var b = new byte[16 * 1024];
            rnd.NextBytes(b);
            using (var stream = new MemoryStream(b))
            {
                using (var store1 = GetDocumentStore(options))
                using (var store2 = GetDocumentStore(options))
                {
                    // Define revisions settings
                    var configuration = new RevisionsConfiguration
                    {
                        Collections = new Dictionary<string, RevisionsCollectionConfiguration>
                        {
                            {
                                "Users", new RevisionsCollectionConfiguration
                                {
                                    PurgeOnDelete = false,
                                    MinimumRevisionsToKeep = 100
                                }
                            }
                        }
                    };

                    const string id = "users/1";
                    const string attachmentName = "Typical attachment name";

                    var db1 = await Server.ServerStore.DatabasesLandlord.TryGetOrCreateResourceStore(options.DatabaseMode == RavenDatabaseMode.Single
                        ? store1.Database
                        : await Sharding.GetShardDatabaseNameForDocAsync(store1, id));
                    db1.Time.UtcDateTime = () => DateTime.UtcNow.Add(TimeSpan.FromDays(-60));

                    using (var session = store1.OpenSession())
                    {
                        var user = new User { Name = "su" };
                        session.Store(user, id);
                        session.SaveChanges();
                    }

                    store1.Operations.Send(new PutAttachmentOperation(id, $"{attachmentName}_1", stream, "application/zip"));
                    stream.Position = 0;
                    await store1.Maintenance.SendAsync(new ConfigureRevisionsOperation(configuration));

                    for (int i = 0; i < 5; i++)
                    {
                        using (var session = store1.OpenSession())
                        {
                            var u = session.Load<User>(id);
                            u.Age = i;
                            session.SaveChanges();
                        }
                    }
                    using (var session = store1.OpenSession())
                    {
                        session.Delete(id);
                        session.SaveChanges();
                    }
                    await SetupReplicationAsync(store1, store2);

                    var db2 = await Server.ServerStore.DatabasesLandlord.TryGetOrCreateResourceStore(options.DatabaseMode == RavenDatabaseMode.Single
                        ? store2.Database
                        : await Sharding.GetShardDatabaseNameForDocAsync(store2, id));

                    Assert.Equal(1, WaitForValue(() => db1.ReplicationLoader.OutgoingHandlers.Count(), 1));

                    var expectedIncomingCount = options.DatabaseMode == RavenDatabaseMode.Single ? 1 : 3; // for sharding we have 3 incoming connections (one for each shard in source)
                    Assert.Equal(expectedIncomingCount, WaitForValue(() => db2.ReplicationLoader.IncomingHandlers.Count(), expectedIncomingCount));
                    var outgoingReplicationConnection = db1.ReplicationLoader.OutgoingHandlers.First();
                    var incomingReplicationConnection = db2.ReplicationLoader.IncomingHandlers.Single(i => string.Equals(db1.DbId.ToString(), i.ConnectionInfo.SourceDatabaseId, StringComparison.OrdinalIgnoreCase));
                    Assert.Equal(20, WaitForValue(() => outgoingReplicationConnection.LastSentDocumentEtag, 20));
                    Assert.Equal(20, WaitForValue(() => incomingReplicationConnection.LastDocumentEtag, 20));

                    var stats1 = await GetDatabaseStatisticsAsync(store1);
                    var stats2 = await GetDatabaseStatisticsAsync(store2);
                    Assert.Equal(stats1.DatabaseChangeVector, stats2.DatabaseChangeVector);
                    Assert.Equal(2, stats2.CountOfTombstones);
                }
            }
        }
    }
}
