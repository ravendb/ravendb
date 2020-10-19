using System;
using System.Collections.Generic;
using System.IO;
using System.Linq;
using System.Security.Cryptography.X509Certificates;
using System.Threading.Tasks;
using FastTests.Server.Replication;
using Raven.Client.Documents.Operations;
using Raven.Client.Documents.Operations.Attachments;
using Raven.Client.Documents.Operations.Replication;
using Raven.Client.Documents.Operations.Revisions;
using Raven.Client.ServerWide.Operations.Certificates;
using Raven.Tests.Core.Utils.Entities;
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

        [Theory]
        [InlineData(false)]
        [InlineData(true)]
        public async Task Master_master_replication_from_etag_zero_without_conflict_should_work(bool useSsl)
        {
            var dbName1 = DbName + "-1";
            var dbName2 = DbName + "-2";

            X509Certificate2 clientCertificate = null;
            X509Certificate2 adminCertificate = null;
            if (useSsl)
            {
                var certificates = SetupServerAuthentication();
                adminCertificate = RegisterClientCertificate(certificates.ServerCertificate.Value, certificates.ClientCertificate1.Value, new Dictionary<string, DatabaseAccess>(), SecurityClearance.ClusterAdmin);
                clientCertificate = RegisterClientCertificate(certificates.ServerCertificate.Value, certificates.ClientCertificate2.Value, new Dictionary<string, DatabaseAccess>(), SecurityClearance.Operator);
            }

            using (var store1 = GetDocumentStore(new Options
            {
                AdminCertificate = adminCertificate,
                ClientCertificate = clientCertificate,
                ModifyDatabaseName = s => dbName1
            }))
            using (var store2 = GetDocumentStore(new Options
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

        [Fact]
        public async Task DisableExternalReplication()
        {
            using (var store1 = GetDocumentStore())
            using (var store2 = GetDocumentStore())
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

                var db1 = await GetDocumentDatabaseInstanceFor(store1);
                var db2 = await GetDocumentDatabaseInstanceFor(store2);
                var replicationConnection = db1.ReplicationLoader.OutgoingHandlers.First();

                var external = new ExternalReplication(store1.Database, $"ConnectionString-{store2.Identifier}")
                {
                    TaskId = externalList.First().TaskId,
                    Disabled = true
                };

                var res = await store1.Maintenance.SendAsync(new UpdateExternalReplicationOperation(external));
                Assert.Equal(externalList.First().TaskId, res.TaskId);

                //make sure the command is processed
                await db1.ServerStore.Cluster.WaitForIndexNotification(res.RaftCommandIndex);
                await db2.ServerStore.Cluster.WaitForIndexNotification(res.RaftCommandIndex);

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

        [Fact]
        public async Task Update_LastWork_With_Replication()
        {
            using (var store1 = GetDocumentStore())
            using (var store2 = GetDocumentStore())
            {
                var db1 = await GetDocumentDatabaseInstanceFor(store1);
                var db2 = await GetDocumentDatabaseInstanceFor(store2);

                await SetupReplicationAsync(store1, store2);

                EnsureReplicating(store1, store2);

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
        [Fact]
        public async Task CanReplicateExpiredRevisionsWithAttachment()
        {
            var rnd = new Random();
            var b = new byte[16 * 1024];
            rnd.NextBytes(b);
            using (var stream = new MemoryStream(b))
            {
                using (var store1 = GetDocumentStore())
                using (var store2 = GetDocumentStore())
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

                    var db1 = await GetDocumentDatabaseInstanceFor(store1);
                    db1.Time.UtcDateTime = () => DateTime.UtcNow.Add(TimeSpan.FromDays(-60));
                    const string id = "users/1";
                    const string attachmentName = "Typical attachment name";
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

                    var db2 = await GetDocumentDatabaseInstanceFor(store2);
                    Assert.Equal(1, WaitForValue(() => db1.ReplicationLoader.OutgoingHandlers.Count(), 1));
                    Assert.Equal(1, WaitForValue(() => db2.ReplicationLoader.IncomingHandlers.Count(), 1));
                    var outgoingReplicationConnection = db1.ReplicationLoader.OutgoingHandlers.First();
                    var incomingReplicationConnection = db2.ReplicationLoader.IncomingHandlers.First();
                    Assert.Equal(20, WaitForValue(() => outgoingReplicationConnection._lastSentDocumentEtag, 20));
                    Assert.Equal(20, WaitForValue(() => incomingReplicationConnection.LastDocumentEtag, 20));

                    var stats1 = store1.Maintenance.Send(new GetStatisticsOperation());
                    var stats2 = store2.Maintenance.Send(new GetStatisticsOperation());
                    Assert.Equal(stats1.DatabaseChangeVector, stats2.DatabaseChangeVector);
                    Assert.Equal(13, stats2.CountOfTombstones);
                }
            }
        }
    }
}
