using System;
using System.Collections.Generic;
using System.Linq;
using System.Security.Cryptography.X509Certificates;
using System.Threading.Tasks;
using FastTests.Server.Replication;
using Raven.Client.Documents.Operations.Replication;
using Raven.Client.ServerWide.Operations.Certificates;
using Raven.Server.ServerWide.Context;
using Raven.Tests.Core.Utils.Entities;
using Xunit;

namespace SlowTests.Server.Replication
{
    public class ReplicationBasicTestsSlow : ReplicationTestBase
    {
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
                var serverCertPath = SetupServerAuthentication();
                adminCertificate = AskServerForClientCertificate(serverCertPath, new Dictionary<string, DatabaseAccess>(), SecurityClearance.ClusterAdmin);
                clientCertificate = AskServerForClientCertificate(serverCertPath, new Dictionary<string, DatabaseAccess>(), SecurityClearance.Operator);
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
        public async Task DontReplicateTombstoneBack()
        {
            var dbName1 = DbName + "-1";
            var dbName2 = DbName + "-2";

            using (var store1 = GetDocumentStore(new Options
            {
                ModifyDatabaseName = s => dbName1
            }))
            using (var store2 = GetDocumentStore(new Options
            {
                ModifyDatabaseName = s => dbName2
            }))
            {
                string changeVector1;
                var documentDatabase = await Server.ServerStore.DatabasesLandlord.TryGetOrCreateResourceStore(dbName1);

                using (var session = store1.OpenSession())
                {
                    var user = new User
                    {
                        Name = "John Dow",
                        Age = 30
                    };
                    session.Store(user, "users/1");
                    session.SaveChanges();
                    session.Delete(user);
                    session.SaveChanges();

                    using (documentDatabase.DocumentsStorage.ContextPool.AllocateOperationContext(out DocumentsOperationContext context))
                    using (context.OpenReadTransaction())
                    {
                        changeVector1 = documentDatabase.DocumentsStorage.GetDocumentOrTombstone(context, "users/1").Tombstone.ChangeVector;
                    }
                }
                await SetupReplicationAsync(store1, store2);
                await SetupReplicationAsync(store2, store1);

                Assert.True(WaitForDocumentDeletion(store2, "users/1"));
                await Task.Delay((int)(documentDatabase.ReplicationLoader.MinimalHeartbeatInterval * 2.5));

                using (documentDatabase.DocumentsStorage.ContextPool.AllocateOperationContext(out DocumentsOperationContext context))
                using (context.OpenReadTransaction())
                {
                    var changeVector2 = documentDatabase.DocumentsStorage.GetDocumentOrTombstone(context, "users/1").Tombstone.ChangeVector;
                    Assert.Equal(changeVector1, changeVector2);
                }
            }
        }

        [Fact]
        public async Task RavenDB_12295()
        {
            var dbName1 = DbName + "-1";
            var dbName2 = DbName + "-2";

            using (var store1 = GetDocumentStore(new Options
            {
                ModifyDatabaseName = s => dbName1
            }))
            using (var store2 = GetDocumentStore(new Options
            {
                ModifyDatabaseName = s => dbName2
            }))
            {
                using (var session = store1.OpenSession())
                {
                    var user = new User
                    {
                        Name = "John Dow",
                        Age = 30
                    };
                    session.Store(user, "users/1");
                    session.SaveChanges();

                    session.Delete(user);
                    session.SaveChanges();
                }

                await SetupReplicationAsync(store1, store2);
                await SetupReplicationAsync(store2, store1);

                using (var session = store1.OpenSession())
                {
                    session.Advanced.UseOptimisticConcurrency = true;

                    var user = new User
                    {
                        Name = "John Dow",
                        Age = 30
                    };
                    session.Store(user, "users/1");
                    session.SaveChanges();

                    await Task.Delay(2500);

                    var changeVector = session.Advanced.GetChangeVectorFor(user);

                    session.Delete(user.Id, changeVector);
                    session.SaveChanges();
                }
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

                var external = new ExternalReplication(store1.Database, $"ConnectionString-{store2.Identifier}")
                {
                    TaskId = externalList.First().TaskId,
                    Disabled = true
                };
                var res = await store1.Maintenance.SendAsync(new UpdateExternalReplicationOperation(external));

                Assert.Equal(externalList.First().TaskId, res.TaskId);
                
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

    }
}
