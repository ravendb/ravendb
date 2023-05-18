using System;
using System.Collections.Generic;
using System.IO;
using System.Linq;
using System.Threading.Tasks;
using Raven.Client;
using Raven.Client.Documents;
using Raven.Client.Documents.Operations.Attachments;
using Raven.Client.ServerWide.Operations;
using Raven.Client.ServerWide.Operations.Certificates;
using Raven.Client.ServerWide.Sharding;
using Raven.Server.Documents;
using Raven.Server.ServerWide.Context;
using Raven.Server.Utils;
using Raven.Tests.Core.Utils.Entities;
using Tests.Infrastructure;
using Xunit;
using Xunit.Abstractions;

namespace SlowTests.Sharding.Encryption
{
    public class ShardedEncryption : ClusterTestBase
    {
        public ShardedEncryption(ITestOutputHelper output) : base(output)
        {
        }

        [RavenFact(RavenTestCategory.Encryption | RavenTestCategory.Sharding, LicenseRequired = true)]
        public void Can_Setup_Sharded_Encrypted_Database()
        {
            Encryption.EncryptedServer(out var certificates, out var dbName);

            var options = new Options
            {
                AdminCertificate = certificates.ServerCertificate.Value,
                ClientCertificate = certificates.ServerCertificate.Value,
                ModifyDatabaseRecord = record =>
                {
                    record.Encrypted = true;
                },
                ModifyDatabaseName = s => dbName
            };

            using (var store = Sharding.GetDocumentStore(options))
            {
                using (var session = store.OpenSession())
                {
                    session.Store(new User
                    {
                        Name = "ayende"
                    }, "users/1");

                    session.SaveChanges();
                }

                using (var session = store.OpenSession())
                {
                    var loaded = session.Load<User>("users/1");
                    Assert.Equal("ayende", loaded.Name);
                }
            }
        }

        [RavenFact(RavenTestCategory.Encryption | RavenTestCategory.Sharding, LicenseRequired = true)]
        public async Task CRUD_Operations_Encrypted()
        {
            Encryption.EncryptedServer(out var certificates, out var dbName);

            var options = new Options
            {
                AdminCertificate = certificates.ServerCertificate.Value,
                ClientCertificate = certificates.ServerCertificate.Value,
                ModifyDatabaseRecord = record =>
                {
                    record.Encrypted = true;
                },
                ModifyDatabaseName = s => dbName
            };

            using (var store = Sharding.GetDocumentStore(options))
            {
                using (var session = store.OpenSession())
                {
                    session.Store(new User { Name = "user1" }, "users/1");
                    var user2 = new User { Name = "user2", Age = 1 };
                    session.Store(user2, "users/2");
                    var user3 = new User { Name = "user3", Age = 1 };
                    session.Store(user3, "users/3");
                    session.Store(new User { Name = "user4" }, "users/4");

                    session.Delete(user2);
                    user3.Age = 3;
                    session.SaveChanges();

                    var tempUser = session.Load<User>("users/2");
                    Assert.Null(tempUser);
                    tempUser = session.Load<User>("users/3");
                    Assert.Equal(tempUser.Age, 3);
                    var user1 = session.Load<User>("users/1");
                    var user4 = session.Load<User>("users/4");

                    session.Delete(user4);
                    user1.Age = 10;
                    session.SaveChanges();

                    tempUser = session.Load<User>("users/4");
                    Assert.Null(tempUser);
                    tempUser = session.Load<User>("users/1");
                    Assert.Equal(tempUser.Age, 10);
                }

                using (var session = store.OpenAsyncSession())
                {
                    await using (var profileStream = new MemoryStream(new byte[] { 1, 2, 3 }))
                    {
                        session.Advanced.Attachments.Store("users/1", "profile.png", profileStream, "image/png");
                        await session.SaveChangesAsync();
                    }
                }

                using (var session = store.OpenAsyncSession())
                {
                    var user = await session.LoadAsync<User>("users/1");
                    var metadata = session.Advanced.GetMetadataFor(user);
                    Assert.Equal(DocumentFlags.HasAttachments.ToString(), metadata[Constants.Documents.Metadata.Flags]);
                    var attachments = metadata.GetObjects(Constants.Documents.Metadata.Attachments);
                    Assert.Equal(1, attachments.Length);
                    var attachment = attachments[0];
                    Assert.Equal("profile.png", attachment.GetString(nameof(AttachmentName.Name)));
                    var hash = attachment.GetString(nameof(AttachmentName.Hash));
                    Assert.Equal("EcDnm3HDl2zNDALRMQ4lFsCO3J2Lb1fM1oDWOk2Octo=", hash);
                    Assert.Equal(3, attachment.GetLong(nameof(AttachmentName.Size)));
                }
            }
        }

        [RavenFact(RavenTestCategory.Encryption | RavenTestCategory.Sharding, LicenseRequired = true)]
        public async Task Can_Add_Shard_To_Encrypted_Database()
        {
            var (nodes, leader, certificates) = await CreateRaftClusterWithSsl(3, watcherCluster: true);
            Encryption.SetupEncryptedDatabaseInCluster(nodes, certificates, out var databaseName);

            var options = Sharding.GetOptionsForCluster(leader, shards: 2, shardReplicationFactor: 1, orchestratorReplicationFactor: 1);
            options.ClientCertificate = certificates.ClientCertificate1.Value;
            options.AdminCertificate = certificates.ServerCertificate.Value;
            options.ModifyDatabaseName = _ => databaseName;
            options.ModifyDatabaseRecord += record =>
            {
                record.Encrypted = true;
                record.Sharding.Shards[0].Members = new List<string> { "A" };
                record.Sharding.Shards[1].Members = new List<string> { "B" };
                record.Sharding.Orchestrator.Topology.Members = new List<string> { "A" };
            };
            
            options.RunInMemory = false;

            using (var store = GetDocumentStore(options))
            {
                var shardingConfiguration = await Sharding.GetShardingConfigurationAsync(store);
                Assert.Equal(2, shardingConfiguration.Shards.Count);

                Assert.Equal(1, shardingConfiguration.Shards[0].Count);
                Assert.Equal("A", shardingConfiguration.Shards[0].Members[0]);

                Assert.Equal(1, shardingConfiguration.Shards[1].Count);
                Assert.Equal("B", shardingConfiguration.Shards[1].Members[0]);

                Assert.Equal(1, shardingConfiguration.Orchestrator.Topology.Count);
                Assert.Equal("A", shardingConfiguration.Orchestrator.Topology.Members[0]);

                //create new shard on a node that didn't have shards or orchestrator before
                var res = await store.Maintenance.Server.SendAsync(new AddDatabaseShardOperation(store.Database, nodes: new []{ "C" }));
                var newShardNumber = res.ShardNumber;
                Assert.Equal(2, newShardNumber);
                Assert.Equal(1, res.ShardTopology.ReplicationFactor);
                Assert.Equal(1, res.ShardTopology.AllNodes.Count());
                Assert.Equal("C", res.ShardTopology.Members[0]);

                await Cluster.WaitForRaftIndexToBeAppliedInClusterAsync(res.RaftCommandIndex);

                await AssertWaitForValueAsync(async () =>
                {
                    var record = await store.Maintenance.Server.SendAsync(new GetDatabaseRecordOperation(store.Database));
                    return record.Sharding.Shards.Count;
                }, 3);

                var record = await store.Maintenance.Server.SendAsync(new GetDatabaseRecordOperation(store.Database));
                var bucket = Sharding.GetBucket(record.Sharding, "foo/bar");
                record.MoveBucket(bucket, newShardNumber);
                var putResult = await store.Maintenance.Server.SendAsync(new UpdateDatabaseOperation(record, replicationFactor:1 ,record.Etag));
                
                await Sharding.WaitForOrchestratorsToUpdate(store.Database, putResult.RaftCommandIndex);
                await Databases.WaitForRaftIndex(ShardHelper.ToShardName(store.Database, newShardNumber), putResult.RaftCommandIndex);

                using (var session = store.OpenAsyncSession(ShardHelper.ToShardName(store.Database, newShardNumber)))
                {
                    await session.StoreAsync(new User
                    {
                        Name = "ayende"
                    }, "foo/bar");

                    await session.SaveChangesAsync();
                }

                using (var session = store.OpenAsyncSession(ShardHelper.ToShardName(store.Database, newShardNumber)))
                {
                    var users = await session.Query<User>().ToListAsync();
                    Assert.Equal(1, users.Count);
                    Assert.Equal("ayende", users[0].Name);
                }
            }
        }

        [RavenFact(RavenTestCategory.Encryption | RavenTestCategory.Sharding, LicenseRequired = true)]
        public async Task ClientCertificateForShardedDatabaseShouldPermitAccessToIndividualShards()
        {
            Encryption.EncryptedServer(out var certificates, out var dbName);

            var options = new Options
            {
                AdminCertificate = certificates.ServerCertificate.Value,
                ClientCertificate = certificates.ClientCertificate1.Value,
                ModifyDatabaseRecord = record =>
                {
                    record.Encrypted = true;
                },
                ModifyDatabaseName = s => dbName,
                DeleteDatabaseOnDispose = false
            };
            var dic = new Dictionary<int, List<string>>();
            ShardingConfiguration sharding;

            using (var store = Sharding.GetDocumentStore(options))
            {
                sharding = await Sharding.GetShardingConfigurationAsync(store);

                // insert dome data
                using (var session = store.OpenSession())
                {
                    for (int i = 0; i < 10; i++)
                    {
                        var id = $"users/{i}";

                        session.Store(new User
                        {
                            Name = $"user-{i}"
                        }, id);

                        var shardNumber = await Sharding.GetShardNumberForAsync(store, id);
                        if (dic.TryGetValue(shardNumber, out var idsList) == false)
                        {
                            dic[shardNumber] = idsList = new List<string>();
                        }
                        idsList.Add(id);
                    }

                    session.SaveChanges();
                }
            }

            var userCert = certificates.ClientCertificate2.Value;

            Certificates.RegisterClientCertificate(certificates.ServerCertificate.Value,
                clientCertificate: userCert,
                permissions: new Dictionary<string, DatabaseAccess>
                {
                    [dbName] = DatabaseAccess.Admin
                },
                clearance: SecurityClearance.ValidUser);

            options.ClientCertificate = userCert;
            options.CreateDatabase = false;
            options.DeleteDatabaseOnDispose = true;

            using (var store = Sharding.GetDocumentStore(options))
            {
                // assert that we can access the sharded-db
                using (var session = store.OpenAsyncSession())
                {
                    for (int i = 0; i < 10; i++)
                    {
                        var id = $"users/{i}";
                        var doc = await session.LoadAsync<User>(id);
                        Assert.Equal($"user-{i}", doc.Name);
                    }
                }

                // assert that we can access each shard directly
                foreach (var shardNumber in sharding.Shards.Keys)
                {
                    string newId;
                    var shard = ShardHelper.ToShardName(store.Database, shardNumber);
                    using (var session = store.OpenAsyncSession(database: shard))
                    {
                        Assert.True(dic.TryGetValue(shardNumber, out var idsList));
                        foreach (var id in idsList)
                        {
                            var doc = await session.LoadAsync<User>(id);
                            Assert.NotNull(doc);
                        }

                        newId = $"users/new/${idsList.First()}";
                        await session.StoreAsync(new User(), newId);

                        await session.SaveChangesAsync();
                    }

                    using (var session = store.OpenAsyncSession(database: shard))
                    {
                        var newDoc = await session.LoadAsync<User>(newId);
                        Assert.NotNull(newDoc);
                    }
                }
            }
        }

        [RavenFact(RavenTestCategory.Encryption | RavenTestCategory.Sharding, LicenseRequired = true)]
        public async Task DatabaseSecretKeyShouldBeDeletedAfterShardedDatabaseDeletion()
        {
            var (nodes, leader, certificates) = await CreateRaftClusterWithSsl(3, watcherCluster: true);
            Encryption.SetupEncryptedDatabaseInCluster(nodes, certificates, out var databaseName);

            var options = Sharding.GetOptionsForCluster(leader, shards: 3, shardReplicationFactor: 1, orchestratorReplicationFactor: 3);
            options.ClientCertificate = certificates.ClientCertificate1.Value;
            options.AdminCertificate = certificates.ServerCertificate.Value;
            options.ModifyDatabaseName = _ => databaseName;
            options.ModifyDatabaseRecord += record => record.Encrypted = true;
            options.RunInMemory = false;
            options.DeleteDatabaseOnDispose = false;

            using (var store = Sharding.GetDocumentStore(options))
            {
                await store.Maintenance.SendAsync(new CreateSampleDataOperation());

                foreach (var server in nodes)
                {
                    using (server.ServerStore.ContextPool.AllocateOperationContext(out TransactionOperationContext ctx))
                    using (ctx.OpenReadTransaction())
                    {
                        var keys = server.ServerStore.GetSecretKeysNames(ctx).ToList();
                        Assert.Equal(1, keys.Count);
                        Assert.Equal(store.Database, keys[0]);
                    }
                }

                await store.Maintenance.Server.SendAsync(new DeleteDatabasesOperation(store.Database, hardDelete: true));

                Assert.True(WaitForValue(() =>
                {
                    var record = store.Maintenance.Server.Send(new GetDatabaseRecordOperation(store.Database));
                    return record == null;
                }, expectedVal: true));

                foreach (var server in nodes)
                {
                    using (server.ServerStore.ContextPool.AllocateOperationContext(out TransactionOperationContext ctx))
                    using (ctx.OpenReadTransaction())
                    {
                        var keys = server.ServerStore.GetSecretKeysNames(ctx).ToList();
                        Assert.Empty(keys);
                    }
                }
            }
        }

        [RavenFact(RavenTestCategory.Encryption | RavenTestCategory.Sharding, LicenseRequired = true)]
        public async Task ShouldNotRemoveSecretKeyFromNodeThatStillHasShards()
        {
            var (nodes, leader, certificates) = await CreateRaftClusterWithSsl(3, watcherCluster: true);
            Encryption.SetupEncryptedDatabaseInCluster(nodes, certificates, out var databaseName);

            var options = Sharding.GetOptionsForCluster(leader, shards: 3, shardReplicationFactor: 1, orchestratorReplicationFactor: 3);
            options.ClientCertificate = certificates.ClientCertificate1.Value;
            options.AdminCertificate = certificates.ServerCertificate.Value;
            options.ModifyDatabaseName = _ => databaseName;
            options.ModifyDatabaseRecord += record => record.Encrypted = true;
            options.RunInMemory = false;

            using (var store = Sharding.GetDocumentStore(options))
            {
                var sharding = await Sharding.GetShardingConfigurationAsync(store);
                Assert.True(sharding.Shards[0].Members.Count > 0,$"No members found. {Environment.NewLine}{sharding.Shards[0]}");
                var nodeToAddShardTo = sharding.Shards[0].Members[0];

                // add shard
                var addShardRes = store.Maintenance.Server.Send(new AddDatabaseShardOperation(store.Database, new[] { nodeToAddShardTo }));
                await Cluster.WaitForRaftIndexToBeAppliedInClusterAsync(addShardRes.RaftCommandIndex);
                var newShardNumber = addShardRes.ShardNumber;

                await AssertWaitForValueAsync(async () =>
                {
                    sharding = await Sharding.GetShardingConfigurationAsync(store);
                    sharding.Shards.TryGetValue(newShardNumber, out var topology);
                    return topology?.Members.Count;
                }, expectedVal: 1);

                Assert.Equal(sharding.Shards[0].Members[0], sharding.Shards[newShardNumber].Members[0]);

                // remove the newly added shard 
                var res = store.Maintenance.Server.Send(new DeleteDatabasesOperation(store.Database, shardNumber: newShardNumber, hardDelete: true, fromNode: nodeToAddShardTo));
                await Cluster.WaitForRaftIndexToBeAppliedInClusterAsync(res.RaftCommandIndex);

                await AssertWaitForValueAsync(async () =>
                {
                    sharding = await Sharding.GetShardingConfigurationAsync(store);
                    return sharding.Shards.TryGetValue(newShardNumber, out _);
                }, expectedVal: false);

                // verify that secret key is not deleted from the shard-node that we removed
                var node = nodes.Single(n => n.ServerStore.NodeTag == nodeToAddShardTo);
                using (node.ServerStore.ContextPool.AllocateOperationContext(out TransactionOperationContext ctx))
                using (ctx.OpenReadTransaction())
                {
                    var keys = node.ServerStore.GetSecretKeysNames(ctx).ToList();
                    Assert.Equal(1, keys.Count);
                }
            }
        }

        [RavenFact(RavenTestCategory.Encryption | RavenTestCategory.Sharding, LicenseRequired = true)]
        public async Task CanAddAndRemoveShardFromEncryptedShardedDb()
        {
            var (nodes, leader, certificates) = await CreateRaftClusterWithSsl(3, watcherCluster: true);
            Encryption.SetupEncryptedDatabaseInCluster(nodes, certificates, out var databaseName);

            var options = Sharding.GetOptionsForCluster(leader, shards: 2, shardReplicationFactor: 1, orchestratorReplicationFactor: 2);
            options.ClientCertificate = certificates.ClientCertificate1.Value;
            options.AdminCertificate = certificates.ServerCertificate.Value;
            options.ModifyDatabaseName = _ => databaseName;
            options.ModifyDatabaseRecord += record => record.Encrypted = true;
            options.RunInMemory = false;

            using (var store = Sharding.GetDocumentStore(options))
            {
                await store.Maintenance.SendAsync(new CreateSampleDataOperation());

                foreach (var server in nodes)
                {
                    using (server.ServerStore.ContextPool.AllocateOperationContext(out TransactionOperationContext ctx))
                    using (ctx.OpenReadTransaction())
                    {
                        var keys = server.ServerStore.GetSecretKeysNames(ctx).ToList();
                        Assert.Equal(1, keys.Count);
                        Assert.Equal(store.Database, keys[0]);
                    }
                }

                // add shard
                var sharding = await Sharding.GetShardingConfigurationAsync(store);
                var shardNodes = sharding.Shards.Select(kvp => kvp.Value.Members[0]);
                var nodeNotInDbGroup = nodes.SingleOrDefault(n => shardNodes.Contains(n.ServerStore.NodeTag) == false);
                Assert.NotNull(nodeNotInDbGroup);

                var addShardRes = store.Maintenance.Server.Send(new AddDatabaseShardOperation(store.Database, new[] { nodeNotInDbGroup.ServerStore.NodeTag }));
                await Cluster.WaitForRaftIndexToBeAppliedInClusterAsync(addShardRes.RaftCommandIndex);

                await AssertWaitForValueAsync(async () =>
                {
                    sharding = await Sharding.GetShardingConfigurationAsync(store);
                    sharding.Shards.TryGetValue(addShardRes.ShardNumber, out var topology);
                    return topology?.Members.Count;
                }, expectedVal: 1);

                Assert.Equal(nodeNotInDbGroup.ServerStore.NodeTag, sharding.Shards[addShardRes.ShardNumber].Members[0]);

                // remove the newly added shard 
                var shardToRemove = addShardRes.ShardNumber;
                var nodeToRemoveFrom = sharding.Shards[shardToRemove].Members[0];

                var res = store.Maintenance.Server.Send(new DeleteDatabasesOperation(store.Database, shardNumber: shardToRemove, hardDelete: true, fromNode: nodeToRemoveFrom));
                await Cluster.WaitForRaftIndexToBeAppliedInClusterAsync(res.RaftCommandIndex);

                await AssertWaitForValueAsync(async () =>
                {
                    sharding = await Sharding.GetShardingConfigurationAsync(store);
                    return sharding.Shards.TryGetValue(shardToRemove, out _);
                }, expectedVal: false);

                // verify that secret key is deleted from the shard-node that we removed,
                // and from that node only
                foreach (var server in nodes)
                {
                    using (server.ServerStore.ContextPool.AllocateOperationContext(out TransactionOperationContext ctx))
                    using (ctx.OpenReadTransaction())
                    {
                        var keys = server.ServerStore.GetSecretKeysNames(ctx).ToList();
                        if (server.ServerStore.NodeTag == nodeToRemoveFrom)
                            Assert.Empty(keys);
                        else
                            Assert.Equal(1, keys.Count);
                    }
                }
            }
        }
    }
}
