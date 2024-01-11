using System;
using System.Collections.Generic;
using System.IO;
using System.Linq;
using System.Threading;
using System.Threading.Tasks;
using FastTests;
using Raven.Client.Documents;
using Raven.Client.Documents.Operations.Backups;
using Raven.Client.Exceptions;
using Raven.Client.ServerWide.Operations;
using Raven.Client.ServerWide.Sharding;
using Raven.Server.Documents;
using Raven.Server.Documents.Sharding;
using Raven.Server.ServerWide.Context;
using Raven.Server.Utils;
using Raven.Tests.Core.Utils.Entities;
using Sparrow.Server;
using Sparrow.Threading;
using Tests.Infrastructure;
using Tests.Infrastructure.Entities;
using Voron;
using Xunit;
using Xunit.Abstractions;
using BucketStats = Raven.Server.Documents.Sharding.BucketStats;

namespace SlowTests.Sharding;

public class PrefixedSharding : RavenTestBase
{
    public PrefixedSharding(ITestOutputHelper output) : base(output)
    {
        DoNotReuseServer();

        Server.ServerStore.Sharding.BlockPrefixedSharding = false;
    }

    [RavenFact(RavenTestCategory.Sharding)]
    public async Task CanShardByDocumentsPrefix()
    {
        using var store = Sharding.GetDocumentStore(new Options
        {
            ModifyDatabaseRecord = record =>
            {
                record.Sharding ??= new ShardingConfiguration();
                record.Sharding.Prefixed = new List<PrefixedShardingSetting>
                {
                    new PrefixedShardingSetting
                    {
                        // range for 'eu/' is : 
                        // shard 0 : [1M, 2M]
                        Prefix = "eu/",
                        Shards = new List<int> { 0 }
                    },
                    new PrefixedShardingSetting
                    {
                        // range for 'asia/' is :
                        // shard 1 : [2M, 2.5M]
                        // shard 2 : [2.5M, 3M]
                        Prefix = "asia/",
                        Shards = new List<int> { 1, 2 }
                    }
                };
            }
        });

        var shardingConfiguration = await Sharding.GetShardingConfigurationAsync(store);
        Assert.Equal(6, shardingConfiguration.BucketRanges.Count);

        // 'eu' range
        Assert.Equal(ShardHelper.NumberOfBuckets, shardingConfiguration.Prefixed[0].BucketRangeStart);

        Assert.Equal(0, shardingConfiguration.BucketRanges[3].ShardNumber);
        Assert.Equal(ShardHelper.NumberOfBuckets, shardingConfiguration.BucketRanges[3].BucketRangeStart);

        // 'asia' ranges
        Assert.Equal(ShardHelper.NumberOfBuckets * 2, shardingConfiguration.Prefixed[1].BucketRangeStart);

        Assert.Equal(1, shardingConfiguration.BucketRanges[4].ShardNumber);
        Assert.Equal(ShardHelper.NumberOfBuckets * 2, shardingConfiguration.BucketRanges[4].BucketRangeStart);

        Assert.Equal(2, shardingConfiguration.BucketRanges[5].ShardNumber);
        Assert.Equal(ShardHelper.NumberOfBuckets * 2.5, shardingConfiguration.BucketRanges[5].BucketRangeStart);

        using (Server.ServerStore.ContextPool.AllocateOperationContext(out TransactionOperationContext context))
        {
            using (Slice.From(context.Allocator, "eu/0", out Slice idSlice))
            {
                var shardNumber = ShardHelper.GetShardNumberFor(shardingConfiguration, idSlice);
                Assert.Equal(0, shardNumber);
            }

            using (Slice.From(context.Allocator, "asia/1", out Slice idSlice))
            {
                var shardNumber = ShardHelper.GetShardNumberFor(shardingConfiguration, idSlice);
                Assert.Equal(1, shardNumber);
            }

            using (Slice.From(context.Allocator, "asia/2", out Slice idSlice))
            {
                var shardNumber = ShardHelper.GetShardNumberFor(shardingConfiguration, idSlice);
                Assert.Equal(2, shardNumber);
            }
        }

        var rand = new System.Random(2022_04_19);
        var prefixes = new[] { "us/", "eu/", "asia/", null };

        int d = 0;
        for (int t = 0; t < 16; t++)
        {
            using (var session = store.OpenAsyncSession())
            {
                for (int i = 0; i < 16; i++)
                {
                    string id = prefixes[rand.Next(prefixes.Length)] + "items/" + (++d);
                    await session.StoreAsync(new Item { }, id);
                }

                await session.SaveChangesAsync();
            }
        }

        using (var s = store.OpenAsyncSession(ShardHelper.ToShardName(store.Database, 0)))
        {
            // shard $0 has all the eu/ docs, no asia/ docs and fair share of the others
            Assert.Equal(73, await s.Query<Item>().CountAsync(i => i.Id.StartsWith("eu/")));
            Assert.Equal(0, await s.Query<Item>().CountAsync(i => i.Id.StartsWith("asia/")));
            Assert.Equal(25, await s.Query<Item>().CountAsync(i => i.Id.StartsWith("us/")));
            Assert.Equal(19, await s.Query<Item>().CountAsync(i => i.Id.StartsWith("items/")));
        }

        using (var s = store.OpenAsyncSession(ShardHelper.ToShardName(store.Database, 1)))
        {
            // shard $1 has no eu/ docs, half of the asia/ docs and fair share of the others
            Assert.Equal(0, await s.Query<Item>().CountAsync(i => i.Id.StartsWith("eu/")));
            Assert.Equal(35, await s.Query<Item>().CountAsync(i => i.Id.StartsWith("asia/")));
            Assert.Equal(19, await s.Query<Item>().CountAsync(i => i.Id.StartsWith("us/")));
            Assert.Equal(23, await s.Query<Item>().CountAsync(i => i.Id.StartsWith("items/")));
        }

        using (var s = store.OpenAsyncSession(ShardHelper.ToShardName(store.Database, 2)))
        {
            // shard $1 has no eu/ docs, half of the asia/ docs and fair share of the others
            Assert.Equal(0, await s.Query<Item>().CountAsync(i => i.Id.StartsWith("eu/")));
            Assert.Equal(19, await s.Query<Item>().CountAsync(i => i.Id.StartsWith("asia/")));
            Assert.Equal(22, await s.Query<Item>().CountAsync(i => i.Id.StartsWith("us/")));
            Assert.Equal(21, await s.Query<Item>().CountAsync(i => i.Id.StartsWith("items/")));
        }
    }

    [RavenFact(RavenTestCategory.Sharding)]
    public async Task ShouldThrowOnAttemptToAddPrefixThatDoesntEndWithSlashOrComma()
    {
        using var store = Sharding.GetDocumentStore();

        var task = store.Maintenance.SendAsync(new AddPrefixedShardingSettingOperation(new PrefixedShardingSetting
        {
            Prefix = "asia",
            Shards = new List<int> { 1, 2 }
        }));

        var e = await Assert.ThrowsAsync<RavenException>(async () => await task);
        Assert.Contains(
            "Cannot add prefix 'asia' to ShardingConfiguration.Prefixed. In order to define sharding by prefix, the prefix string must end with '/' or '-' characters",
            e.Message);
    }

    [RavenFact(RavenTestCategory.Sharding)]
    public async Task ShouldNotAllowToAddPrefixIfWeHaveDocsStartingWith()
    {
        using var store = Sharding.GetDocumentStore(new Options
        {
            ModifyDatabaseRecord = record =>
            {
                record.Sharding ??= new ShardingConfiguration();
                record.Sharding.Prefixed = new List<PrefixedShardingSetting>
                {
                    new PrefixedShardingSetting
                    {
                        Prefix = "eu/",
                        Shards = new List<int> { 0 }
                    }
                };
            }
        });

        using (var session = store.OpenAsyncSession())
        {
            for (int i = 0; i < 10; i++)
            {
                string id = "asia/items/" + i;
                await session.StoreAsync(new Item(), id);
            }

            await session.SaveChangesAsync();
        }

        var task = store.Maintenance.SendAsync(new AddPrefixedShardingSettingOperation(new PrefixedShardingSetting
        {
            Prefix = "asia/",
            Shards = new List<int> { 1, 2 }
        }));

        var e = await Assert.ThrowsAsync<RavenException>(async () => await task);
        Assert.Contains(
            $"Cannot add prefix 'asia/' to ShardingConfiguration.Prefixed. There are existing documents in database '{store.Database}' that start with 'asia/'",
            e.Message);
    }

    [RavenFact(RavenTestCategory.Sharding)]
    public async Task ShouldNotAllowToDeletePrefixIfWeHaveDocsStartingWith()
    {
        using var store = Sharding.GetDocumentStore(new Options
        {
            ModifyDatabaseRecord = record =>
            {
                record.Sharding ??= new ShardingConfiguration();
                record.Sharding.Prefixed = new List<PrefixedShardingSetting>
                {
                    new PrefixedShardingSetting
                    {
                        Prefix = "asia/",
                        Shards = new List<int> { 0 }
                    },
                    new PrefixedShardingSetting
                    {
                        Prefix = "eu/",
                        Shards = new List<int> { 1, 2 }
                    }
                };
            }
        });

        using (var session = store.OpenAsyncSession())
        {
            for (int i = 0; i < 10; i++)
            {
                string id = "asia/items/" + i;
                await session.StoreAsync(new Item(), id);
            }

            await session.SaveChangesAsync();
        }

        var task = store.Maintenance.SendAsync(new DeletePrefixedShardingSettingOperation("asia/"));
        var e = await Assert.ThrowsAsync<RavenException>(async () => await task);
        Assert.Contains(
            $"Cannot remove prefix 'asia/' from ShardingConfiguration.Prefixed. There are existing documents in database '{store.Database}' that start with 'asia/'",
            e.Message);
    }

    [RavenFact(RavenTestCategory.Sharding)]
    public async Task CanAddPrefixIfNoDocsStartingWith()
    {
        using var store = Sharding.GetDocumentStore(new Options
        {
            ModifyDatabaseRecord = record =>
            {
                record.Sharding ??= new ShardingConfiguration();
                record.Sharding.Prefixed = new List<PrefixedShardingSetting>
                {
                    new PrefixedShardingSetting
                    {
                        Prefix = "eu/",
                        Shards = new List<int> { 0 }
                    }
                };
            }
        });

        using (var session = store.OpenAsyncSession())
        {
            for (int i = 0; i < 10; i++)
            {
                string id = "eu/items/" + i;
                await session.StoreAsync(new Item(), id);
            }

            await session.SaveChangesAsync();
        }

        var record = await store.Maintenance.Server.SendAsync(new GetDatabaseRecordOperation(store.Database));
        var shardingConfiguration = record.Sharding;
        Assert.Equal(4, shardingConfiguration.BucketRanges.Count);

        await store.Maintenance.SendAsync(new AddPrefixedShardingSettingOperation(new PrefixedShardingSetting
        {
            Prefix = "asia/",
            Shards = new List<int> { 1, 2 }
        }));

        shardingConfiguration = await Sharding.GetShardingConfigurationAsync(store);

        Assert.Equal(2, shardingConfiguration.Prefixed.Count);
        Assert.Equal(ShardHelper.NumberOfBuckets * 2, shardingConfiguration.Prefixed[1].BucketRangeStart);
        Assert.Equal(6, shardingConfiguration.BucketRanges.Count);
    }

    [RavenFact(RavenTestCategory.Sharding)]
    public async Task CanDeletePrefixIfNoDocsStartingWith()
    {
        using var store = Sharding.GetDocumentStore(new Options
        {
            ModifyDatabaseRecord = record =>
            {
                record.Sharding ??= new ShardingConfiguration();
                record.Sharding.Prefixed = new List<PrefixedShardingSetting>
                {
                    new PrefixedShardingSetting
                    {
                        Prefix = "eu/",
                        Shards = new List<int> { 0, 1 }
                    }
                };
            }
        });

        using (var session = store.OpenAsyncSession())
        {
            for (int i = 0; i < 10; i++)
            {
                string id = "asia/items/" + i;
                await session.StoreAsync(new Item(), id);
            }

            await session.SaveChangesAsync();
        }

        var record = await store.Maintenance.Server.SendAsync(new GetDatabaseRecordOperation(store.Database));
        var shardingConfiguration = record.Sharding;
        Assert.Equal(5, shardingConfiguration.BucketRanges.Count);

        await store.Maintenance.SendAsync(new DeletePrefixedShardingSettingOperation("eu/"));

        shardingConfiguration = await Sharding.GetShardingConfigurationAsync(store);

        Assert.Equal(0, shardingConfiguration.Prefixed.Count);
        Assert.Equal(3, shardingConfiguration.BucketRanges.Count);
    }

    [RavenFact(RavenTestCategory.Sharding)]
    public async Task CanDeleteOnePrefixThenAddAnotherIfNoDocsStartingWith()
    {
        using var store = Sharding.GetDocumentStore(new Options
        {
            ModifyDatabaseRecord = record =>
            {
                record.Sharding ??= new ShardingConfiguration();
                record.Sharding.Prefixed = new List<PrefixedShardingSetting>
                {
                    new PrefixedShardingSetting
                    {
                        Prefix = "eu/",
                        Shards = new List<int> { 0, 1 }
                    },
                    new PrefixedShardingSetting
                    {
                        Prefix = "us/",
                        Shards = new List<int> { 1, 2 }
                    }
                };
            }
        });

        using (var session = store.OpenAsyncSession())
        {
            for (int i = 0; i < 10; i++)
            {
                string id = "asia/items/" + i;
                await session.StoreAsync(new Item(), id);
            }

            await session.SaveChangesAsync();
        }
        
        var record = await store.Maintenance.Server.SendAsync(new GetDatabaseRecordOperation(store.Database));
        var shardingConfiguration = record.Sharding;
        Assert.Equal(7, shardingConfiguration.BucketRanges.Count);

        // remove 'eu/' prefix
        await store.Maintenance.SendAsync(new DeletePrefixedShardingSettingOperation("eu/"));

        shardingConfiguration = await Sharding.GetShardingConfigurationAsync(store);
        Assert.Equal(1, shardingConfiguration.Prefixed.Count);
        Assert.Equal(5, shardingConfiguration.BucketRanges.Count);

        // add a new prefix
        await store.Maintenance.SendAsync(new AddPrefixedShardingSettingOperation(new PrefixedShardingSetting
        {
            Prefix = "africa/",
            Shards = new List<int> { 0, 2 }
        }));

        shardingConfiguration = await Sharding.GetShardingConfigurationAsync(store);
        Assert.Equal(2, shardingConfiguration.Prefixed.Count);
        Assert.Equal(7, shardingConfiguration.BucketRanges.Count);

        Assert.Equal(ShardHelper.NumberOfBuckets, shardingConfiguration.BucketRanges[3].BucketRangeStart);
        Assert.Equal(ShardHelper.NumberOfBuckets * 1.5, shardingConfiguration.BucketRanges[4].BucketRangeStart);
        Assert.Equal(ShardHelper.NumberOfBuckets * 2, shardingConfiguration.BucketRanges[5].BucketRangeStart);
        Assert.Equal(ShardHelper.NumberOfBuckets * 2.5, shardingConfiguration.BucketRanges[6].BucketRangeStart);
    }

    [RavenFact(RavenTestCategory.BackupExportImport | RavenTestCategory.Sharding)]
    public async Task BackupAndRestoreShardedDatabase_ShouldPreservePrefixedSettingsAndBucketRanges()
    {
        using var store = Sharding.GetDocumentStore(new Options
        {
            ModifyDatabaseRecord = databaseRecord =>
            {
                databaseRecord.Sharding ??= new ShardingConfiguration();
                databaseRecord.Sharding.Prefixed = new List<PrefixedShardingSetting>
                {
                    new PrefixedShardingSetting
                    {
                        Prefix = "users/",
                        Shards = new List<int> { 0 }
                    },
                    new PrefixedShardingSetting
                    {
                        Prefix = "orders/",
                        Shards = new List<int> { 1 }
                    },
                    new PrefixedShardingSetting
                    {
                        Prefix = "employees/",
                        Shards = new List<int> { 2 }
                    }
                };
            }
        });

        var sharding = await Sharding.GetShardingConfigurationAsync(store);

        Assert.Equal(6, sharding.BucketRanges.Count);
        Assert.Equal(ShardHelper.NumberOfBuckets, sharding.BucketRanges[3].BucketRangeStart);
        Assert.Equal(ShardHelper.NumberOfBuckets * 2, sharding.BucketRanges[4].BucketRangeStart);
        Assert.Equal(ShardHelper.NumberOfBuckets * 3, sharding.BucketRanges[5].BucketRangeStart);

        using (var session = store.OpenAsyncSession())
        {
            for (int i = 0; i < 10; i++)
            {
                await session.StoreAsync(new User(), $"users/{i}");
                await session.StoreAsync(new Order(), $"orders/{i}");
                await session.StoreAsync(new Employee(), $"employees/{i}");
            }

            await session.SaveChangesAsync();
        }

        using (var session = store.OpenAsyncSession(database: ShardHelper.ToShardName(store.Database, 0)))
        {
            for (int i = 0; i < 10; i++)
            {
                var doc = await session.LoadAsync<User>($"users/{i}");
                Assert.NotNull(doc);
            }
        }

        using (var session = store.OpenAsyncSession(database: ShardHelper.ToShardName(store.Database, 1)))
        {
            for (int i = 0; i < 10; i++)
            {
                var doc = await session.LoadAsync<Order>($"orders/{i}");
                Assert.NotNull(doc);
            }
        }

        using (var session = store.OpenAsyncSession(database: ShardHelper.ToShardName(store.Database, 2)))
        {
            for (int i = 0; i < 10; i++)
            {
                var doc = await session.LoadAsync<Employee>($"employees/{i}");
                Assert.NotNull(doc);
            }
        }

        var bucketStats = new Dictionary<int, List<BucketStats>>();
        await foreach (var db in Sharding.GetShardsDocumentDatabaseInstancesFor(store))
        {
            using (db.DocumentsStorage.ContextPool.AllocateOperationContext(out DocumentsOperationContext ctx))
            using (ctx.OpenReadTransaction())
            {
                var stats = ShardedDocumentsStorage.GetBucketStatistics(ctx, 0, int.MaxValue).ToList();
                Assert.Equal(10, stats.Count);
                bucketStats[db.ShardNumber] = stats;
            }
        }

        var waitHandles = await Sharding.Backup.WaitForBackupToComplete(store);
        var backupPath = NewDataPath(suffix: "BackupFolder");
        var config = Backup.CreateBackupConfiguration(backupPath);

        await Sharding.Backup.UpdateConfigurationAndRunBackupAsync(Server, store, config);
        Assert.True(WaitHandle.WaitAll(waitHandles, TimeSpan.FromMinutes(1)));

        var dirs = Directory.GetDirectories(backupPath);
        Assert.Equal(3, dirs.Length);

        sharding = await Sharding.GetShardingConfigurationAsync(store);
        var settings = Sharding.Backup.GenerateShardRestoreSettings(dirs, sharding);

        // restore the database with a different name
        var restoredDatabaseName = $"restored_database-{Guid.NewGuid()}";
        using (Sharding.Backup.ReadOnly(backupPath))
        using (Backup.RestoreDatabase(store, new RestoreBackupConfiguration 
        {
            DatabaseName = restoredDatabaseName,
            ShardRestoreSettings = settings
        }, timeout: TimeSpan.FromSeconds(60)))
        {
            var newDatabaseRecord = await store.Maintenance.Server.SendAsync(new GetDatabaseRecordOperation(restoredDatabaseName));
            Assert.Equal(3, newDatabaseRecord.Sharding.Shards.Count);
            Assert.Equal(3, newDatabaseRecord.Sharding.Prefixed.Count);

            var usersPrefixSetting = newDatabaseRecord.Sharding.Prefixed[0];
            Assert.Equal("users/", usersPrefixSetting.Prefix);
            Assert.Equal(1, usersPrefixSetting.Shards.Count);
            Assert.Equal(0, usersPrefixSetting.Shards[0]);
            Assert.Equal(ShardHelper.NumberOfBuckets, usersPrefixSetting.BucketRangeStart);

            var ordersPrefixSetting = newDatabaseRecord.Sharding.Prefixed[1];
            Assert.Equal("orders/", ordersPrefixSetting.Prefix);
            Assert.Equal(1, ordersPrefixSetting.Shards.Count);
            Assert.Equal(1, ordersPrefixSetting.Shards[0]);
            Assert.Equal(ShardHelper.NumberOfBuckets * 2, ordersPrefixSetting.BucketRangeStart);

            var employeesPrefixSetting = newDatabaseRecord.Sharding.Prefixed[2];
            Assert.Equal("employees/", employeesPrefixSetting.Prefix);
            Assert.Equal(1, employeesPrefixSetting.Shards.Count);
            Assert.Equal(2, employeesPrefixSetting.Shards[0]);
            Assert.Equal(ShardHelper.NumberOfBuckets * 3, employeesPrefixSetting.BucketRangeStart);

            Assert.Equal(6, newDatabaseRecord.Sharding.BucketRanges.Count);
            Assert.Equal(ShardHelper.NumberOfBuckets, newDatabaseRecord.Sharding.BucketRanges[3].BucketRangeStart);
            Assert.Equal(ShardHelper.NumberOfBuckets * 2, newDatabaseRecord.Sharding.BucketRanges[4].BucketRangeStart);
            Assert.Equal(ShardHelper.NumberOfBuckets * 3, newDatabaseRecord.Sharding.BucketRanges[5].BucketRangeStart);

            using (var session = store.OpenAsyncSession(database: restoredDatabaseName))
            {
                for (int i = 0; i < 10; i++)
                {
                    var user = await session.LoadAsync<User>($"users/{i}");
                    Assert.NotNull(user);

                    var order = await session.LoadAsync<Order>($"orders/{i}");
                    Assert.NotNull(order);

                    var employee = await session.LoadAsync<Employee>($"employees/{i}");
                    Assert.NotNull(employee);
                }
            }

            // assert valid bucket stats
            await foreach (var db in Sharding.GetShardsDocumentDatabaseInstancesFor(restoredDatabaseName))
            {
                using (db.DocumentsStorage.ContextPool.AllocateOperationContext(out DocumentsOperationContext ctx))
                using (ctx.OpenReadTransaction())
                {
                    var stats = ShardedDocumentsStorage.GetBucketStatistics(ctx, 0, int.MaxValue).ToList();
                    var originalStats = bucketStats[db.ShardNumber];

                    Assert.Equal(originalStats.Count, stats.Count);
                    for (int i = 0; i < stats.Count; i++)
                    {
                        Assert.Equal(originalStats[i].Bucket, stats[i].Bucket);
                        Assert.Equal(originalStats[i].NumberOfDocuments, stats[i].NumberOfDocuments);
                    }
                }
            }

            using (var session = store.OpenAsyncSession(database: restoredDatabaseName))
            {
                await session.StoreAsync(new User(), "users/11");
                await session.StoreAsync(new Order(), "orders/11");
                await session.StoreAsync(new Employee(), "employees/11");

                await session.SaveChangesAsync();
            }

            using (var session = store.OpenAsyncSession(database: ShardHelper.ToShardName(restoredDatabaseName, 0)))
            {
                var user = await session.LoadAsync<User>("users/11");
                Assert.NotNull(user);
            }

            using (var session = store.OpenAsyncSession(database: ShardHelper.ToShardName(restoredDatabaseName, 1)))
            {
                var order = await session.LoadAsync<Order>("orders/11");
                Assert.NotNull(order);
            }

            using (var session = store.OpenAsyncSession(database: ShardHelper.ToShardName(restoredDatabaseName, 2)))
            {
                var employee = await session.LoadAsync<Employee>("employees/11");
                Assert.NotNull(employee);
            }
        }
    }

    [RavenFact(RavenTestCategory.Sharding)]
    public async Task CanMoveOneBucketFromPrefixedRange()
    {
        using var store = Sharding.GetDocumentStore(new Options
        {
            ModifyDatabaseRecord = record =>
            {
                record.Sharding ??= new ShardingConfiguration();
                record.Sharding.Prefixed = new List<PrefixedShardingSetting>
                    {
                        new PrefixedShardingSetting
                        {
                            // bucket range for 'users/' is : 
                            // shard 0 : [1M, 1.5M]
                            // shard 1 : [1.5M, 2M]
                            Prefix = "users/",
                            Shards = new List<int> { 0, 1 }
                        }
                    };
            }
        });

        const string id = "users/1";
        using (var session = store.OpenAsyncSession())
        {
            var user = new User
            {
                Name = "Original shard"
            };
            await session.StoreAsync(user, id);
            await session.SaveChangesAsync();
        }

        var shardingConfiguration = await Sharding.GetShardingConfigurationAsync(store);

        Assert.Equal(5, shardingConfiguration.BucketRanges.Count);
        Assert.Equal(ShardHelper.NumberOfBuckets, shardingConfiguration.Prefixed[0].BucketRangeStart);

        var bucket = await Sharding.GetBucketAsync(store, id);

        var originalLocation = ShardHelper.GetShardNumberFor(shardingConfiguration, bucket);
        Assert.Contains(originalLocation, shardingConfiguration.Prefixed[0].Shards);
        var newLocation = shardingConfiguration.Prefixed[0].Shards.Single(s => s != originalLocation);

        await Sharding.Resharding.MoveShardForId(store, id);

        shardingConfiguration = await Sharding.GetShardingConfigurationAsync(store);
        Assert.Equal(7, shardingConfiguration.BucketRanges.Count);

        Assert.Equal(bucket, shardingConfiguration.BucketRanges[^2].BucketRangeStart);
        Assert.Equal(newLocation, shardingConfiguration.BucketRanges[^2].ShardNumber);

        Assert.Equal(bucket + 1, shardingConfiguration.BucketRanges[^1].BucketRangeStart);
        Assert.Equal(originalLocation, shardingConfiguration.BucketRanges[^1].ShardNumber);

        using (var session = store.OpenAsyncSession(ShardHelper.ToShardName(store.Database, originalLocation)))
        {
            var user = await session.LoadAsync<User>(id);
            Assert.Null(user);
        }
        using (var session = store.OpenAsyncSession(ShardHelper.ToShardName(store.Database, newLocation)))
        {
            var user = await session.LoadAsync<User>(id);
            Assert.Equal("Original shard", user.Name);
        }

        // the document will be written to the new location
        using (var session = store.OpenAsyncSession())
        {
            var user = await session.LoadAsync<User>(id);
            user.Name = "New shard";
            await session.SaveChangesAsync();
        }

        using (var session = store.OpenAsyncSession(ShardHelper.ToShardName(store.Database, originalLocation)))
        {
            var user = await session.LoadAsync<User>(id);
            Assert.Null(user);
        }

        using (var session = store.OpenAsyncSession(ShardHelper.ToShardName(store.Database, newLocation)))
        {
            var user = await session.LoadAsync<User>(id);
            Assert.Equal("New shard", user.Name);
        }
    }

    [RavenFact(RavenTestCategory.Sharding)]
    public async Task CanGetBucketStats_Prefixed()
    {
        using (var store = Sharding.GetDocumentStore(new Options
        {
            ModifyDatabaseRecord = record =>
            {
                record.Sharding ??= new ShardingConfiguration();
                record.Sharding.Prefixed = new List<PrefixedShardingSetting>
                   {
                       new PrefixedShardingSetting()
                       {
                           Prefix = "Users/",
                           Shards = new List<int> { 0 }
                       },
                       new PrefixedShardingSetting()
                       {
                           Prefix = "Orders/",
                           Shards = new List<int> { 1 }
                       }
                   };
            }
        }))
        {
            var before1 = DateTime.UtcNow;
            using (var session = store.OpenAsyncSession())
            {
                for (int i = 0; i < 10; i++)
                {
                    await session.StoreAsync(new User(), $"users/{i}/$abc");
                }

                await session.SaveChangesAsync();
            }
            var after1 = DateTime.UtcNow;
            using (var session = store.OpenAsyncSession())
            {
                for (int i = 0; i < 10; i++)
                {
                    await session.StoreAsync(new Order(), $"orders/{i}/$abc");
                }

                await session.SaveChangesAsync();
            }
            var after2 = DateTime.UtcNow;

            var shardingConfig = await Sharding.GetShardingConfigurationAsync(store);
            using (var allocator = new ByteStringContext(SharedMultipleUseFlag.None))
            {
                var id = "users/1/$abc";
                var shard = ShardHelper.GetShardNumberFor(shardingConfig, allocator, id);
                Assert.Equal(0, shard);

                var bucket = await Sharding.GetBucketAsync(store, id);

                var db = await GetDocumentDatabaseInstanceFor(store, ShardHelper.ToShardName(store.Database, shard));
                using (db.DocumentsStorage.ContextPool.AllocateOperationContext(out DocumentsOperationContext ctx))
                using (ctx.OpenReadTransaction())
                {
                    var stats = ShardedDocumentsStorage.GetBucketStatisticsFor(ctx, bucket);
                    Assert.Equal(bucket, stats.Bucket);
                    Assert.Equal(10, stats.NumberOfDocuments);
                    Assert.True(stats.LastModified > before1);
                    Assert.True(stats.LastModified < after1);
                }

                id = "orders/1/$abc";
                shard = ShardHelper.GetShardNumberFor(shardingConfig, allocator, id);
                Assert.Equal(1, shard);

                bucket = await Sharding.GetBucketAsync(store, id);

                db = await GetDocumentDatabaseInstanceFor(store, ShardHelper.ToShardName(store.Database, shard));
                using (db.DocumentsStorage.ContextPool.AllocateOperationContext(out DocumentsOperationContext ctx))
                using (ctx.OpenReadTransaction())
                {
                    var stats = ShardedDocumentsStorage.GetBucketStatisticsFor(ctx, bucket);
                    Assert.Equal(bucket, stats.Bucket);
                    Assert.Equal(10, stats.NumberOfDocuments);
                    Assert.True(stats.LastModified > after1);
                    Assert.True(stats.LastModified < after2);
                }
            }
        }
    }

    [RavenFact(RavenTestCategory.Sharding)]
    public void RavenDb_19737()
    {
        using var store = Sharding.GetDocumentStore(new Options
        {
            ModifyDatabaseRecord = record =>
            {
                record.Sharding ??= new ShardingConfiguration();
                record.Sharding.Prefixed = new List<PrefixedShardingSetting>
                {
                    new PrefixedShardingSetting
                    {
                        // range for 'eu/' is : 
                        // shard 0 : [1M, 2M]
                        Prefix = "eu/",
                        Shards = new List<int> { 0 }
                    },
                    new PrefixedShardingSetting
                    {
                        // range for 'asia/' is :
                        // shard 1 : [2M, 2.5M]
                        // shard 2 : [2.5M, 3M]
                        Prefix = "asia/",
                        Shards = new List<int> { 1, 2 }
                    }
                };
            }
        });

        using (Server.ServerStore.ContextPool.AllocateOperationContext(out TransactionOperationContext context))
        using (context.OpenReadTransaction())
        using (var raw = Server.ServerStore.Cluster.ReadRawDatabaseRecord(context, store.Database))
        {
            var rawPrefixed = raw.Sharding.Prefixed;

            Assert.Equal(2, rawPrefixed.Count);

            var euSetting = rawPrefixed[0];
            var asiaSetting = rawPrefixed[1];

            Assert.Equal("eu/", euSetting.Prefix);
            Assert.Equal(1, euSetting.Shards.Count);
            Assert.Equal(0, euSetting.Shards[0]);

            Assert.Equal("asia/", asiaSetting.Prefix);
            Assert.Equal(2, asiaSetting.Shards.Count);
            Assert.Equal(1, asiaSetting.Shards[0]);
            Assert.Equal(2, asiaSetting.Shards[1]);
        }
    }

    [RavenFact(RavenTestCategory.Sharding)]
    public async Task CanUpdatePrefixSetting()
    {
        using var store = Sharding.GetDocumentStore(new Options
        {
            ModifyDatabaseRecord = record =>
            {
                record.Sharding ??= new ShardingConfiguration();
                record.Sharding.Prefixed = new List<PrefixedShardingSetting>
                {
                    new PrefixedShardingSetting
                    {
                        Prefix = "eu/",
                        Shards = new List<int> { 0, 1 }
                    },
                    new PrefixedShardingSetting
                    {
                        Prefix = "us/",
                        Shards = new List<int> { 1, 2 }
                    }
                };
            }
        });

        using (var session = store.OpenAsyncSession())
        {
            for (int i = 0; i < 10; i++)
            {
                string id = "eu/users/" + i;
                await session.StoreAsync(new Item(), id);
            }

            await session.SaveChangesAsync();
        }

        var shardingConfiguration = await Sharding.GetShardingConfigurationAsync(store);
        var bucketRanges = shardingConfiguration.BucketRanges;
        Assert.Equal(7, bucketRanges.Count);


        Assert.Equal(ShardHelper.NumberOfBuckets, shardingConfiguration.BucketRanges[3].BucketRangeStart);
        Assert.Equal(ShardHelper.NumberOfBuckets * 1.5, shardingConfiguration.BucketRanges[4].BucketRangeStart);
        Assert.Equal(ShardHelper.NumberOfBuckets * 2, shardingConfiguration.BucketRanges[5].BucketRangeStart);
        Assert.Equal(ShardHelper.NumberOfBuckets * 2.5, shardingConfiguration.BucketRanges[6].BucketRangeStart);

        // update 'eu/' prefix setting : add shard #2

        await store.Maintenance.SendAsync(new UpdatePrefixedShardingSettingOperation(new PrefixedShardingSetting
        {
            Prefix = "eu/",
            Shards = new List<int> { 0, 1, 2 }
        }));

        shardingConfiguration = await Sharding.GetShardingConfigurationAsync(store);
        Assert.Equal(2, shardingConfiguration.Prefixed.Count);
        Assert.Equal("eu/", shardingConfiguration.Prefixed[1].Prefix);
        Assert.Equal(3, shardingConfiguration.Prefixed[1].Shards.Count);

        // shard #2 should get no bucket ranges for the 'eu/' prefix
        Assert.Equal(bucketRanges.Count, shardingConfiguration.BucketRanges.Count);
        for (int index = 0; index < bucketRanges.Count; index++)
        {
            ShardBucketRange oldRange = bucketRanges[index];
            var newRange = shardingConfiguration.BucketRanges[index];

            Assert.Equal(oldRange.BucketRangeStart, newRange.BucketRangeStart);
            Assert.Equal(oldRange.ShardNumber, newRange.ShardNumber);
        }

        // attempt to remove shard #1 from 'eu/' setting should throw
        // we cannot remove shard #1 because we have docs starting with 'eu/' on it

        await Assert.ThrowsAsync<RavenException>(async () =>
            await store.Maintenance.SendAsync(
                new UpdatePrefixedShardingSettingOperation(new PrefixedShardingSetting
                {
                    Prefix = "eu/", 
                    Shards = new List<int> { 0, 2 }
                })));

        // can delete shard #1 from 'us/' prefix setting (no docs starting with)
        await store.Maintenance.SendAsync(
            new UpdatePrefixedShardingSettingOperation(new PrefixedShardingSetting
            {
                Prefix = "us/",
                Shards = new List<int> { 2 }
            }));

        shardingConfiguration = await Sharding.GetShardingConfigurationAsync(store);
        Assert.Equal(2, shardingConfiguration.Prefixed.Count);

        // shard #1 should be removed from 'us/' prefix setting and shouldn't get any bucket ranges for 'us/' prefix
        // shard #2 should get the entire 'us/' prefixed bucket range (1M - 2M)
        Assert.Equal("us/", shardingConfiguration.Prefixed[0].Prefix);
        Assert.Equal(1, shardingConfiguration.Prefixed[0].Shards.Count);
        Assert.Equal(2, shardingConfiguration.Prefixed[0].Shards[0]);

        Assert.Equal(6, shardingConfiguration.BucketRanges.Count);

        Assert.Equal(ShardHelper.NumberOfBuckets * 1, shardingConfiguration.BucketRanges[3].BucketRangeStart);
        Assert.Equal(2, shardingConfiguration.BucketRanges[3].ShardNumber);

        Assert.Equal(ShardHelper.NumberOfBuckets * 2, shardingConfiguration.BucketRanges[4].BucketRangeStart);
        Assert.Equal(ShardHelper.NumberOfBuckets * 2.5, shardingConfiguration.BucketRanges[5].BucketRangeStart);
    }

    [RavenFact(RavenTestCategory.Sharding)]
    public async Task CanHandlePrefixOfPrefix()
    {
        using var store = Sharding.GetDocumentStore(new Options
        {
            ModifyDatabaseRecord = record =>
            {
                record.Sharding ??= new ShardingConfiguration();
                record.Sharding.Prefixed = new List<PrefixedShardingSetting>
                {
                    new PrefixedShardingSetting
                    {
                        Prefix = "users/",
                        Shards = new List<int> { 0 }
                    },
                    new PrefixedShardingSetting
                    {
                        Prefix = "users/us/utah/",
                        Shards = new List<int> { 1 }
                    },
                    new PrefixedShardingSetting
                    {
                        Prefix = "users/us/",
                        Shards = new List<int> { 2 }
                    }
                };
            }
        });

        using (var session = store.OpenAsyncSession())
        {
            for (int i = 0; i < 10; i++)
            {
                string id = "users/us/california/" + i;
                await session.StoreAsync(new Item(), id);
            }

            await session.SaveChangesAsync();
        }

        using (var session = store.OpenAsyncSession(database: ShardHelper.ToShardName(store.Database, 0)))
        {
            var docs = (await session.Advanced.LoadStartingWithAsync<User>("users/us/")).ToList();
            Assert.Equal(0, docs.Count);
        }

        using (var session = store.OpenAsyncSession(database: ShardHelper.ToShardName(store.Database, 1)))
        {
            var docs = (await session.Advanced.LoadStartingWithAsync<User>("users/us/")).ToList();
            Assert.Equal(0, docs.Count);
        }

        using (var session = store.OpenAsyncSession(database: ShardHelper.ToShardName(store.Database, 2)))
        {
            var docs = (await session.Advanced.LoadStartingWithAsync<User>("users/us/")).ToList();
            Assert.Equal(10, docs.Count);
        }

        var shardingConfiguration = await Sharding.GetShardingConfigurationAsync(store);
        Assert.Equal(3, shardingConfiguration.Prefixed.Count);

        // should be sorted by descending 
        Assert.Equal("users/us/utah/", shardingConfiguration.Prefixed[0].Prefix);
        Assert.Equal("users/us/", shardingConfiguration.Prefixed[1].Prefix);
        Assert.Equal("users/", shardingConfiguration.Prefixed[2].Prefix);

        var bucketRanges = shardingConfiguration.BucketRanges;
        Assert.Equal(6, bucketRanges.Count);
        Assert.Equal(ShardHelper.NumberOfBuckets, bucketRanges[3].BucketRangeStart);
        Assert.Equal(ShardHelper.NumberOfBuckets * 2, bucketRanges[4].BucketRangeStart);
        Assert.Equal(ShardHelper.NumberOfBuckets * 3, bucketRanges[5].BucketRangeStart);

        Assert.Equal(ShardHelper.NumberOfBuckets, shardingConfiguration.Prefixed[0].BucketRangeStart);
        Assert.Equal(ShardHelper.NumberOfBuckets * 2, shardingConfiguration.Prefixed[1].BucketRangeStart);
        Assert.Equal(ShardHelper.NumberOfBuckets * 3, shardingConfiguration.Prefixed[2].BucketRangeStart);

        // add 'users/us/arizona/' prefix setting
        await store.Maintenance.SendAsync(new AddPrefixedShardingSettingOperation(new PrefixedShardingSetting
        {
            Prefix = "users/us/arizona/",
            Shards = new List<int> { 1 }
        }));

        shardingConfiguration = await Sharding.GetShardingConfigurationAsync(store);
        Assert.Equal(4, shardingConfiguration.Prefixed.Count);

        // should still be sorted
        Assert.Equal("users/us/utah/", shardingConfiguration.Prefixed[0].Prefix);
        Assert.Equal("users/us/arizona/", shardingConfiguration.Prefixed[1].Prefix);
        Assert.Equal("users/us/", shardingConfiguration.Prefixed[2].Prefix);
        Assert.Equal("users/", shardingConfiguration.Prefixed[3].Prefix);

        Assert.Equal(ShardHelper.NumberOfBuckets, shardingConfiguration.Prefixed[0].BucketRangeStart);
        
        // new prefix should be added at the end of BucketRanges
        Assert.Equal(ShardHelper.NumberOfBuckets * 4, shardingConfiguration.Prefixed[1].BucketRangeStart);

        Assert.Equal(ShardHelper.NumberOfBuckets * 2, shardingConfiguration.Prefixed[2].BucketRangeStart);
        Assert.Equal(ShardHelper.NumberOfBuckets * 3, shardingConfiguration.Prefixed[3].BucketRangeStart);

        // check bucket ranges

        var newBucketRanges = shardingConfiguration.BucketRanges;
        Assert.Equal(7, newBucketRanges.Count);

        for (int i = 0; i < bucketRanges.Count; i++)
        {
            var oldRange = bucketRanges[i];
            var newRange = newBucketRanges[i];

            Assert.Equal(oldRange.BucketRangeStart, newRange.BucketRangeStart);
            Assert.Equal(oldRange.ShardNumber, newRange.ShardNumber);
        }

        Assert.Equal(ShardHelper.NumberOfBuckets * 4, newBucketRanges[6].BucketRangeStart);
        Assert.Equal(1, newBucketRanges[6].ShardNumber);

        // should all go to shard #1
        using (var session = store.OpenAsyncSession())
        {
            for (int i = 0; i < 10; i++)
            {
                string id = "users/us/arizona/" + i;
                await session.StoreAsync(new Item(), id);
            }

            await session.SaveChangesAsync();

            WaitForUserToContinueTheTest(store);
        }

        using (var session = store.OpenAsyncSession(database: ShardHelper.ToShardName(store.Database, 0)))
        {
            var docs = (await session.Advanced.LoadStartingWithAsync<User>("users/us/arizona/")).ToList();
            Assert.Equal(0, docs.Count);
        }

        using (var session = store.OpenAsyncSession(database: ShardHelper.ToShardName(store.Database, 1)))
        {
            var docs = (await session.Advanced.LoadStartingWithAsync<User>("users/us/arizona/")).ToList();
            Assert.Equal(10, docs.Count);
        }

        using (var session = store.OpenAsyncSession(database: ShardHelper.ToShardName(store.Database, 2)))
        {
            var docs = (await session.Advanced.LoadStartingWithAsync<User>("users/us/arizona/")).ToList();
            Assert.Equal(0, docs.Count);
        }

    }

    [RavenFact(RavenTestCategory.Sharding)]
    public async Task AfterAddingNewPrefixMatchingDocsShouldNotGoToWrongShard()
    {
        using var store = Sharding.GetDocumentStore(new Options
        {
            ModifyDatabaseRecord = record =>
            {
                record.Sharding ??= new ShardingConfiguration();
                record.Sharding.Prefixed = new List<PrefixedShardingSetting>
                {
                    new PrefixedShardingSetting
                    {
                        Prefix = "users/",
                        Shards = new List<int> { 0 }
                    },
                    new PrefixedShardingSetting
                    {
                        Prefix = "users/eu/",
                        Shards = new List<int> { 0 }
                    },
                    new PrefixedShardingSetting
                    {
                        Prefix = "users/asia/",
                        Shards = new List<int> { 2 }
                    },
                    new PrefixedShardingSetting
                    {
                        Prefix = "users/africa/",
                        Shards = new List<int> { 2 }
                    }
                };
            }
        });

        await foreach (var shard in Sharding.GetShardsDocumentDatabaseInstancesFor(store))
        {
            shard.ForTestingPurposes ??= new DocumentDatabase.TestingStuff
            {
                EnableWritesToTheWrongShard = true
            };
        }

        using (var session = store.OpenAsyncSession())
        {
            for (int i = 0; i < 10; i++)
            {
                string id = "users/eu/" + i;
                await session.StoreAsync(new Item(), id);
            }

            await session.SaveChangesAsync();
        }

        // add 'users/us/' prefix setting
        await store.Maintenance.SendAsync(new AddPrefixedShardingSettingOperation(new PrefixedShardingSetting
        {
            Prefix = "users/us/",
            Shards = new List<int> { 1 }
        }));

        // should all go to shard #1
        using (var session = store.OpenAsyncSession())
        {
            for (int i = 0; i < 10; i++)
            {
                string id = "users/us/" + i;
                await session.StoreAsync(new Item(), id);
            }

            await session.SaveChangesAsync();
        }

        using (var session = store.OpenAsyncSession(database: ShardHelper.ToShardName(store.Database, 0)))
        {
            var docs = (await session.Advanced.LoadStartingWithAsync<User>("users/us/")).ToList();
            Assert.Equal(0, docs.Count);
        }

        using (var session = store.OpenAsyncSession(database: ShardHelper.ToShardName(store.Database, 1)))
        {
            var docs = (await session.Advanced.LoadStartingWithAsync<User>("users/us/")).ToList();
            Assert.Equal(10, docs.Count);
        }

        using (var session = store.OpenAsyncSession(database: ShardHelper.ToShardName(store.Database, 2)))
        {
            var docs = (await session.Advanced.LoadStartingWithAsync<User>("users/us/")).ToList();
            Assert.Equal(0, docs.Count);
        }

    }

    private class Item
    {
#pragma warning disable CS0649
        public string Id;
#pragma warning restore CS0649
    }
}
