using System;
using System.Collections.Generic;
using System.IO;
using System.Linq;
using System.Threading;
using System.Threading.Tasks;
using Raven.Client.Documents;
using Raven.Client.Documents.Operations.Backups;
using Raven.Client.Documents.Smuggler;
using Raven.Client.Exceptions;
using Raven.Client.ServerWide.Operations;
using Raven.Client.ServerWide.Sharding;
using Raven.Client.Util;
using Raven.Server.Documents;
using Raven.Server.Documents.Sharding;
using Raven.Server.Rachis;
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

public class PrefixedSharding : ClusterTestBase
{
    public PrefixedSharding(ITestOutputHelper output) : base(output)
    {
    }

    [RavenFact(RavenTestCategory.Sharding)]
    public async Task CanShardByDocumentsPrefix()
    {
        using var store = Sharding.GetDocumentStore(new Options
        {
            ModifyDatabaseRecord = record =>
            {
                record.Sharding ??= new ShardingConfiguration();
                record.Sharding.Prefixed =
                [
                    new PrefixedShardingSetting
                    {
                        // range for 'eu/' is : 
                        // shard 0 : [1M, 2M]
                        Prefix = "eu/", 
                        Shards = [0]
                    },
                    new PrefixedShardingSetting
                    {
                        // range for 'asia/' is :
                        // shard 1 : [2M, 2.5M]
                        // shard 2 : [2.5M, 3M]
                        Prefix = "asia/", 
                        Shards = [1, 2]
                    }
                ];
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

        var rand = new Random(2022_04_19);
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
            Shards = [1, 2]
        }));

        var e = await Assert.ThrowsAsync<RavenException>(async () => await task);
        Assert.Contains(
            "Cannot add prefix 'asia' to ShardingConfiguration.Prefixed. In order to define sharding by prefix, the prefix string must end with '/' or '-' characters",
            e.Message);
    }

    [RavenFact(RavenTestCategory.Sharding)]
    public async Task ShouldThrowOnPrefixSettingWithNoShards()
    {
        using var store = Sharding.GetDocumentStore();

        await Assert.ThrowsAsync<RavenException>(async () => await store.Maintenance.SendAsync(new AddPrefixedShardingSettingOperation(new PrefixedShardingSetting
        {
            Prefix = "users/",
            Shards = []
        })));

        await store.Maintenance.SendAsync(new AddPrefixedShardingSettingOperation(new PrefixedShardingSetting
        {
            Prefix = "users/",
            Shards = [0]
        }));
          
        Assert.Throws<RavenException>(() =>
        {
            using var newStore = Sharding.GetDocumentStore(new Options
            {
                ModifyDatabaseRecord = record =>
                {
                    record.Sharding ??= new ShardingConfiguration();
                    record.Sharding.Prefixed =
                    [
                        new PrefixedShardingSetting
                        {
                            Prefix = "users/",
                            Shards = []
                        }
                    ];
                }
            });
        });
          

    }

    [RavenFact(RavenTestCategory.Sharding)]
    public async Task ShouldNotAllowToAddPrefixIfWeHaveDocsStartingWith()
    {
        using var store = Sharding.GetDocumentStore(new Options
        {
            ModifyDatabaseRecord = record =>
            {
                record.Sharding ??= new ShardingConfiguration();
                record.Sharding.Prefixed =
                [
                    new PrefixedShardingSetting
                    {
                        Prefix = "eu/", 
                        Shards = [0]
                    }
                ];
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
            Shards = [1, 2]
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
                record.Sharding.Prefixed =
                [
                    new PrefixedShardingSetting
                    {
                        Prefix = "asia/", 
                        Shards = [0]
                    },
                    new PrefixedShardingSetting
                    {
                        Prefix = "eu/", 
                        Shards = [1, 2]
                    }
                ];
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
                record.Sharding.Prefixed =
                [
                    new PrefixedShardingSetting
                    {
                        Prefix = "eu/", 
                        Shards = [0]
                    }
                ];
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
            Shards = [1, 2]
        }));

        shardingConfiguration = await Sharding.GetShardingConfigurationAsync(store);

        Assert.Equal(2, shardingConfiguration.Prefixed.Count);
        Assert.Equal(ShardHelper.NumberOfBuckets * 2, shardingConfiguration.Prefixed[1].BucketRangeStart);
        Assert.Equal(6, shardingConfiguration.BucketRanges.Count);

        // check that we can add prefixes even if none were defined in database creation 
        var newStore = Sharding.GetDocumentStore();
        await newStore.Maintenance.SendAsync(new AddPrefixedShardingSettingOperation(new PrefixedShardingSetting
        {
            Prefix = "users/",
            Shards = [0, 2]
        }));

        shardingConfiguration = await Sharding.GetShardingConfigurationAsync(newStore);
        Assert.Equal(1, shardingConfiguration.Prefixed.Count);

    }

    [RavenFact(RavenTestCategory.Sharding)]
    public async Task CanDeletePrefixIfNoDocsStartingWith()
    {
        using var store = Sharding.GetDocumentStore(new Options
        {
            ModifyDatabaseRecord = record =>
            {
                record.Sharding ??= new ShardingConfiguration();
                record.Sharding.Prefixed =
                [
                    new PrefixedShardingSetting
                    {
                        Prefix = "eu/", 
                        Shards = [0, 1]
                    }
                ];
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
                record.Sharding.Prefixed =
                [
                    new PrefixedShardingSetting
                    {
                        Prefix = "eu/", 
                        Shards = [0, 1]
                    },
                    new PrefixedShardingSetting
                    {
                        Prefix = "us/", 
                        Shards = [1, 2]
                    }
                ];
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
            Shards = [0, 2]
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
                databaseRecord.Sharding.Prefixed =
                [
                    new PrefixedShardingSetting
                    {
                        Prefix = "users/", 
                        Shards = [0]
                    },
                    new PrefixedShardingSetting
                    {
                        Prefix = "orders/", 
                        Shards = [1]
                    },
                    new PrefixedShardingSetting
                    {
                        Prefix = "employees/", 
                        Shards = [2]
                    }
                ];
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
    public async Task CanGetBucketStats_Prefixed()
    {
        using (var store = Sharding.GetDocumentStore(new Options
        {
            ModifyDatabaseRecord = record =>
            {
                record.Sharding ??= new ShardingConfiguration();
                record.Sharding.Prefixed =
                [
                    new PrefixedShardingSetting
                    {
                        Prefix = "Users/", 
                        Shards = [0]
                    },
                    new PrefixedShardingSetting
                    {
                        Prefix = "Orders/", 
                        Shards = [1]
                    }
                ];
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
                record.Sharding.Prefixed =
                [
                    new PrefixedShardingSetting
                    {
                        // range for 'eu/' is : 
                        // shard 0 : [1M, 2M]
                        Prefix = "eu/", 
                        Shards = [0]
                    },

                    new PrefixedShardingSetting
                    {
                        // range for 'asia/' is :
                        // shard 1 : [2M, 2.5M]
                        // shard 2 : [2.5M, 3M]
                        Prefix = "asia/", 
                        Shards = [1, 2]
                    }
                ];
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
                record.Sharding.Prefixed =
                [
                    new PrefixedShardingSetting
                    {
                        Prefix = "eu/", 
                        Shards = [0, 1]
                    },
                    new PrefixedShardingSetting
                    {
                        Prefix = "us/", 
                        Shards = [1, 2]
                    }
                ];
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

        // attempt to remove shard #1 from 'us/' setting should throw
        // we cannot remove shard #1 because there are bucket ranges mapped to shard #1 for this prefix

        await Assert.ThrowsAsync<RavenException>(async () => 
            await store.Maintenance.SendAsync(
            new UpdatePrefixedShardingSettingOperation(new PrefixedShardingSetting
            {
                Prefix = "us/", Shards = [2]
            })));

    }

    [RavenFact(RavenTestCategory.Sharding)]
    public async Task CanHandlePrefixOfPrefix()
    {
        using var store = Sharding.GetDocumentStore(new Options
        {
            ModifyDatabaseRecord = record =>
            {
                record.Sharding ??= new ShardingConfiguration();
                record.Sharding.Prefixed =
                [
                    new PrefixedShardingSetting
                    {
                        Prefix = "users/", 
                        Shards = [0]
                    },
                    new PrefixedShardingSetting
                    {
                        Prefix = "users/us/utah/", 
                        Shards = [1]
                    },
                    new PrefixedShardingSetting
                    {
                        Prefix = "users/us/", 
                        Shards = [2]
                    }
                ];
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
            Shards = [1]
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
                record.Sharding.Prefixed =
                [
                    new PrefixedShardingSetting
                    {
                        Prefix = "users/", 
                        Shards = [0]
                    },

                    new PrefixedShardingSetting
                    {
                        Prefix = "users/eu/", 
                        Shards = [0]
                    },

                    new PrefixedShardingSetting
                    {
                        Prefix = "users/asia/", 
                        Shards = [2]
                    },

                    new PrefixedShardingSetting
                    {
                        Prefix = "users/africa/", 
                        Shards = [2]
                    }
                ];
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
            Shards = [1]
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

    [RavenFact(RavenTestCategory.Sharding)]
    public async Task PrefixesOperationsShouldBeCaseInsensitive()
    {
        using var store = Sharding.GetDocumentStore(new Options
        {
            ModifyDatabaseRecord = record =>
            {
                record.Sharding ??= new ShardingConfiguration();
                record.Sharding.Prefixed =
                [
                    new PrefixedShardingSetting
                    {
                        Prefix = "Users/", 
                        Shards = [0, 1]
                    },
                    new PrefixedShardingSetting
                    {
                        Prefix = "Companies/", 
                        Shards = [0, 1, 2]
                    }
                ];
            }
        });


        await store.Maintenance.SendAsync(new UpdatePrefixedShardingSettingOperation(new PrefixedShardingSetting
        {
            Prefix = "users/",
            Shards = [0, 1, 2]
        }));

        var shardingConfiguration = await Sharding.GetShardingConfigurationAsync(store);

        Assert.Equal(2, shardingConfiguration.Prefixed.Count);
        Assert.Equal("Users/", shardingConfiguration.Prefixed[0].Prefix);
        Assert.Equal(new[] { 0, 1, 2 }, shardingConfiguration.Prefixed[0].Shards);

        await store.Maintenance.SendAsync(new DeletePrefixedShardingSettingOperation("COMPANIES/"));

        shardingConfiguration = await Sharding.GetShardingConfigurationAsync(store);

        Assert.Equal(1, shardingConfiguration.Prefixed.Count);

        await store.Maintenance.SendAsync(new AddPrefixedShardingSettingOperation(new PrefixedShardingSetting
        {
            Prefix = "products/",
            Shards = new List<int> { 2 }
        }));

        using (var session = store.OpenAsyncSession())
        {
            for (int i = 0; i < 10; i++)
            {
                var id = $"Products/{i}";
                await session.StoreAsync(new Item(), id);
            }

            await session.SaveChangesAsync();
        }

        using (var session = store.OpenAsyncSession(database: ShardHelper.ToShardName(store.Database, 0)))
        {
            var docs = (await session.Advanced.LoadStartingWithAsync<User>("PRODUCTS/")).ToList();
            Assert.Equal(0, docs.Count);
        }

        using (var session = store.OpenAsyncSession(database: ShardHelper.ToShardName(store.Database, 1)))
        {
            var docs = (await session.Advanced.LoadStartingWithAsync<User>("PRODUCTS/")).ToList();
            Assert.Equal(0, docs.Count);
        }

        using (var session = store.OpenAsyncSession(database: ShardHelper.ToShardName(store.Database, 2)))
        {
            var docs = (await session.Advanced.LoadStartingWithAsync<User>("PRODUCTS/")).ToList();
            Assert.Equal(10, docs.Count);
        }

        var task = store.Maintenance.SendAsync(new UpdatePrefixedShardingSettingOperation(new PrefixedShardingSetting()
        {
            Prefix = "Products/", 
            Shards = new List<int>() { 1 }
        }));
        await Assert.ThrowsAsync<RavenException>(async () => await task);
    }

    [RavenFact(RavenTestCategory.Sharding)]
    public async Task UpdatePrefixesInCluster()
    {
        var cluster = await CreateRaftCluster(3, watcherCluster: true);
        var options = Sharding.GetOptionsForCluster(cluster.Leader, shards: 3, shardReplicationFactor: 1, orchestratorReplicationFactor: 3);
        options.ModifyDatabaseRecord += record =>
        {
            record.Sharding ??= new ShardingConfiguration();
            record.Sharding.Prefixed =
            [
                new PrefixedShardingSetting
                {
                    Prefix = "users/", 
                    Shards = [0]
                },
                new PrefixedShardingSetting
                {
                    Prefix = "users/us/utah/", 
                    Shards = [1]
                },
                new PrefixedShardingSetting
                {
                    Prefix = "users/us/", 
                    Shards = [2]
                }
            ];
        };
        using var store = GetDocumentStore(options);

        using (var session = store.OpenAsyncSession())
        {
            for (int i = 0; i < 10; i++)
            {
                await session.StoreAsync(new Item(), "users/eu/sweden/" + i);
                await session.StoreAsync(new Item(), "users/us/utah/" + i);
                await session.StoreAsync(new Item(), "users/us/california/" + i);
            }

            await session.SaveChangesAsync();
        }

        //var stores = Cluster.GetDocumentStores()

        using (var session = store.OpenAsyncSession(database: ShardHelper.ToShardName(store.Database, 0)))
        {
            var docs = (await session.Advanced.LoadStartingWithAsync<User>("users/")).ToList();
            Assert.Equal(10, docs.Count);

            foreach (var doc in docs)
            {
                Assert.StartsWith("users/eu/sweden/", doc.Id);
            }
        }

        using (var session = store.OpenAsyncSession(database: ShardHelper.ToShardName(store.Database, 1)))
        {
            var docs = (await session.Advanced.LoadStartingWithAsync<User>("users/")).ToList();
            Assert.Equal(10, docs.Count);

            foreach (var doc in docs)
            {
                Assert.StartsWith("users/us/utah/", doc.Id);
            }
        }

        using (var session = store.OpenAsyncSession(database: ShardHelper.ToShardName(store.Database, 2)))
        {
            var docs = (await session.Advanced.LoadStartingWithAsync<User>("users/")).ToList();
            Assert.Equal(10, docs.Count);

            foreach (var doc in docs)
            {
                Assert.StartsWith("users/us/california/", doc.Id);
            }
        }

        // add 'users/us/arizona/' prefix setting
        await store.Maintenance.SendAsync(new AddPrefixedShardingSettingOperation(new PrefixedShardingSetting
        {
            Prefix = "users/us/arizona/",
            Shards = new List<int> { 1 }
        }));

        using (var session = store.OpenAsyncSession())
        {
            for (int i = 0; i < 10; i++)
            {
                string id = "users/us/arizona/" + i;
                await session.StoreAsync(new Item(), id);
            }

            await session.SaveChangesAsync();
        }

        var stores = GetDocumentStores(cluster.Nodes, store.Database, disableTopologyUpdates: true);
        foreach (var s in stores)
        {
            using (var session = s.OpenAsyncSession())
            {
                var docs = (await session.Advanced.LoadStartingWithAsync<User>("users/us/arizona/")).ToList();
                Assert.Equal(10, docs.Count);
            }
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
    public async Task CanMoveOneBucketFromPrefixedRange()
    {
        using var store = Sharding.GetDocumentStore(new Options
        {
            ModifyDatabaseRecord = record =>
            {
                record.Sharding ??= new ShardingConfiguration();
                record.Sharding.Prefixed =
                [
                    new PrefixedShardingSetting
                    {
                        // bucket range for 'users/' is : 
                        // shard 0 : [1M, 1.5M]
                        // shard 1 : [1.5M, 2M]
                        Prefix = "users/", 
                        Shards = [0, 1]
                    }
                ];
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
    public async Task CanMoveOneBucketFromPrefixedRangeToNewShard()
    {
        var (_, leader) = await CreateRaftCluster(3, watcherCluster: true);
        var options = Sharding.GetOptionsForCluster(leader, shards: 2, shardReplicationFactor: 2, orchestratorReplicationFactor: 2);
        options.ModifyDatabaseRecord += record =>
        {
            record.Sharding.Prefixed =
            [
                new PrefixedShardingSetting
                {
                    Prefix = "foo/", 
                    Shards = [0]
                }
            ];

        };

        using (var store = GetDocumentStore(options))
        {
            var record = await store.Maintenance.Server.SendAsync(new GetDatabaseRecordOperation(store.Database));
            var shardTopology = record.Sharding.Shards[0];
            Assert.Equal(2, shardTopology.Members.Count);
            Assert.Equal(0, shardTopology.Promotables.Count);
            Assert.Equal(2, shardTopology.ReplicationFactor);

            //create new shard
            var res = store.Maintenance.Server.Send(new AddDatabaseShardOperation(store.Database));
            var newShardNumber = res.ShardNumber;
            Assert.Equal(2, newShardNumber);
            Assert.Equal(2, res.ShardTopology.ReplicationFactor);
            Assert.Equal(2, res.ShardTopology.AllNodes.Count());
            await Cluster.WaitForRaftIndexToBeAppliedInClusterAsync(res.RaftCommandIndex);

            await AssertWaitForValueAsync(async () =>
            {
                record = await store.Maintenance.Server.SendAsync(new GetDatabaseRecordOperation(store.Database));
                return record.Sharding.Shards.Count;
            }, 3);

            await AssertWaitForValueAsync(async () =>
            {
                record = await store.Maintenance.Server.SendAsync(new GetDatabaseRecordOperation(store.Database));
                record.Sharding.Shards.TryGetValue(newShardNumber, out shardTopology);
                return shardTopology?.Members?.Count;
            }, 2);

            var nodesContainingNewShard = shardTopology.Members;

            foreach (var node in nodesContainingNewShard)
            {
                var serverWithNewShard = Servers.Single(x => x.ServerStore.NodeTag == node);
                Assert.True(serverWithNewShard.ServerStore.DatabasesLandlord.DatabasesCache.TryGetValue(ShardHelper.ToShardName(store.Database, newShardNumber), out _));
            }

            var id = "foo/bar";
            var bucket = await Sharding.GetBucketAsync(store, id);
            var originalDocShard = await Sharding.GetShardNumberForAsync(store, id);
            Assert.Equal(0, originalDocShard);

            using (var session = store.OpenAsyncSession())
            {
                session.Advanced.WaitForReplicationAfterSaveChanges(replicas: 1);

                var user = new User
                {
                    Name = "Original shard"
                };
                await session.StoreAsync(user, id);
                await session.SaveChangesAsync();
            }

            using (var session = store.OpenAsyncSession(ShardHelper.ToShardName(store.Database, originalDocShard)))
            {
                var user = await session.LoadAsync<User>(id);
                Assert.NotNull(user);
            }

            // first we need to add the new shard to the prefix setting
            await store.Maintenance.SendAsync(new UpdatePrefixedShardingSettingOperation(new PrefixedShardingSetting
            {
                Prefix = "foo/", 
                Shards = [0, newShardNumber]
            }));

            // move bucket
            await Sharding.Resharding.MoveShardForId(store, id, newShardNumber);

            var exists = WaitForDocument<User>(store, id, predicate: null, database: ShardHelper.ToShardName(store.Database, newShardNumber));
            Assert.True(exists);

            using (var session = store.OpenAsyncSession(ShardHelper.ToShardName(store.Database, newShardNumber)))
            {
                var user = await session.LoadAsync<User>(id);
                Assert.NotNull(user);
            }

            // check bucket ranges
            var sharding = await Sharding.GetShardingConfigurationAsync(store);
            Assert.Equal(5, sharding.BucketRanges.Count);
            Assert.Equal(ShardHelper.NumberOfBuckets ,sharding.BucketRanges[2].BucketRangeStart);
            Assert.Equal(bucket, sharding.BucketRanges[3].BucketRangeStart);
            Assert.Equal(bucket + 1, sharding.BucketRanges[4].BucketRangeStart);

            // the document will be written to the new location
            using (var session = store.OpenAsyncSession())
            {
                var user = await session.LoadAsync<User>(id);
                user.Name = "New shard";
                await session.SaveChangesAsync();
            }

            using (var session = store.OpenAsyncSession(ShardHelper.ToShardName(store.Database, originalDocShard)))
            {
                var user = await session.LoadAsync<User>(id);
                Assert.Null(user);
            }
            using (var session = store.OpenAsyncSession(ShardHelper.ToShardName(store.Database, newShardNumber)))
            {
                var user = await session.LoadAsync<User>(id);
                Assert.Equal("New shard", user.Name);
            }
        }
    }

    [RavenFact(RavenTestCategory.Sharding)]
    public async Task ShouldThrowOnAttemptToMovePrefixedBucketToShardNotInPrefixSetting()
    {
        using var store = Sharding.GetDocumentStore(new Options
        {
            ModifyDatabaseRecord = record =>
            {
                record.Sharding ??= new ShardingConfiguration();
                record.Sharding.Prefixed =
                [
                    new PrefixedShardingSetting
                    {
                        Prefix = "users/", 
                        Shards = [0, 1]
                    }
                ];
            }
        });

        var shardingConfig = await Sharding.GetShardingConfigurationAsync(store);
        var bucket =  Sharding.GetBucket(shardingConfig, "users/1");

        // shard #2 is not a part of Prefixed['users/'].Shards 
        await Assert.ThrowsAsync<RachisApplyException>(async ()=> 
            await Server.ServerStore.Sharding.StartBucketMigration(store.Database, bucket, toShard : 2, prefix: "users/", RaftIdGenerator.NewId()));
    }

    [RavenFact(RavenTestCategory.Sharding)]
    public async Task ShardByDocumentsPrefixWithManyDocs_CanMoveBigBucketToNewShard()
    {
        using var store = Sharding.GetDocumentStore(new Options
        {
            ModifyDatabaseRecord = record =>
            {
                record.Sharding ??= new ShardingConfiguration();
                record.Sharding.Prefixed =
                [
                    new PrefixedShardingSetting
                    {
                        Prefix = "users/",
                        Shards = [0 , 1]
                    }
                ];
            }
        });

        const string bigBucketId = "users/123";
        using (var bulk = store.BulkInsert())
        {
            for (int i = 0; i < 100_000; i++)
            {
                var id = $"users/{i}";
                bulk.Store(new User(), id);
                bulk.Store(new User(), $"{id}${bigBucketId}");
            }
        }

        using (var session = store.OpenAsyncSession(database: ShardHelper.ToShardName(store.Database, 0)))
        {
            var numberOfDocs = (await session.Advanced.LoadStartingWithAsync<User>("users/", pageSize: int.MaxValue)).Count();
            Assert.Equal(149724, numberOfDocs);
        }

        using (var session = store.OpenAsyncSession(database: ShardHelper.ToShardName(store.Database, 1)))
        {
            var numberOfDocs = (await session.Advanced.LoadStartingWithAsync<User>("users/", pageSize: int.MaxValue)).Count();
            Assert.Equal(50276, numberOfDocs);
        }

        using (var session = store.OpenAsyncSession(database: ShardHelper.ToShardName(store.Database, 2)))
        {
            var numberOfDocs = (await session.Advanced.LoadStartingWithAsync<User>("users/", pageSize: int.MaxValue)).Count();
            Assert.Equal(0, numberOfDocs);
        }

        int bucket, shardNumber;
        var record = await store.Maintenance.Server.SendAsync(new GetDatabaseRecordOperation(store.Database));
        using (var allocator = new ByteStringContext(SharedMultipleUseFlag.None))
            (shardNumber, bucket) =  ShardHelper.GetShardNumberAndBucketFor(record.Sharding, allocator, bigBucketId);

        var shard = await GetDocumentDatabaseInstanceFor(store, ShardHelper.ToShardName(store.Database, shardNumber));

        using (shard.DocumentsStorage.ContextPool.AllocateOperationContext(out DocumentsOperationContext ctx))
        using (ctx.OpenReadTransaction())
        {
            var stats = ShardedDocumentsStorage.GetBucketStatisticsFor(ctx, bucket);
            Assert.Equal(30303958, stats.Size);
            Assert.Equal(100_001, stats.NumberOfDocuments);
        }

        // add shard #2 to prefix setting
        await store.Maintenance.SendAsync(new UpdatePrefixedShardingSettingOperation(new PrefixedShardingSetting
        {
            Prefix = "users/", 
            Shards = [0, 1, 2]
        }));

        // move big bucket to the newly added shard
        await Sharding.Resharding.MoveShardForId(store, $"users/0${bigBucketId}", toShard: 2);

        // assert stats 
        using (shard.DocumentsStorage.ContextPool.AllocateOperationContext(out DocumentsOperationContext ctx))
        using (ctx.OpenReadTransaction())
        {
            var stats = ShardedDocumentsStorage.GetBucketStatisticsFor(ctx, bucket);
            Assert.Equal(0, stats.NumberOfDocuments);
            Assert.Equal(9988978, stats.Size);

            var tombsCount = shard.DocumentsStorage.GetNumberOfTombstones(ctx);
            Assert.Equal(100_001, tombsCount);

            await shard.TombstoneCleaner.ExecuteCleanup();
        }

        using (shard.DocumentsStorage.ContextPool.AllocateOperationContext(out DocumentsOperationContext ctx))
        using (ctx.OpenReadTransaction())
        {
            var stats = ShardedDocumentsStorage.GetBucketStatisticsFor(ctx, bucket);
            Assert.Null(stats);

            var tombsCount = shard.DocumentsStorage.GetNumberOfTombstones(ctx);
            Assert.Equal(0, tombsCount);
        }

        using (var session = store.OpenAsyncSession(database: ShardHelper.ToShardName(store.Database, 0)))
        {
            var numberOfDocs = (await session.Advanced.LoadStartingWithAsync<User>("users/", pageSize: int.MaxValue)).Count();
            Assert.Equal(49723, numberOfDocs);
        }

        using (var session = store.OpenAsyncSession(database: ShardHelper.ToShardName(store.Database, 1)))
        {
            var numberOfDocs = (await session.Advanced.LoadStartingWithAsync<User>("users/", pageSize: int.MaxValue)).Count();
            Assert.Equal(50276, numberOfDocs);
        }

        using (var session = store.OpenAsyncSession(database: ShardHelper.ToShardName(store.Database, 2)))
        {
            var numberOfDocs = (await session.Advanced.LoadStartingWithAsync<User>("users/", pageSize: int.MaxValue)).Count();
            Assert.Equal(100_001, numberOfDocs);
        }

    }

    [RavenFact(RavenTestCategory.Sharding)]
    public async Task CanMoveBucketFromPrefixedRangeWhileWriting()
    {
        using var store = Sharding.GetDocumentStore(new Options
        {
            ModifyDatabaseRecord = record =>
            {
                record.Sharding ??= new ShardingConfiguration();
                record.Sharding.Prefixed =
                [
                    new PrefixedShardingSetting
                    {
                        Prefix = "users/",
                        Shards = [0, 1]
                    }
                ];
            }
        });

        using (var bulk = store.BulkInsert())
        {
            for (int i = 0; i < 1000; i++)
            {
                var id = $"users/{i}";
                bulk.Store(new User(), id);
            }
        }

        using (var session = store.OpenAsyncSession(database: ShardHelper.ToShardName(store.Database, 0)))
        {
            var numberOfDocs = await session.Query<User>().CountAsync();
            Assert.Equal(538, numberOfDocs);
        }

        using (var session = store.OpenAsyncSession(database: ShardHelper.ToShardName(store.Database, 1)))
        {
            var numberOfDocs = await session.Query<User>().CountAsync();
            Assert.Equal(462, numberOfDocs);
        }

        using (var session = store.OpenAsyncSession(database: ShardHelper.ToShardName(store.Database, 2)))
        {
            var numberOfDocs = await session.Query<User>().CountAsync();
            Assert.Equal(0, numberOfDocs);
        }

        // add shard #2 to prefix setting
        await store.Maintenance.SendAsync(new UpdatePrefixedShardingSettingOperation(new PrefixedShardingSetting
        {
            Prefix = "users/",
            Shards = [0, 1, 2]
        }));

        // move bucket to the newly added shard while writing
        var docId = "users/1";
        var bucket = await Sharding.GetBucketAsync(store, docId);
        var originalShardNumber = await Sharding.GetShardNumberForAsync(store, docId);

        var writes = Task.Run(async () =>
        {
            using (var session = store.OpenAsyncSession())
            {
                for (int i = 1000; i < 2000; i++)
                {
                    var id = $"users/{i}${docId}";
                    await session.StoreAsync(new User(), id);
                }

                await session.SaveChangesAsync();
            }
        });
        var bucketMigration = Sharding.Resharding.MoveShardForId(store, docId, toShard: 2);

        await Task.WhenAll(bucketMigration, writes);

        // assert bucket ranges
        var shardingConfig = await Sharding.GetShardingConfigurationAsync(store);
        Assert.Equal(7, shardingConfig.BucketRanges.Count);

        Assert.Equal(ShardHelper.NumberOfBuckets, shardingConfig.BucketRanges[3].BucketRangeStart);
        Assert.Equal(0, shardingConfig.BucketRanges[3].ShardNumber);

        Assert.Equal(ShardHelper.NumberOfBuckets * 1.5, shardingConfig.BucketRanges[4].BucketRangeStart);
        Assert.Equal(1, shardingConfig.BucketRanges[4].ShardNumber);

        Assert.Equal(bucket, shardingConfig.BucketRanges[5].BucketRangeStart);
        Assert.Equal(2, shardingConfig.BucketRanges[5].ShardNumber);

        Assert.Equal(bucket + 1, shardingConfig.BucketRanges[6].BucketRangeStart);
        Assert.Equal(1, shardingConfig.BucketRanges[6].ShardNumber);

        // assert stats 
        var originalShard = await GetDocumentDatabaseInstanceFor(store, ShardHelper.ToShardName(store.Database, originalShardNumber));
        using (originalShard.DocumentsStorage.ContextPool.AllocateOperationContext(out DocumentsOperationContext ctx))
        using (ctx.OpenReadTransaction())
        {
            var stats = ShardedDocumentsStorage.GetBucketStatisticsFor(ctx, bucket);
            Assert.Equal(0, stats.NumberOfDocuments);
        }

        var newShard = await GetDocumentDatabaseInstanceFor(store, ShardHelper.ToShardName(store.Database, shard: 2));
        using (newShard.DocumentsStorage.ContextPool.AllocateOperationContext(out DocumentsOperationContext ctx))
        using (ctx.OpenReadTransaction())
        {
            var stats = ShardedDocumentsStorage.GetBucketStatisticsFor(ctx, bucket);
            Assert.Equal(1001, stats.NumberOfDocuments);
        }

        // assert docs
        using (var session = store.OpenAsyncSession(database: ShardHelper.ToShardName(store.Database, 0)))
        {
            var numberOfDocs = await session.Query<User>().CountAsync();
            Assert.Equal(538, numberOfDocs);
        }

        using (var session = store.OpenAsyncSession(database: ShardHelper.ToShardName(store.Database, 1)))
        {
            var numberOfDocs = await session.Query<User>().CountAsync();
            Assert.Equal(461, numberOfDocs);
        }

        using (var session = store.OpenAsyncSession(database: ShardHelper.ToShardName(store.Database, 2)))
        {
            var numberOfDocs = await session.Query<User>().CountAsync();
            Assert.Equal(1001, numberOfDocs);
        }

    }

    [RavenFact(RavenTestCategory.Sharding)]
    public async Task ShouldNotAllowToRemoveShardFromDbIfItHasPrefixesSettings()
    {
        var cluster = await CreateRaftCluster(numberOfNodes: 3, watcherCluster: true);
        var options = Sharding.GetOptionsForCluster(cluster.Leader, shards: 2, shardReplicationFactor: 1, orchestratorReplicationFactor: 3);
        options.ModifyDatabaseRecord += record =>
        {
            record.Sharding ??= new ShardingConfiguration();
            record.Sharding.Prefixed =
            [
                new PrefixedShardingSetting
                {
                    Prefix = "users/", 
                    Shards = [0, 1]
                }
            ];
        };

        using var store = Sharding.GetDocumentStore(options);

        using var session = store.OpenAsyncSession();
        for (int i = 0; i < 1000; i++)
        {
            var id = $"users/{i}";
            await session.StoreAsync(new User(), id);
        }
        await session.SaveChangesAsync();

        // add shard #2 to database
        var sharding = await Sharding.GetShardingConfigurationAsync(store);
        var shardNodes = sharding.Shards.Select(kvp => kvp.Value.Members[0]);
        var nodeNotInDbGroup = cluster.Nodes.SingleOrDefault(n => shardNodes.Contains(n.ServerStore.NodeTag) == false)?.ServerStore.NodeTag;
        Assert.NotNull(nodeNotInDbGroup);

        var addShardRes = store.Maintenance.Server.Send(new AddDatabaseShardOperation(store.Database, [nodeNotInDbGroup]));
        Assert.Equal(2, addShardRes.ShardNumber);
        await Cluster.WaitForRaftIndexToBeAppliedInClusterAsync(addShardRes.RaftCommandIndex);

        await AssertWaitForValueAsync(async () =>
        {
            sharding = await Sharding.GetShardingConfigurationAsync(store);
            sharding.Shards.TryGetValue(2, out var topology);
            return topology?.Members.Count;
        }, expectedVal: 1);


        // add shard #2 to 'users/' prefix setting
        await store.Maintenance.SendAsync(new UpdatePrefixedShardingSettingOperation(new PrefixedShardingSetting
        {
            Prefix = "users/", 
            Shards = [0, 1, 2]
        }));

        // should not allow to delete shard #2 because it's part of 'users/' prefix setting
        var deleteShardTask = store.Maintenance.Server.SendAsync(new DeleteDatabasesOperation(store.Database, shardNumber: 2, hardDelete: true, fromNode: nodeNotInDbGroup));
        await Assert.ThrowsAsync<RavenException>(async () => await deleteShardTask);

        // remove shard #2 from 'users/' prefix setting
        // can be removed because shard #2 has no bucket ranges for this prefix
        await store.Maintenance.SendAsync(new UpdatePrefixedShardingSettingOperation(new PrefixedShardingSetting
        {
            Prefix = "users/",
            Shards = [0, 1]
        }));

        // now we should be able to delete shard #2 from database
        var res = await store.Maintenance.Server.SendAsync(new DeleteDatabasesOperation(store.Database, shardNumber: 2, hardDelete: true, fromNode: nodeNotInDbGroup));
        await Cluster.WaitForRaftIndexToBeAppliedInClusterAsync(res.RaftCommandIndex);

        await AssertWaitForValueAsync(async () =>
        {
            sharding = await Sharding.GetShardingConfigurationAsync(store);
            return sharding.Shards.TryGetValue(2, out _);
        }, expectedVal: false);
    }

    [RavenFact(RavenTestCategory.Sharding)]
    public async Task WhenAddingNewPrefixShouldFillBucketRangeGaps()
    {
        using var store = Sharding.GetDocumentStore(new Options
        {
            ModifyDatabaseRecord = record =>
            {
                record.Sharding ??= new ShardingConfiguration();
                record.Sharding.Prefixed =
                [
                    new PrefixedShardingSetting
                    {
                        Prefix = "users/a/",
                        Shards = [0]
                    },
                    new PrefixedShardingSetting
                    {
                        Prefix = "users/b/",
                        Shards = [1]
                    },
                    new PrefixedShardingSetting
                    {
                        Prefix = "users/c/",
                        Shards = [0, 1]
                    },
                    new PrefixedShardingSetting
                    {
                        Prefix = "users/d/",
                        Shards = [0, 1, 2]
                    },
                ];
            }
        });

        var sharding = await Sharding.GetShardingConfigurationAsync(store);
        Assert.Equal(4, sharding.Prefixed.Count);
        Assert.Equal("users/d/", sharding.Prefixed[0].Prefix);
        Assert.Equal(ShardHelper.NumberOfBuckets, sharding.Prefixed[0].BucketRangeStart);

        Assert.Equal("users/c/", sharding.Prefixed[1].Prefix);
        Assert.Equal(ShardHelper.NumberOfBuckets * 2, sharding.Prefixed[1].BucketRangeStart);

        Assert.Equal("users/b/", sharding.Prefixed[2].Prefix);
        Assert.Equal(ShardHelper.NumberOfBuckets * 3, sharding.Prefixed[2].BucketRangeStart);

        Assert.Equal("users/a/", sharding.Prefixed[3].Prefix);
        Assert.Equal(ShardHelper.NumberOfBuckets * 4, sharding.Prefixed[3].BucketRangeStart);

        // deleting 'users/c/' will create a gap in prefixes bucket range start (range 1M - 2M range is missing)
        await store.Maintenance.SendAsync(new DeletePrefixedShardingSettingOperation("users/c/"));
        sharding = await Sharding.GetShardingConfigurationAsync(store);

        Assert.Equal(3, sharding.Prefixed.Count);
        Assert.Equal("users/d/", sharding.Prefixed[0].Prefix);
        Assert.Equal(ShardHelper.NumberOfBuckets, sharding.Prefixed[0].BucketRangeStart);

        Assert.Equal("users/b/", sharding.Prefixed[1].Prefix);
        Assert.Equal(ShardHelper.NumberOfBuckets * 3, sharding.Prefixed[1].BucketRangeStart);

        Assert.Equal("users/a/", sharding.Prefixed[2].Prefix);
        Assert.Equal(ShardHelper.NumberOfBuckets * 4, sharding.Prefixed[2].BucketRangeStart);

        // add a new prefix, 1M - 2M range should be assigned to it
        await store.Maintenance.SendAsync(new AddPrefixedShardingSettingOperation(new PrefixedShardingSetting
        {
            Prefix = "users/z/",
            Shards = [1, 2]
        }));

        sharding = await Sharding.GetShardingConfigurationAsync(store);
        Assert.Equal(4, sharding.Prefixed.Count);
        Assert.Equal("users/z/", sharding.Prefixed[0].Prefix);
        Assert.Equal(ShardHelper.NumberOfBuckets * 2, sharding.Prefixed[0].BucketRangeStart);

        Assert.Equal("users/d/", sharding.Prefixed[1].Prefix);
        Assert.Equal(ShardHelper.NumberOfBuckets, sharding.Prefixed[1].BucketRangeStart);

        Assert.Equal("users/b/", sharding.Prefixed[2].Prefix);
        Assert.Equal(ShardHelper.NumberOfBuckets * 3, sharding.Prefixed[2].BucketRangeStart);

        Assert.Equal("users/a/", sharding.Prefixed[3].Prefix);
        Assert.Equal(ShardHelper.NumberOfBuckets * 4, sharding.Prefixed[3].BucketRangeStart);

    }

    [RavenTheory(RavenTestCategory.Sharding | RavenTestCategory.Etl)]
    [RavenData(SearchEngineMode = RavenSearchEngineMode.All)]
    public async Task ReshardingWithEtl_PrefixedSource(Options options)
    {
        using var srcStore = Sharding.GetDocumentStore(new Options
        {
            ModifyDatabaseRecord = record =>
            {
                record.Sharding ??= new ShardingConfiguration();
                record.Sharding.Prefixed =
                [
                    new PrefixedShardingSetting
                    {
                        Prefix = "users/",
                        Shards = [0, 1]
                    }
                ];
            }
        });

        using var dstStore = GetDocumentStore(options);
        Etl.AddEtl(srcStore, dstStore, "users", script: null);

        using (var bulk = srcStore.BulkInsert())
        {
            for (int i = 0; i < 1000; i++)
            {
                var id = $"users/{i}";
                bulk.Store(new User(), id);
            }
        }

        await AssertWaitForValueAsync(async () =>
        {
            using var session = dstStore.OpenAsyncSession();
            return await session.Query<User>().CountAsync();
        }, expectedVal: 1000);


        // add shard #2 to prefix setting
        await srcStore.Maintenance.SendAsync(new UpdatePrefixedShardingSettingOperation(new PrefixedShardingSetting
        {
            Prefix = "users/",
            Shards = [0, 1, 2]
        }));

        var docId = "users/1";
        var writes = Task.Run(async () =>
        {
            using (var session = srcStore.OpenAsyncSession())
            {
                for (int i = 1000; i < 2000; i++)
                {
                    var id = $"users/{i}${docId}";
                    await session.StoreAsync(new User(), id);
                }

                await session.SaveChangesAsync();
            }
        });

        await Sharding.Resharding.MoveShardForId(srcStore, docId, toShard: 2);
        await writes;

        // assert docs
        using (var session = srcStore.OpenAsyncSession(database: ShardHelper.ToShardName(srcStore.Database, 0)))
        {
            var numberOfDocs = await session.Query<User>().CountAsync();
            Assert.Equal(538, numberOfDocs);
        }

        using (var session = srcStore.OpenAsyncSession(database: ShardHelper.ToShardName(srcStore.Database, 1)))
        {
            var numberOfDocs = await session.Query<User>().CountAsync();
            Assert.Equal(461, numberOfDocs);
        }

        using (var session = srcStore.OpenAsyncSession(database: ShardHelper.ToShardName(srcStore.Database, 2)))
        {
            var numberOfDocs = await session.Query<User>().CountAsync();
            Assert.Equal(1001, numberOfDocs);
        }

        await AssertWaitForValueAsync(async () =>
        {
            using var session = dstStore.OpenAsyncSession();
            return await session.Query<User>().CountAsync();
        }, expectedVal: 2000);
    }

    [RavenTheory(RavenTestCategory.Sharding | RavenTestCategory.Etl)]
    [RavenData(SearchEngineMode = RavenSearchEngineMode.All)]
    public async Task ReshardingWithEtl_PrefixedDestination(Options options)
    {
        using var srcStore = GetDocumentStore(options);
        using var dstStore = Sharding.GetDocumentStore(new Options
        {
            ModifyDatabaseRecord = record =>
            {
                record.Sharding ??= new ShardingConfiguration();
                record.Sharding.Prefixed =
                [
                    new PrefixedShardingSetting
                    {
                        Prefix = "users/",
                        Shards = [0, 1]
                    }
                ];
            }
        });

        Etl.AddEtl(srcStore, dstStore, "users", script: null);

        using (var bulk = srcStore.BulkInsert())
        {
            for (int i = 0; i < 1000; i++)
            {
                var id = $"users/{i}";
                bulk.Store(new User(), id);
            }
        }

        await AssertWaitForValueAsync(async () =>
        {
            using var session = dstStore.OpenAsyncSession();
            return await session.Query<User>().CountAsync();
        }, expectedVal: 1000);


        // add shard #2 to prefix setting
        await dstStore.Maintenance.SendAsync(new UpdatePrefixedShardingSettingOperation(new PrefixedShardingSetting
        {
            Prefix = "users/",
            Shards = [0, 1, 2]
        }));

        var docId = "users/1";
        var writes = Task.Run(() =>
        {
            using (var bulk = srcStore.BulkInsert())
            {
                for (int i = 1000; i < 2000; i++)
                {
                    var id = $"users/{i}${docId}";
                    bulk.Store(new User(), id);
                }
            }
        });

        await Sharding.Resharding.MoveShardForId(dstStore, docId, toShard: 2);
        await writes;

        await AssertWaitForValueAsync(async () =>
        {
            using var session = dstStore.OpenAsyncSession();
            return await session.Query<User>().CountAsync();
        }, expectedVal: 2000);

        // assert docs
        using (var session = dstStore.OpenAsyncSession(database: ShardHelper.ToShardName(dstStore.Database, 0)))
        {
            var numberOfDocs = await session.Query<User>().CountAsync();
            Assert.Equal(538, numberOfDocs);
        }

        using (var session = dstStore.OpenAsyncSession(database: ShardHelper.ToShardName(dstStore.Database, 1)))
        {
            var numberOfDocs = await session.Query<User>().CountAsync();
            Assert.Equal(461, numberOfDocs);
        }

        using (var session = dstStore.OpenAsyncSession(database: ShardHelper.ToShardName(dstStore.Database, 2)))
        {
            var numberOfDocs = await session.Query<User>().CountAsync();
            Assert.Equal(1001, numberOfDocs);
        }
    }

    [RavenFact(RavenTestCategory.BackupExportImport | RavenTestCategory.Sharding)]
    public async Task CanImportIncrementalIntoPrefixedShardedDatabase()
    {
        var backupPath = NewDataPath(suffix: "_BackupFolder");

        using (var store1 = Sharding.GetDocumentStore(new Options() 
        {
           ModifyDatabaseRecord = record =>
           {
               record.Sharding ??= new();
               record.Sharding.Prefixed = [new PrefixedShardingSetting
               {
                   Prefix = "Users/", 
                   Shards = [0, 1]
               }];

           }
        }))
        using (var store2 = Sharding.GetDocumentStore(new Options()
        {
           ModifyDatabaseRecord = record =>
           {
               record.Sharding ??= new();
               record.Sharding.Prefixed = [new PrefixedShardingSetting
               {
                   Prefix = "Users/",
                   Shards = [1 , 2]
               }];
           }
        }))
        {
            var shardNumToDocIds = new Dictionary<int, List<string>>();
            var dbRecord = await store1.Maintenance.Server.SendAsync(new GetDatabaseRecordOperation(store1.Database));
            var shardedCtx = new ShardedDatabaseContext(Server.ServerStore, dbRecord);

            // generate data on store1, keep track of doc-ids per shard
            using (var session = store1.OpenAsyncSession())
            using (Server.ServerStore.ContextPool.AllocateOperationContext(out TransactionOperationContext context))
            {
                for (int i = 0; i < 100; i++)
                {
                    var user = new User { Name = i.ToString() };
                    var id = $"users/{i}";

                    var shardNumber = shardedCtx.GetShardNumberFor(context, id);
                    if (shardNumToDocIds.TryGetValue(shardNumber, out var ids) == false)
                    {
                        shardNumToDocIds[shardNumber] = ids = new List<string>();
                    }
                    ids.Add(id);

                    await session.StoreAsync(user, id);
                }

                Assert.Equal(2, shardNumToDocIds.Count);
                Assert.False(shardNumToDocIds.ContainsKey(2));

                await session.SaveChangesAsync();
            }

            var waitHandles = await Sharding.Backup.WaitForBackupToComplete(store1);

            var config = Backup.CreateBackupConfiguration(backupPath, incrementalBackupFrequency: "* * * * *");
            await Sharding.Backup.UpdateConfigurationAndRunBackupAsync(Server, store1, config);

            Assert.True(WaitHandle.WaitAll(waitHandles, TimeSpan.FromMinutes(1)));

            // import
            var dirs = Directory.GetDirectories(backupPath);
            Assert.Equal(3, dirs.Length);

            foreach (var dir in dirs)
            {
                await store2.Smuggler.ImportIncrementalAsync(new DatabaseSmugglerImportOptions(), dir);
            }

            using (var session = store2.OpenAsyncSession(ShardHelper.ToShardName(store2.Database, 0)))
            {
                var docs = await session.Query<User>().ToListAsync();
                Assert.Equal(0, docs.Count);
            }
            using (var session = store2.OpenAsyncSession(ShardHelper.ToShardName(store2.Database, 1)))
            {
                var docs = await session.Query<User>().ToListAsync();
                Assert.Equal(shardNumToDocIds[0].Count, docs.Count);

                foreach (var doc in docs)
                {
                    var id = doc.Id;
                    Assert.True(shardNumToDocIds[0].Contains(id));
                }
            }
            using (var session = store2.OpenAsyncSession(ShardHelper.ToShardName(store2.Database, 2)))
            {
                var docs = await session.Query<User>().ToListAsync();
                Assert.Equal(shardNumToDocIds[1].Count, docs.Count);

                foreach (var doc in docs)
                {
                    var id = doc.Id;
                    Assert.True(shardNumToDocIds[1].Contains(id));
                }
            }

            // add more data to store1
            using (var session = store1.OpenAsyncSession())
            using (Server.ServerStore.ContextPool.AllocateOperationContext(out TransactionOperationContext context))
            {
                for (int i = 100; i < 200; i++)
                {
                    var user = new User { Name = i.ToString() };
                    var id = $"users/{i}";

                    var shardNumber = shardedCtx.GetShardNumberFor(context, id);
                    shardNumToDocIds[shardNumber].Add(id);

                    await session.StoreAsync(user, id);
                }

                await session.SaveChangesAsync();
            }

            waitHandles = await Sharding.Backup.WaitForBackupToComplete(store1);

            await Sharding.Backup.UpdateConfigurationAndRunBackupAsync(Server, store1, config);

            Assert.True(WaitHandle.WaitAll(waitHandles, TimeSpan.FromMinutes(1)));

            // import
            var newDirs = Directory.GetDirectories(backupPath).Except(dirs).ToList();
            Assert.Equal(3, newDirs.Count);

            foreach (var dir in newDirs)
            {
                await store2.Smuggler.ImportIncrementalAsync(new DatabaseSmugglerImportOptions(), dir);
            }

            // assert
            using (var session = store2.OpenAsyncSession(ShardHelper.ToShardName(store2.Database, 0)))
            {
                var docs = await session.Query<User>().ToListAsync();
                Assert.Equal(0, docs.Count);
            }
            using (var session = store2.OpenAsyncSession(ShardHelper.ToShardName(store2.Database, 1)))
            {
                var docs = await session.Query<User>().ToListAsync();
                Assert.Equal(shardNumToDocIds[0].Count, docs.Count);

                foreach (var doc in docs)
                {
                    var id = doc.Id;
                    Assert.True(shardNumToDocIds[0].Contains(id));
                }
            }
            using (var session = store2.OpenAsyncSession(ShardHelper.ToShardName(store2.Database, 2)))
            {
                var docs = await session.Query<User>().ToListAsync();
                Assert.Equal(shardNumToDocIds[1].Count, docs.Count);

                foreach (var doc in docs)
                {
                    var id = doc.Id;
                    Assert.True(shardNumToDocIds[1].Contains(id));
                }
            }
        }
    }

    [RavenFact(RavenTestCategory.BackupExportImport | RavenTestCategory.Sharding)]
    public async Task CanBackupAndRestorePrefixedShardedDatabase_FromIncrementalBackup()
    {
        var backupPath = NewDataPath(suffix: "BackupFolder");
        var cluster = await CreateRaftCluster(3, watcherCluster: true);

        var options = Sharding.GetOptionsForCluster(cluster.Leader, shards: 3, shardReplicationFactor: 1, orchestratorReplicationFactor: 3);
        options.ModifyDatabaseRecord += record =>
        {
            record.Sharding.Prefixed = [new PrefixedShardingSetting
            {
                Prefix = "users/", 
                Shards = [0, 1]
            }];
        };

        using (var store = Sharding.GetDocumentStore(options))
        {
            using (var session = store.OpenAsyncSession())
            {
                for (int i = 0; i < 10; i++)
                {
                    await session.StoreAsync(new User(), $"users/{i}");
                }

                await session.SaveChangesAsync();
            }

            var waitHandles = await Sharding.Backup.WaitForBackupsToComplete(cluster.Nodes, store.Database);

            var config = Backup.CreateBackupConfiguration(backupPath);
            var backupTaskId = await Sharding.Backup.UpdateConfigurationAndRunBackupAsync(cluster.Nodes, store, config, isFullBackup: false);

            Assert.True(WaitHandle.WaitAll(waitHandles, TimeSpan.FromMinutes(1)));

            // add more data
            waitHandles = await Sharding.Backup.WaitForBackupsToComplete(cluster.Nodes, store.Database);
            using (var session = store.OpenAsyncSession())
            {
                for (int i = 10; i < 20; i++)
                {
                    await session.StoreAsync(new User(), $"users/{i}");
                }

                await session.SaveChangesAsync();
            }

            // add shard #2 to prefix setting and move one bucket to the new shard
            await store.Maintenance.SendAsync(new UpdatePrefixedShardingSettingOperation(new PrefixedShardingSetting
            {
                Prefix = "users/", 
                Shards = [0, 1, 2]
            }));

            await Sharding.Resharding.MoveShardForId(store, "users/11", toShard: 2);

            using (var session = store.OpenAsyncSession(ShardHelper.ToShardName(store.Database, 0)))
            {
                var count = await session.Query<User>().CountAsync();
                Assert.Equal(9, count);
            }

            using (var session = store.OpenAsyncSession(ShardHelper.ToShardName(store.Database, 1)))
            {
                var count = await session.Query<User>().CountAsync();
                Assert.Equal(10, count);
            }

            using (var session = store.OpenAsyncSession(ShardHelper.ToShardName(store.Database, 2)))
            {
                var count = await session.Query<User>().CountAsync();
                Assert.Equal(1, count);
            }

            await Sharding.Backup.RunBackupAsync(store.Database, backupTaskId, isFullBackup: false, cluster.Nodes);
            Assert.True(WaitHandle.WaitAll(waitHandles, TimeSpan.FromMinutes(1)));

            var dirs = Directory.GetDirectories(backupPath);
            Assert.Equal(cluster.Nodes.Count, dirs.Length);

            foreach (var dir in dirs)
            {
                var files = Directory.GetFiles(dir);
                Assert.Equal(2, files.Length);
            }

            var sharding = await Sharding.GetShardingConfigurationAsync(store);
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
                var dbRec = await store.Maintenance.Server.SendAsync(new GetDatabaseRecordOperation(restoredDatabaseName));
                Assert.Equal(3, dbRec.Sharding.Shards.Count);

                var shardNodes = new HashSet<string>();
                foreach (var shardToTopology in dbRec.Sharding.Shards)
                {
                    var shardTopology = shardToTopology.Value;
                    Assert.Equal(1, shardTopology.Members.Count);
                    Assert.Equal(sharding.Shards[shardToTopology.Key].Members[0], shardTopology.Members[0]);
                    Assert.True(shardNodes.Add(shardTopology.Members[0]));
                }

                using (var session = store.OpenSession(restoredDatabaseName))
                {
                    for (int i = 0; i < 20; i++)
                    {
                        var doc = session.Load<User>($"users/{i}");
                        Assert.NotNull(doc);
                    }
                }

                sharding = await Sharding.GetShardingConfigurationAsync(store, restoredDatabaseName);

                Assert.Equal(1, sharding.Prefixed.Count);
                Assert.Equal("users/", sharding.Prefixed[0].Prefix);
                Assert.Equal(ShardHelper.NumberOfBuckets, sharding.Prefixed[0].BucketRangeStart);
                Assert.Equal(3, sharding.Prefixed[0].Shards.Count);

                using (var session = store.OpenAsyncSession(ShardHelper.ToShardName(restoredDatabaseName, 0)))
                {
                    var count = await session.Query<User>().CountAsync();
                    Assert.Equal(9, count);
                }

                using (var session = store.OpenAsyncSession(ShardHelper.ToShardName(restoredDatabaseName, 1)))
                {
                    var count = await session.Query<User>().CountAsync();
                    Assert.Equal(10, count);
                }

                using (var session = store.OpenAsyncSession(ShardHelper.ToShardName(restoredDatabaseName, 2)))
                {
                    var count = await session.Query<User>().CountAsync();
                    Assert.Equal(1, count);
                }
            }
        }
    }

    [RavenFact(RavenTestCategory.Sharding)]
    public async Task DeletingPrefixAfterShardsDistributionHasBeenUpdated()
    {
        using var store = Sharding.GetDocumentStore(new Options
        {
            ModifyDatabaseRecord = record =>
            {
                record.Sharding ??= new ShardingConfiguration();
                record.Sharding.Prefixed =
                [
                    new PrefixedShardingSetting
                    {
                        Prefix = "users/",
                        Shards = [0]
                    }
                ];
            }
        });

        var shardingConfiguration = await Sharding.GetShardingConfigurationAsync(store);
        Assert.Equal(4, shardingConfiguration.BucketRanges.Count);

        await store.Maintenance.SendAsync(new UpdatePrefixedShardingSettingOperation(new PrefixedShardingSetting
        {
            Prefix = "users/",
            Shards = [0, 1, 2]
        }));

        shardingConfiguration = await Sharding.GetShardingConfigurationAsync(store);
        Assert.Equal(4, shardingConfiguration.BucketRanges.Count);

        await store.Maintenance.SendAsync(new DeletePrefixedShardingSettingOperation("users/"));

        shardingConfiguration = await Sharding.GetShardingConfigurationAsync(store);

        Assert.Equal(3, shardingConfiguration.BucketRanges.Count);
    }

    [RavenFact(RavenTestCategory.Sharding)]
    public async Task DeletingPrefixAfterBucketMigration()
    {
        using var store = Sharding.GetDocumentStore(new Options
        {
            ModifyDatabaseRecord = record =>
            {
                record.Sharding ??= new ShardingConfiguration();
                record.Sharding.Prefixed =
                [
                    new PrefixedShardingSetting
                    {
                        Prefix = "users/",
                        Shards = [0, 1, 2]
                    }
                ];
            }
        });

        var shardingConfiguration = await Sharding.GetShardingConfigurationAsync(store);
        Assert.Equal(6, shardingConfiguration.BucketRanges.Count);

        var id = "users/2";
        var originalShardForId = await Sharding.GetShardNumberForAsync(store, id);

        Assert.Equal(0, originalShardForId);

        using (var session = store.OpenAsyncSession())
        {
            await session.StoreAsync(new User(), id);
            await session.SaveChangesAsync();
        }

        await Sharding.Resharding.MoveShardForId(store, id, toShard: 2);

        shardingConfiguration = await Sharding.GetShardingConfigurationAsync(store);
        Assert.Equal(8, shardingConfiguration.BucketRanges.Count);

        using (var session = store.OpenAsyncSession())
        {
            session.Delete(id);
            await session.SaveChangesAsync();
        }

        // this should delete all 5 bucket ranges assigned for this prefix
        await store.Maintenance.SendAsync(new DeletePrefixedShardingSettingOperation("users/"));

        shardingConfiguration = await Sharding.GetShardingConfigurationAsync(store);
        Assert.Equal(3, shardingConfiguration.BucketRanges.Count);
    }

    [RavenFact(RavenTestCategory.Sharding)]
    public void PrefixedSharding_CanQueryWithSpecifiedShardContext()
    {
        using var store = Sharding.GetDocumentStore(new Options
        {
            ModifyDatabaseRecord = record =>
            {
                record.Sharding ??= new ShardingConfiguration();
                record.Sharding.Prefixed =
                [
                    new PrefixedShardingSetting
                    {
                        Prefix = "users/us/",
                        Shards = [0]
                    },
                    new PrefixedShardingSetting
                    {
                        Prefix = "users/eu/",
                        Shards = [1]
                    },
                    new PrefixedShardingSetting
                    {
                        Prefix = "users/asia/",
                        Shards = [2]
                    }
                ];
            }
        });
        {
            using (var session = store.OpenSession())
            {
                session.Store(new User(), "users/us/1");
                session.Store(new User(), "users/us/2");
                session.Store(new User(), "users/us/3");

                session.Store(new User(), "users/eu/1");
                session.Store(new User(), "users/eu/2");

                session.Store(new User(), "users/asia/1");


                session.SaveChanges();
            }

            using (var session = store.OpenSession())
            {
                var results = session.Query<User>()
                    .Customize(x => x.ShardContext(s => s.ByDocumentId("users/us/1")))
                    .ToList();

                Assert.Equal(3, results.Count);

                var results2 = session.Query<User>()
                    .Customize(x => x.ShardContext(s => s.ByDocumentIds(new[] { "users/us/1", "users/eu/1" })))
                    .Select(x => x.Id)
                    .ToList();

                Assert.Equal(5, results2.Count);
                Assert.Contains("users/us/1", results2);
                Assert.Contains("users/us/2", results2);
                Assert.Contains("users/us/3", results2);
                Assert.Contains("users/eu/1", results2);
                Assert.Contains("users/eu/2", results2);

                var results3 = session.Query<User>()
                    .Customize(x => x.ShardContext(s => s.ByDocumentId("users/asia/")))
                    .ToList();

                Assert.Equal(1, results3.Count);
            }

            using (var session = store.OpenSession())
            {
                var results = session.Advanced.DocumentQuery<User>()
                    .ShardContext(s => s.ByDocumentId("users/us/1"))
                    .ToList();

                Assert.Equal(3, results.Count);

                var results2 = session.Advanced.DocumentQuery<User>()
                    .ShardContext(s => s.ByDocumentIds(new[] { "users/us/1", "users/eu/2" }))
                    .ToList();

                Assert.Equal(5, results2.Count);
                var results3 = session.Advanced.DocumentQuery<User>()
                    .ShardContext(s => s.ByDocumentId("users/asia/1"))
                    .ToList();

                Assert.Equal(1, results3.Count);
            }
        }
    }

    [RavenFact(RavenTestCategory.Sharding)]
    public async Task PrefixesShouldGetPrecedenceOverAnchoring()
    {
        const string companyId = "companies/1";
        const string relatedDocId = $"products/1${companyId}";
        const int productsShard = 0;

        using var store = Sharding.GetDocumentStore(new Options
        {
            ModifyDatabaseRecord = record =>
            {
                record.Sharding ??= new ShardingConfiguration();
                record.Sharding.Prefixed =
                [
                    new PrefixedShardingSetting
                    {
                        Prefix = "products/",
                        Shards = [productsShard]
                    }
                ];
            }
        });
        {
            using (var session = store.OpenSession())
            {
                session.Store(new Company(), companyId);
                session.SaveChanges();
            }

            var companyShardNumber = await Sharding.GetShardNumberForAsync(store, companyId);
            Assert.Equal(1, companyShardNumber);

            using (var session = store.OpenSession())
            {
                session.Store(new Product(), relatedDocId);
                session.SaveChanges();
            }

            using (var session = store.OpenSession(ShardHelper.ToShardName(store.Database, companyShardNumber)))
            {
                var product = session.Query<Product>()
                    .FirstOrDefault();

                Assert.Null(product);
            }

            using (var session = store.OpenSession(ShardHelper.ToShardName(store.Database, productsShard)))
            {
                var product = session.Query<Product>()
                    .FirstOrDefault();

                Assert.NotNull(product);
            }
        }
    }

    private class Item
    {
#pragma warning disable CS0649
        public string Id;
#pragma warning restore CS0649
    }
}
