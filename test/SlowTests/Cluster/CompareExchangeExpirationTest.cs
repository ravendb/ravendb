using System;
using System.Collections.Generic;
using System.Globalization;
using System.IO;
using System.IO.Compression;
using System.Linq;
using System.Threading;
using System.Threading.Tasks;
using FastTests.Graph;
using Raven.Client;
using Raven.Client.Documents;
using Raven.Client.Documents.Conventions;
using Raven.Client.Documents.Operations;
using Raven.Client.Documents.Operations.Backups;
using Raven.Client.Documents.Operations.CompareExchange;
using Raven.Client.Documents.Session;
using Raven.Client.Http;
using Raven.Client.Json;
using Raven.Client.ServerWide.Commands.Cluster;
using Raven.Client.ServerWide.Operations;
using Raven.Server.Config;
using Raven.Server.Config.Settings;
using Raven.Server.Rachis;
using Raven.Server.ServerWide;
using Raven.Server.ServerWide.Context;
using Raven.Server.Utils;
using Sparrow;
using Tests.Infrastructure;
using Xunit;
using Xunit.Abstractions;

namespace SlowTests.Cluster
{
    public class CompareExchangeExpirationTest : ClusterTestBase
    {
        private const string _chars = "ABCDEFGHIJKLMNOPQRSTUVWXYZ0123456789";

        public CompareExchangeExpirationTest(ITestOutputHelper output) : base(output)
        {
        }

        [Fact]
        public async Task CanAddCompareExchangeWithExpiration()
        {
            using var server = GetNewServer();

            var utcFormats = new Dictionary<string, DateTimeKind>
                {
                    {DefaultFormat.DateTimeFormatsToRead[0], DateTimeKind.Utc},
                    {DefaultFormat.DateTimeFormatsToRead[1], DateTimeKind.Unspecified},
                    {DefaultFormat.DateTimeFormatsToRead[2], DateTimeKind.Local},
                    {DefaultFormat.DateTimeFormatsToRead[3], DateTimeKind.Utc},
                    {DefaultFormat.DateTimeFormatsToRead[4], DateTimeKind.Unspecified},
                    {DefaultFormat.DateTimeFormatsToRead[5], DateTimeKind.Utc},
                    {DefaultFormat.DateTimeFormatsToRead[6], DateTimeKind.Utc},
                };
            Assert.Equal(utcFormats.Count, DefaultFormat.DateTimeFormatsToRead.Length);

            foreach (var dateTimeFormat in utcFormats)
            {
                using (var store = GetDocumentStore(new Options
                {
                    Server = server
                }))
                {
                    var rnd = new Random(DateTime.Now.Millisecond);
                    var user = new User { Name = new string(Enumerable.Repeat(_chars, 10).Select(s => s[rnd.Next(s.Length)]).ToArray()) };
                    var expiry = DateTime.Now.AddMinutes(2);

                    if (dateTimeFormat.Value == DateTimeKind.Utc)
                        expiry = expiry.ToUniversalTime();

                    var key = new string(Enumerable.Repeat(_chars, 10).Select(s => s[rnd.Next(s.Length)]).ToArray());
                    using (var session = store.OpenAsyncSession(new SessionOptions { TransactionMode = TransactionMode.ClusterWide }))
                    {
                        var result = session.Advanced.ClusterTransaction.CreateCompareExchangeValue(key, user);
                        result.Metadata[Constants.Documents.Metadata.Expires] = expiry.ToString(dateTimeFormat.Key);
                        await session.SaveChangesAsync();
                    }

                    var res = await store.Operations.SendAsync(new GetCompareExchangeValueOperation<User>(key));
                    Assert.NotNull(res);
                    Assert.Equal(user.Name, res.Value.Name);
                    var expirationDate = res.Metadata.GetString(Constants.Documents.Metadata.Expires);
                    Assert.NotNull(expirationDate);
                    var dateTime = DateTime.ParseExact(expirationDate, DefaultFormat.DateTimeFormatsToRead, CultureInfo.InvariantCulture, DateTimeStyles.RoundtripKind);
                    Assert.Equal(dateTimeFormat.Value, dateTime.Kind);
                    Assert.Equal(expiry.ToString(dateTimeFormat.Key), expirationDate);

                    server.ServerStore.Observer.Time.UtcDateTime = () => DateTime.UtcNow.AddMinutes(3);

                    var val = await WaitForValueAsync(async () =>
                    {
                        var stats = await store.Maintenance.SendAsync(new GetDetailedStatisticsOperation());
                        return stats.CountOfCompareExchange;
                    }, 0);

                    Assert.Equal(0, val);

                    server.ServerStore.Observer.Time.UtcDateTime = () => DateTime.UtcNow;
                    server.ServerStore.Observer._lastExpiredCompareExchangeCleanupTimeInTicks = DateTime.UtcNow.Ticks;
                }
            }
        }

        [Theory]
        [InlineData(10)]
        [InlineData(100)]
        public async Task CanAddManyCompareExchangeWithExpiration(int count)
        {
            using var server = GetNewServer(new ServerCreationOptions
            {
                CustomSettings = new Dictionary<string, string>()
                {
                    { RavenConfiguration.GetKey(x => x.Cluster.CompareExchangeExpiredCleanupInterval), "5" }
                }
            });
            using (var store = GetDocumentStore(new Options
            {
                Server = server
            }))
            {
                var expiry = DateTime.Now.AddMinutes(2);
                var compareExchanges = new Dictionary<string, User>();
                await AddCompareExchangesWithExpire(count, compareExchanges, store, expiry);
                await AssertCompareExchanges(compareExchanges, store, expiry);

                server.ServerStore.Observer.Time.UtcDateTime = () => DateTime.UtcNow.AddMinutes(3);

                var val = await WaitForValueAsync(async () =>
                {
                    var stats = await store.Maintenance.SendAsync(new GetDetailedStatisticsOperation());
                    return stats.CountOfCompareExchange;
                }, 0);

                Assert.Equal(0, val);
            }
        }

        [Fact]
        public async Task CanSnapshotManyCompareExchangeWithExpiration()
        {
            DebuggerAttachedTimeout.DisableLongTimespan = true;
            var count = 1024;
            using var leader = GetNewServer();
            using var follower = GetNewServer();
            using (var store = GetDocumentStore(new Options
            {
                Server = leader
            }))
            {
                var expiry = DateTime.Now.AddMinutes(2);
                var compareExchanges = new Dictionary<string, User>();
                await AddCompareExchangesWithExpire(count, compareExchanges, store, expiry);
                await AssertCompareExchanges(compareExchanges, store, expiry);

                using (var requestExecutor = ClusterRequestExecutor.CreateForSingleNode(leader.WebUrl, null, DocumentConventions.DefaultForServer))
                using (requestExecutor.ContextPool.AllocateOperationContext(out var ctx))
                {
                    await requestExecutor.ExecuteAsync(new AddClusterNodeCommand(follower.WebUrl, watcher: true), ctx);
                    var cmd = new AddDatabaseNodeOperation(store.Database).GetCommand(store.Conventions, ctx);
                    await requestExecutor.ExecuteAsync(cmd, ctx);
                    await follower.ServerStore.Cluster.WaitForIndexNotification(cmd.Result.RaftCommandIndex);
                }


                using (var fStore = GetDocumentStore(new Options
                {
                    Server = follower,
                    CreateDatabase = false,
                    ModifyDatabaseName = _ => store.Database,
                    ModifyDocumentStore = s => s.Conventions = new DocumentConventions { DisableTopologyUpdates = true }
                }))
                {
                    await AssertCompareExchanges(compareExchanges, store, expiry);

                    leader.ServerStore.Observer.Time.UtcDateTime = () => DateTime.UtcNow.AddMinutes(3);

                    var val = await WaitForValueAsync(async () =>
                    {
                        var stats = await store.Maintenance.SendAsync(new GetDetailedStatisticsOperation());
                        return stats.CountOfCompareExchange;
                    }, 0);

                    Assert.Equal(0, val);

                    val = await WaitForValueAsync(async () =>
                    {
                        var stats = await fStore.Maintenance.SendAsync(new GetDetailedStatisticsOperation());
                        return stats.CountOfCompareExchange;
                    }, 0);

                    Assert.Equal(0, val);
                }
            }
        }

        [Fact]
        public async Task CanSnapshotManyCompareExchangeWithExpirationToManyNodes()
        {
            var count = 3 * 1024;
            var nodesCount = 7;
            using var leader = GetNewServer();
            using (var store = GetDocumentStore(new Options
            {
                Server = leader
            }))
            {
                var now = DateTime.UtcNow;
                var expiry = now.AddMinutes(2);
                var compareExchanges = new Dictionary<string, User>();
                await AddCompareExchangesWithExpire(count, compareExchanges, store, expiry);

                for (int i = 0; i < nodesCount; i++)
                {
                    var follower = GetNewServer();
                    ServersForDisposal.Add(follower);

                    using (var requestExecutor = ClusterRequestExecutor.CreateForSingleNode(leader.WebUrl, null, DocumentConventions.DefaultForServer))
                    using (requestExecutor.ContextPool.AllocateOperationContext(out var ctx))
                    {
                        await requestExecutor.ExecuteAsync(new AddClusterNodeCommand(follower.WebUrl, watcher: true), ctx);
                    }

                    await follower.ServerStore.Engine.WaitForTopology(Leader.TopologyModification.NonVoter);
                }

                leader.ServerStore.Observer.Time.UtcDateTime = () => now.AddMinutes(3);

                var val = await WaitForValueAsync(async () =>
                {
                    var stats = await store.Maintenance.SendAsync(new GetDetailedStatisticsOperation());
                    return stats.CountOfCompareExchange;
                }, 0);

                Assert.Equal(0, val);
            }
        }


        [Theory]
        [InlineData(15)]
        [InlineData(150)]
        public async Task CanAddManyCompareExchangeWithAndWithoutExpiration(int count)
        {
            using var server = GetNewServer();
            using (var store = GetDocumentStore(new Options
            {
                Server = server
            }))
            {
                var expiry = DateTime.Now.AddMinutes(2);
                var longExpiry = DateTime.Now.AddMinutes(4);
                var compareExchangesWithShortExpiration = new Dictionary<string, User>();
                var compareExchangesWithLongExpiration = new Dictionary<string, User>();
                var compareExchanges = new Dictionary<string, User>();
                var amountToAdd = count / 3;
                await AddCompareExchangesWithExpire(amountToAdd, compareExchanges, store, expiry: null);
                await AddCompareExchangesWithExpire(amountToAdd, compareExchangesWithShortExpiration, store, expiry);
                await AddCompareExchangesWithExpire(amountToAdd, compareExchangesWithLongExpiration, store, longExpiry);

                await AssertCompareExchanges(compareExchangesWithShortExpiration, store, expiry);
                await AssertCompareExchanges(compareExchangesWithLongExpiration, store, longExpiry);
                await AssertCompareExchanges(compareExchanges, store, expiry: null);
                var stats = await store.Maintenance.SendAsync(new GetDetailedStatisticsOperation());
                Assert.Equal(count, stats.CountOfCompareExchange);

                server.ServerStore.Observer.Time.UtcDateTime = () => DateTime.UtcNow.AddMinutes(3);
                var val = await WaitForValueAsync(async () =>
                {
                    var stats = await store.Maintenance.SendAsync(new GetDetailedStatisticsOperation());
                    return stats.CountOfCompareExchange;
                }, count - amountToAdd, 15000);
                Assert.Equal(count - amountToAdd, val);

                server.ServerStore.Observer.Time.UtcDateTime = () => DateTime.UtcNow.AddMinutes(5);

                var nextVal = await WaitForValueAsync(async () =>
                {
                    var stats = await store.Maintenance.SendAsync(new GetDetailedStatisticsOperation());
                    return stats.CountOfCompareExchange;
                }, amountToAdd, 15000);
                Assert.Equal(amountToAdd, nextVal);
            }
        }

        [Fact, Trait("Category", "Smuggler")]
        public async Task CanExportImportCompareExchangeWithExpiration()
        {
            using var server = GetNewServer();
            int count = 15;
            var expiry = DateTime.Now.AddMinutes(2);
            var longExpiry = DateTime.Now.AddMinutes(4);
            var compareExchangesWithShortExpiration = new Dictionary<string, User>();
            var compareExchangesWithLongExpiration = new Dictionary<string, User>();
            var compareExchanges = new Dictionary<string, User>();
            var amountToAdd = count / 3;
            var backupPath = NewDataPath(suffix: "BackupFolder");
            var databaseName = GetDatabaseName() + "restore";

            using (var store = GetDocumentStore(new Options
            {
                Server = server
            }))
            {
                await AddCompareExchangesWithExpire(amountToAdd, compareExchanges, store, expiry: null);
                await AddCompareExchangesWithExpire(amountToAdd, compareExchangesWithShortExpiration, store, expiry);
                await AddCompareExchangesWithExpire(amountToAdd, compareExchangesWithLongExpiration, store, longExpiry);

                await AssertCompareExchanges(compareExchangesWithShortExpiration, store, expiry);
                await AssertCompareExchanges(compareExchangesWithLongExpiration, store, longExpiry);
                await AssertCompareExchanges(compareExchanges, store, expiry: null);

                var config = Backup.CreateBackupConfiguration(backupPath);
                var backupTaskId = Backup.UpdateConfigAndRunBackup(server, config, store);

                var backupDirectory = Directory.GetDirectories(backupPath).First();
                var restoreConfig = new RestoreBackupConfiguration()
                {
                    BackupLocation = backupDirectory,
                    DatabaseName = databaseName
                };

                var restoreOperation = new RestoreBackupOperation(restoreConfig);
                var o = await store.Maintenance.Server.SendAsync(restoreOperation);
                await o.WaitForCompletionAsync(TimeSpan.FromSeconds(30));

                using (var store2 = GetDocumentStore(new Options
                {
                    Server = server,
                    CreateDatabase = false,
                    ModifyDatabaseName = s => databaseName
                }))
                {
                    var stats1 = await store.Maintenance.SendAsync(new GetDetailedStatisticsOperation());
                    Assert.Equal(count, stats1.CountOfCompareExchange);
                    var stats = await store2.Maintenance.SendAsync(new GetDetailedStatisticsOperation());
                    Assert.Equal(count, stats.CountOfCompareExchange);
                    server.ServerStore.Observer.Time.UtcDateTime = () => DateTime.UtcNow.AddMinutes(3);

                    var val = await WaitForValueAsync(async () =>
                    {
                        var stats = await store2.Maintenance.SendAsync(new GetDetailedStatisticsOperation());
                        return stats.CountOfCompareExchange;
                    }, count - amountToAdd);
                    Assert.Equal(count - amountToAdd, val);

                    server.ServerStore.Observer.Time.UtcDateTime = () => DateTime.UtcNow.AddMinutes(5);

                    var nextVal = await WaitForValueAsync(async () =>
                    {
                        var stats = await store2.Maintenance.SendAsync(new GetDetailedStatisticsOperation());
                        return stats.CountOfCompareExchange;
                    }, amountToAdd);
                    Assert.Equal(amountToAdd, nextVal);

                    stats1 = await store.Maintenance.SendAsync(new GetDetailedStatisticsOperation());
                    Assert.Equal(amountToAdd, stats1.CountOfCompareExchange);
                    stats = await store2.Maintenance.SendAsync(new GetDetailedStatisticsOperation());
                    Assert.Equal(amountToAdd, stats.CountOfCompareExchange);
                }
            }
        }

        [Theory]
        [InlineData(10)]
        [InlineData(100)]
        public async Task CanAddManyCompareExchangeWithExpirationAndEditExpiration(int count)
        {
            using var server = GetNewServer();
            using (var store = GetDocumentStore(new Options
            {
                Server = server
            }))
            {
                var expiry = DateTime.Now.AddMinutes(2);
                var compareExchanges = new Dictionary<string, User>();
                var compareExchangeIndexes = new Dictionary<string, long>();
                for (int i = 0; i < count; i++)
                {
                    var rnd = new Random(DateTime.Now.Millisecond);
                    var user = new User { Name = new string(Enumerable.Repeat(_chars, 10).Select(s => s[rnd.Next(s.Length)]).ToArray()) };
                    var key = $"{new string(Enumerable.Repeat(_chars, 10).Select(s => s[rnd.Next(s.Length)]).ToArray())}{i}";
                    compareExchanges[key] = user;
                    using (var session = store.OpenAsyncSession(new SessionOptions { TransactionMode = TransactionMode.ClusterWide }))
                    {
                        var result = session.Advanced.ClusterTransaction.CreateCompareExchangeValue(key, user);
                        result.Metadata[Constants.Documents.Metadata.Expires] = expiry;

                        await session.SaveChangesAsync();
                        compareExchangeIndexes[key] = result.Index;
                    }
                }
                await AssertCompareExchanges(compareExchanges, store, expiry);

                expiry = DateTime.Now.AddMinutes(4);
                foreach (var kvp in compareExchanges)
                {
                    var metadata = new MetadataAsDictionary { [Constants.Documents.Metadata.Expires] = expiry };
                    await store.Operations.SendAsync(new PutCompareExchangeValueOperation<User>(kvp.Key, kvp.Value, compareExchangeIndexes[kvp.Key], metadata));
                }
                await AssertCompareExchanges(compareExchanges, store, expiry);

                server.ServerStore.Observer.Time.UtcDateTime = () => DateTime.UtcNow.AddMinutes(3);

                Thread.Sleep(count == 10 ? 1000 : 3000);

                var stats = await store.Maintenance.SendAsync(new GetDetailedStatisticsOperation());
                Assert.Equal(count, stats.CountOfCompareExchange);

                server.ServerStore.Observer.Time.UtcDateTime = () => DateTime.UtcNow.AddMinutes(5);
                var val = await WaitForValueAsync(async () =>
                {
                    var stats = await store.Maintenance.SendAsync(new GetDetailedStatisticsOperation());
                    return stats.CountOfCompareExchange;
                }, 0);

                Assert.Equal(0, val);
            }

        }

        [Fact]
        public async Task CanSnapshotCompareExchangeWithExpiration()
        {
            var count = 45;
            var (_, leader) = await CreateRaftCluster(1, watcherCluster: true);

            using (var store = GetDocumentStore(options: new Options
            {
                Server = leader
            }))
            {
                var expiry = DateTime.Now.AddMinutes(2);
                var compareExchanges = new Dictionary<string, User>();
                await CompareExchangeExpirationTest.AddCompareExchangesWithExpire(count, compareExchanges, store, expiry);
                await CompareExchangeExpirationTest.AssertCompareExchanges(compareExchanges, store, expiry);

                using (leader.ServerStore.Engine.ContextPool.AllocateOperationContext(out ClusterOperationContext context))
                using (context.OpenReadTransaction())
                {
                    Assert.Equal(count, CompareExchangeExpirationStorage.GetExpiredValues(context, long.MaxValue).Count());
                }

                var server2 = GetNewServer();
                var server2Url = server2.ServerStore.GetNodeHttpServerUrl();
                Servers.Add(server2);

                using (var requestExecutor = ClusterRequestExecutor.CreateForSingleNode(leader.WebUrl, null, DocumentConventions.DefaultForServer))
                using (requestExecutor.ContextPool.AllocateOperationContext(out var ctx))
                {
                    await requestExecutor.ExecuteAsync(new AddClusterNodeCommand(server2Url, watcher: true), ctx);

                    var addDatabaseNode = new AddDatabaseNodeOperation(store.Database);
                    await store.Maintenance.Server.SendAsync(addDatabaseNode);
                }

                using (server2.ServerStore.Engine.ContextPool.AllocateOperationContext(out ClusterOperationContext context))
                using (context.OpenReadTransaction())
                {
                    Assert.Equal(count, CompareExchangeExpirationStorage.GetExpiredValues(context, long.MaxValue).Count());
                }

                var now = DateTime.UtcNow;
                leader.ServerStore.Observer.Time.UtcDateTime = () => now.AddMinutes(3);

                var leaderCount = WaitForValue(() =>
                {
                    using (leader.ServerStore.Engine.ContextPool.AllocateOperationContext(out ClusterOperationContext context))
                    using (context.OpenReadTransaction())
                    {
                        return CompareExchangeExpirationStorage.GetExpiredValues(context, long.MaxValue).Count();
                    }
                }, 0, interval: 333);

                Assert.Equal(0, leaderCount);

                var server2count = WaitForValue(() =>
                {
                    using (server2.ServerStore.Engine.ContextPool.AllocateOperationContext(out ClusterOperationContext context))
                    using (context.OpenReadTransaction())
                    {
                        return CompareExchangeExpirationStorage.GetExpiredValues(context, long.MaxValue).Count();
                    }
                }, 0, interval: 333);

                Assert.Equal(0, server2count);
            }
        }

        [Fact]
        public void AllCompareExchangeAndExpirationPreserveAfterServerSchemaUpgradeFrom50000()
        {
            var folder = NewDataPath(forceCreateDir: true);
            DoNotReuseServer();

            var zipPath = new PathSetting("SchemaUpgrade/Issues/SystemVersion/RavenDB_17999/CompareExchange_Expiration_RavenData_v5.2_from50000.zip");
            Assert.True(File.Exists(zipPath.FullPath));

            ZipFile.ExtractToDirectory(zipPath.FullPath, folder);
            string[] expected = { "a/rli91za5ny0", "a/p7g0r2xzbl1", "a/4wl4ytgohw2", "a/20c7rb46lg3", "a/aderu6e1om4", "a/8h5uno2ir65", "a/6lwxg6rzvp6", "a/dyyhj20uyv7", "a/b2pkckpb2f8", "a/97gn51dt6z9", "a/8b7qyj2aai10", "a/2ogzc13qm611", "a/0t725js7qp12", "a/fhc6caaxv113", "a/dm395szezl14", "a/bqtcyaow3515", "a/9vkfrscd7o16", "a/7zbija1ub817", "a/532lcspbfs18", "a/48to5aetjc19", "a/jwysc1wjpn20", "a/yl2wjrf9vz21", "a/wqtzc94qyj22", "a/syb5y9ho6m23", "a/20piji1uol24", "a/5l988dooyu25", "a/qr70ch4qbw26", "a/y39kgdele127", "a/5fb4j8ngh728", "a/m0p6yhiojz29", "a/ucrq1drjm530", "a/bw5rgmlsox31", "a/i97bjhvnr332", "a/ztkdyqpwtv33", "a/gdyeczj4vm34", "a/op0ygvtzys35", "a/59e0u4n8zk36", "a/dmgkyzw32q37", "a/u6tmc8qb4i38", "a/i397ucuf9f39", "a/znn98loob740", "a/7zptchyjed41", "a/fbrdgc7ehj42", "a/387zyhbhmh43", "a/bk9j1ckcpn44", "a/s4nkglflre45", "a/zhp5jhoguk46", "a/g126ypiowc47", "a/od4q1lsjzi48", "a/vq7a5g1e1o49", "a/dakcjpvn3g50", "a/kmmwnl5i6l51", "a/160x1uzr8d52", "a/zbr0ucn8cx53", "a/gv428lihep54", "a/xfi3nucpgh55", "a/5rknqplkjn56", "a/mcyp5ygtlf57", "a/to098tpook58", "a/b8dbn2jwqc59", "a/ilgvqytrti60", "a/7hvg82wvyg61", "a/m60lgtfl4r62", "a/ti25jppg6x63", "a/a2g6yyjo8p64", "a/smt8c7dxah65", "a/zzvsg2msdn66", "a/gj9uubh1ff67", "a/x3mv8kb9h768", "a/5gpfcfk4kc69", "a/csrzgbuzni70", "a/i9kmcosbu871", "a/qlm7gk16xe72", "a/opda82qo1x73", "a/5arbnbkw3p74", "a/cmtvq6tr6v75", "a/kyvfu13m8176", "a/1i9h8axvat77", "a/8vb1c66qdz78", "a/pfp3qf1zfq79", "a/6z245ov7hi80", "a/ojg5jxpgja81", "a/54t7y6jol282", "a/mo78cfexnu83", "a/t09tgansq084", "a/akmuujh1ss85", "a/ixpexfrwvx86", "a/p9ry1a0ry387", "a/6t40fjuz0v88", "a/e57kje4u3189", "a/mi94nadp6790", "a/32m61j8y8z91", "a/aepq5ehta492", "a/irra8aqoda93", "a/zb4bnjlwf294", "a/6n7wqeuri895", "a/n7kx5no0k096", "a/4syzjwj9ms97", "a/c40jnss4py98", "a/kg23qn1zs399", "a/8dip8r52x1100", "a/gpk9cnex07101", "a/veodjexn6j102", "a/2qrxn97i9o103", "a/a2tiq5gdbu104", "a/rm6j5eamdm105", "a/zz9389khgs106", "a/gjm5niepik107", "a/nvopqdnklq108", "a/v8r9u9xfov109", "a/cs4b8iroqn110", "a/j46vcd0jtt111", "a/rh9ff9aewz112", "a/ztbzj4j9z5113", "a/65djn0t42b114", "a/4a4mfihl6u115", "a/cm66jdqg90116", "a/aqx9cvfxdk117", "a/8voc5d4eg4118", "a/g7rw88d9j9119", "a/njthc4m4mf120", "a/446iqdhdo7121", "a/cg92u8q8rd122", "a/8pr8f836zg123", "a/rnkuxl7ahr124", "a/yzme1gh5jx125", "a/6boz5cq0m3126", "a/nv20jlk9ov127", "a/u84kngu4r0128", "a/9w8pu7dtxc129", "a/h9b9x2mo0i130", "a/ytoacbgx2a131", "a/65quf7qs5g132", "a/np4wugk177133", "a/u26gxbtwad134", "a/bmkhcko4c5135", "a/jym2fgxzfb136", "a/du2yqjxra1137", "a/uefz4sr0ct138", "a/2qhk8n1vfz139", "a/92k4cjaqi4140", "a/qnx5qs5zkw141", "a/yzzpunetn2142", "a/fjdr8w82ou143" };

            using (var server = GetNewServer(new ServerCreationOptions { DeletePrevious = false, RunInMemory = false, DataDirectory = folder, RegisterForDisposal = false }))
            {
                using (server.ServerStore.Engine.ContextPool.AllocateOperationContext(out ClusterOperationContext context))
                {
                    context.OpenReadTransaction();
                    var tmp = expected.ToList();
                    foreach (var expired in CompareExchangeExpirationStorage.GetExpiredValues(context, long.MaxValue))
                    {
                        string k = expired.keySlice.ToString();

                        Assert.Contains(k, expected);
                        tmp.Remove(k);
                    }

                    Assert.Equal(0, tmp.Count);
                }
            }
        }

        private static async Task AssertCompareExchanges(Dictionary<string, User> compareExchangesWithExpiration, DocumentStore store, DateTime? expiry = null)
        {
            foreach ((string key, User user) in compareExchangesWithExpiration)
            {
                var res = await store.Operations.SendAsync(new GetCompareExchangeValueOperation<User>(key));
                Assert.NotNull(res);
                Assert.Equal(user.Name, res.Value.Name);
                if (expiry != null)
                {
                    var expirationDate = res.Metadata.GetString(Constants.Documents.Metadata.Expires);
                    Assert.NotNull(expirationDate);
                    var dateTime = DateTime.ParseExact(expirationDate, DefaultFormat.DateTimeFormatsToRead, CultureInfo.InvariantCulture, DateTimeStyles.RoundtripKind);
                    Assert.Equal(expiry.Value.ToString("yyyy'-'MM'-'dd'T'HH':'mm':'ss.fffffff"), expirationDate);
                }
            }
        }

        private static async Task AddCompareExchangesWithExpire(int count, Dictionary<string, User> compareExchanges, DocumentStore store, DateTime? expiry = null)
        {
            for (int i = 0; i < count; i++)
            {
                var rnd = new Random(DateTime.Now.Millisecond);
                var user = new User { Name = new string(Enumerable.Repeat(_chars, 10).Select(s => s[rnd.Next(s.Length)]).ToArray()) };
                var key = $"{new string(Enumerable.Repeat(_chars, 10).Select(s => s[rnd.Next(s.Length)]).ToArray())}{i}";
                compareExchanges[key] = user;
                using (var session = store.OpenAsyncSession(new SessionOptions { TransactionMode = TransactionMode.ClusterWide }))
                {
                    var result = session.Advanced.ClusterTransaction.CreateCompareExchangeValue(key, user);
                    if (expiry != null)
                    {
                        result.Metadata[Constants.Documents.Metadata.Expires] = expiry;
                    }
                    await session.SaveChangesAsync();
                }
            }
        }
    }
}
