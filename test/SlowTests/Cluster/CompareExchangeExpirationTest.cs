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

                using (var requestExecutor = ClusterRequestExecutor.CreateForSingleNode(leader.WebUrl, null))
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

                    using (var requestExecutor = ClusterRequestExecutor.CreateForSingleNode(leader.WebUrl, null))
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

                using (var requestExecutor = ClusterRequestExecutor.CreateForSingleNode(leader.WebUrl, null))
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


        [Theory]
        [InlineData("SchemaUpgrade/Issues/SystemVersion/RavenDB_17999/CompareExchange_Expiration_RavenData_v5.3_from50000.zip", new[] { "a/fb0ruyd1b90", "a/n69zufl8xh1", "a/2ue3164y3t2", "a/0z56uosf7d3", "a/y3w9n6hwbw4", "a/6fytr1qre25", "a/4kpwjjf9im6", "a/bwrgnfo4ls7", "a/91ijgxdlpb8", "a/759m9f12sv9", "a/690p1xqjwf10", "a/qfyg516m9g11", "a/ojpjyjv3d012", "a/48to5aetjc13", "a/2ckqys2anv14", "a/0hbtr9rrrf15", "a/7teeu50mul16", "a/5x5gnnp4x517", "a/42wjg5dl1o18", "a/syb5y9ho6m19", "a/7ng9500ecy20", "a/qx541m5ytj21", "a/ftkpjr91xg22", "a/97tyy9ah9423", "a/dseon4xbkd24", "a/k4g8qz66nj25", "a/sgituvf1qp26", "a/ztkdyqpwtv27", "a/75nx1myrw028", "a/op0ygvtzys29", "a/w22jjq2u0y30", "a/3e53nlbp3431", "a/kyi41u6y5w32", "a/sako5qft8233", "a/znn98loob734", "a/g70anujwdz35", "a/oj2uqqsrg536", "a/53gw5zm0ix37", "a/dgig8uwvl338", "a/ksk0cq5qo939", "a/1cy2qzzzq040", "a/9p0muu9tt641", "a/q9en8332vy42", "a/xlg7cycxy443", "a/5xisgums1a44", "a/mivtu3g12245", "a/uuydyypw5746", "a/irdzg3tza547", "a/q3gjjy3udb48", "a/xfi3nucpgh49", "a/5rknqplkjn50", "a/mcyp5ygtlf51", "a/to098tpook52", "a/b8dbn2jwqc53", "a/ilgvqytrti54", "a/z5tw57n0va55", "a/gp7yjgh9x256", "a/o19inbr40857", "a/5lmj1klc2z58", "a/cyp45gu74559", "a/ti25jppg6x60", "a/9779qg76c961", "a/gj9uubh1ff62", "a/en0wnt5ijy63", "a/v8dy120rlq64", "a/csrzgbuzni65", "a/k4tkj63uqo66", "a/1o7lyfx3sg67", "a/i9kmcosbu868", "a/ztyoqxmkw069", "a/gdbp56gtyr70", "a/opda82qo1x71", "a/5arbnbkw3p72", "a/cmtvq6tr6v73", "a/kyvfu13m8174", "a/8vb1c66qdz75", "a/g7dlg1glg476", "a/xrrnuaatiw77", "a/54t7y6jol278", "a/mo78cfexnu79", "a/38kaqo86pm80", "a/ksyb5x2ere81", "a/r40v8sc9uj82", "a/8pdxn16iwb83", "a/p9ry1a0ry384", "a/xltj55am1985", "a/e57kje4u3186", "a/mi94nadp6787", "a/32m61j8y8z88", "a/aepq5ehta489", "a/ry2rjnb1cw90", "a/8jgtxw5aeo91", "a/gvid1sf5hu92", "a/xfvef19ejm93", "a/ez9gua3mle94", "a/vkmh8jyvn695", "a/c40jnss4py96", "a/todk11mcrp97", "a/10f45ww7uv98", "a/ilt6j5qgwn99", "a/z567xekoyf100", "a/gpk9cnex07101", "a/n1mtfios3d102", "a/4m0uuri155103", "a/l6dw80c97w104", "a/1ui0frvzc8105", "a/ifv2u0p8e0106", "a/zz9389khgs107", "a/gjm5niepik108", "a/x3061r8ykc109", "a/4g2q5mitnh110", "a/l0fsjvc1p9111", "a/2kttx46ar1112", "a/j46vcd0jtt113", "a/rh9ff9aewz114", "a/81mgui4myr115", "a/xx22cm8q3o116", "a/4a4mfihl6u117", "a/cm66jdqg90118", "a/t6k8xmlobs119", "a/aqx9cvfxdk120", "a/h30tfqosfq121", "a/pf2ejmyniv122", "a/xr4ynh7il1123", "a/446iqdhdo7124", "a/lokj5mbmqz125", "a/t0m48hkht5126", "a/0coocdubwb127", "a/hx2pqmoky3128", "a/p94auhxf18129", "a/6tib8qso30130", "a/e5kvcl1j66131", "a/vqxxquvr8y132", "a/220huq5mb4133", "a/hr4l1hncgf134", "a/p3655cx7jl135", "a/wf9q8862mr136", "a/2w2d5l4eth137", "a/a84x8he9wm138", "a/hl6hccn4zs139", "a/y5kiqlhd1k140", "a/w3dhuy5mnh141", "a/pldt1yvj3o142", "a/bmkhcko4c5143" })]
        [InlineData("SchemaUpgrade/Issues/SystemVersion/RavenDB_17999/CompareExchange_Expiration_RavenData_v5.2_from50000.zip", new[] { "a/rli91za5ny0", "a/p7g0r2xzbl1", "a/4wl4ytgohw2", "a/20c7rb46lg3", "a/aderu6e1om4", "a/8h5uno2ir65", "a/6lwxg6rzvp6", "a/dyyhj20uyv7", "a/b2pkckpb2f8", "a/97gn51dt6z9", "a/8b7qyj2aai10", "a/2ogzc13qm611", "a/0t725js7qp12", "a/fhc6caaxv113", "a/dm395szezl14", "a/bqtcyaow3515", "a/9vkfrscd7o16", "a/7zbija1ub817", "a/532lcspbfs18", "a/48to5aetjc19", "a/jwysc1wjpn20", "a/yl2wjrf9vz21", "a/wqtzc94qyj22", "a/syb5y9ho6m23", "a/20piji1uol24", "a/5l988dooyu25", "a/qr70ch4qbw26", "a/y39kgdele127", "a/5fb4j8ngh728", "a/m0p6yhiojz29", "a/ucrq1drjm530", "a/bw5rgmlsox31", "a/i97bjhvnr332", "a/ztkdyqpwtv33", "a/gdyeczj4vm34", "a/op0ygvtzys35", "a/59e0u4n8zk36", "a/dmgkyzw32q37", "a/u6tmc8qb4i38", "a/i397ucuf9f39", "a/znn98loob740", "a/7zptchyjed41", "a/fbrdgc7ehj42", "a/387zyhbhmh43", "a/bk9j1ckcpn44", "a/s4nkglflre45", "a/zhp5jhoguk46", "a/g126ypiowc47", "a/od4q1lsjzi48", "a/vq7a5g1e1o49", "a/dakcjpvn3g50", "a/kmmwnl5i6l51", "a/160x1uzr8d52", "a/zbr0ucn8cx53", "a/gv428lihep54", "a/xfi3nucpgh55", "a/5rknqplkjn56", "a/mcyp5ygtlf57", "a/to098tpook58", "a/b8dbn2jwqc59", "a/ilgvqytrti60", "a/7hvg82wvyg61", "a/m60lgtfl4r62", "a/ti25jppg6x63", "a/a2g6yyjo8p64", "a/smt8c7dxah65", "a/zzvsg2msdn66", "a/gj9uubh1ff67", "a/x3mv8kb9h768", "a/5gpfcfk4kc69", "a/csrzgbuzni70", "a/i9kmcosbu871", "a/qlm7gk16xe72", "a/opda82qo1x73", "a/5arbnbkw3p74", "a/cmtvq6tr6v75", "a/kyvfu13m8176", "a/1i9h8axvat77", "a/8vb1c66qdz78", "a/pfp3qf1zfq79", "a/6z245ov7hi80", "a/ojg5jxpgja81", "a/54t7y6jol282", "a/mo78cfexnu83", "a/t09tgansq084", "a/akmuujh1ss85", "a/ixpexfrwvx86", "a/p9ry1a0ry387", "a/6t40fjuz0v88", "a/e57kje4u3189", "a/mi94nadp6790", "a/32m61j8y8z91", "a/aepq5ehta492", "a/irra8aqoda93", "a/zb4bnjlwf294", "a/6n7wqeuri895", "a/n7kx5no0k096", "a/4syzjwj9ms97", "a/c40jnss4py98", "a/kg23qn1zs399", "a/8dip8r52x1100", "a/gpk9cnex07101", "a/veodjexn6j102", "a/2qrxn97i9o103", "a/a2tiq5gdbu104", "a/rm6j5eamdm105", "a/zz9389khgs106", "a/gjm5niepik107", "a/nvopqdnklq108", "a/v8r9u9xfov109", "a/cs4b8iroqn110", "a/j46vcd0jtt111", "a/rh9ff9aewz112", "a/ztbzj4j9z5113", "a/65djn0t42b114", "a/4a4mfihl6u115", "a/cm66jdqg90116", "a/aqx9cvfxdk117", "a/8voc5d4eg4118", "a/g7rw88d9j9119", "a/njthc4m4mf120", "a/446iqdhdo7121", "a/cg92u8q8rd122", "a/8pr8f836zg123", "a/rnkuxl7ahr124", "a/yzme1gh5jx125", "a/6boz5cq0m3126", "a/nv20jlk9ov127", "a/u84kngu4r0128", "a/9w8pu7dtxc129", "a/h9b9x2mo0i130", "a/ytoacbgx2a131", "a/65quf7qs5g132", "a/np4wugk177133", "a/u26gxbtwad134", "a/bmkhcko4c5135", "a/jym2fgxzfb136", "a/du2yqjxra1137", "a/uefz4sr0ct138", "a/2qhk8n1vfz139", "a/92k4cjaqi4140", "a/qnx5qs5zkw141", "a/yzzpunetn2142", "a/fjdr8w82ou143" })]
        [InlineData("SchemaUpgrade/Issues/SystemVersion/RavenDB_17999/CompareExchange_Expiration_RavenData_v5.2_new_from50000.zip", new[] { "a/9d4dgk8euw0", "a/g7dlg1glg41", "a/mo78cfexnu2", "a/ksyb5x2ere3", "a/ixpexfrwvx4", "a/p9ry1a0ry35", "a/ndi1usp82n6", "a/vqklxny35t7", "a/tuboq5nk8d8", "a/16d9u1wfbi9", "a/3wo2ce7qqb10", "a/10f45ww7uv11", "a/pxvqn0zbzt12", "a/4m0uuri15513", "a/tifgcwm4a214", "a/rm6j5eamdm15", "a/gjm5niepik16", "a/end8f026m317", "a/cs4b8iroqn18", "a/h9xy5vp0xd19", "a/nprl19nc4220", "a/luiourbt8m21", "a/jy9rn90bc622", "a/8voc5d4eg423", "a/6zffxvswkn24", "a/446iqdhdo725", "a/28xljv5usr26", "a/0coocdubwb27", "a/p94auhxf1828", "a/ndvcnzmw5s29", "a/tuo0jck9ci30", "a/rzf2cu8qf131", "a/ybinfqili732", "a/wf9q8862mr33", "a/uk0s1qvjqb34", "a/torvu8j1uv35", "a/hl6hccn4zs36", "a/px91f7wz2y37", "a/n1048plh6i38", "a/l6r7179ya239", "a/uw8c86tosq40", "a/jsoyqbxrxo41", "a/d6x64ty79b42", "a/hrhwuol1kk43", "a/fv8zm69in444", "a/m7bjq1jdqa45", "a/4sol4adms246", "a/b4q586mhv847", "a/so47mfgpx048", "a/98h81obyzr49", "a/qtv9fx561j50", "a/y5xujse14p51", "a/fpbvx19a6h52", "a/m1df1wi59n53", "a/3mqhf5cebf54", "a/byt1j1m9el55", "a/si62xaghgc56", "a/0u8n15pcji57", "a/hfmofeklla58", "a/oro8jatgog59", "a/5b2axjnop860", "a/1kkfji0nxb61", "a/0pbic0p41v62", "a/yt2l4idm5f63", "a/55468enh8l64", "a/mph7mnhpad65", "a/u2krqirkdi66", "a/9qowx99aju67", "a/qb1xci4jlm68", "a/7vfyqryrne69", "a/f7hjum7mqk70", "a/koa6q05zw971", "a/s0dquvftzf72", "a/9kqr84921773", "a/q54tmd3b3z74", "a/7phu1mxj5r75", "a/o9vwfvss7j76", "a/wlxgjr1nao77", "a/d6ahx0vwcg78", "a/kid21v5rfm79", "a/12q3f4zzhe80", "a/9esnjz8ukk81", "a/grv7mvipnq82", "a/xb8914cyph83", "a/5nat4zltsn84", "a/d0dd8vvout85", "a/ukqfm4pwwl86", "a/1wszqzzrzr87", "a/ig6048t01j88", "a/qt8l842v4o89", "a/7dmmmdw46g90", "a/epo6q86z9m91", "a/m1qqu3ftcs92", "a/3m4s8c92ek93", "a/by6cc8jxhq94", "a/sijdqhd6ji95", "a/77oix8wwpt96", "a/ejq2135rsz97", "a/v344fc0zur98", "a/3g6oj89uwx99", "a/as88m3ipz3100", "a/scm91cdy1u101", "a/zpou47mt40102", "a/g91vjgg16s103", "a/ol4fmcqw9y104", "a/55hh1lk5bq105", "a/cij14gt0ew106", "a/t2x2jpo9gn107", "a/1ezmmlx4jt108", "a/iydo1urcll109", "a/qbf84p17or110", "a/7vsajyvgqj111", "a/e7vumt4btp112", "a/vr8v12zjvh113", "a/34af4y8exm114", "a/koohj72nze115", "a/bil9yxkwzz116", "a/ivnt2ttr25117", "a/q7qe5o2m5a118", "a/7r3fkxxu72119", "a/e45zns6pa8120", "a/mg7jrogkce121", "a/30ll5xate6122", "a/acn59sjohc123", "a/ipppcotjkh124", "a/xduukfb9qt125", "a/5qwenal4tz126", "a/maaf2jfcvr127", "a/tmcz5eo7yx128", "a/1zek9ay212129", "a/8bg4c57x48130", "a/5kyay5kwcc131", "a/tgevgaozg9132", "a/1tgfk5xujf133", "a/iduhyes3l7134", "a/ppw1291yod135", "a/x1yl55atrj136", "a/auut5ei01e137", "a/zraenil46c138", "a/xv1hg0alaw139", "a/egeju94tcn140", "a/msg3y5eoft141", "a/t4jn20njhz142", "a/1hl75vwek5143" })]
        public void AllCompareExchangeAndExpirationPreserveAfterServerSchemaUpgradeFrom50000(string path, string[] expected)
        {
            var folder = NewDataPath(forceCreateDir: true);
            DoNotReuseServer();

            var zipPath = new PathSetting(path);
            Assert.True(File.Exists(zipPath.FullPath));

            ZipFile.ExtractToDirectory(zipPath.FullPath, folder);

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
