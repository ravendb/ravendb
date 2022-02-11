using System;
using System.Collections.Generic;
using System.Globalization;
using System.IO;
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
using Raven.Client.ServerWide;
using Raven.Client.ServerWide.Commands.Cluster;
using Raven.Client.ServerWide.Operations;
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
            using var server = GetNewServer();
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
