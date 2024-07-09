using System;
using System.Collections.Generic;
using System.Diagnostics;
using System.IO;
using System.Linq;
using System.Runtime.CompilerServices;
using System.Threading;
using System.Threading.Tasks;
using FastTests.Server.Replication;
using FastTests.Utils;
using Nito.AsyncEx;
using Raven.Client;
using Raven.Client.Documents;
using Raven.Client.Documents.Conventions;
using Raven.Client.Documents.Indexes;
using Raven.Client.Documents.Operations.CompareExchange;
using Raven.Client.Documents.Operations.Revisions;
using Raven.Client.Documents.Session;
using Raven.Client.Documents.Smuggler;
using Raven.Client.Exceptions;
using Raven.Client.Http;
using Raven.Client.ServerWide;
using Raven.Client.ServerWide.Operations;
using Raven.Client.Util;
using Raven.Server;
using Raven.Server.Config;
using Raven.Server.Config.Settings;
using Raven.Server.Documents;
using Raven.Server.Rachis;
using Raven.Server.ServerWide.Commands;
using Raven.Server.ServerWide.Context;
using Raven.Server.ServerWide.Maintenance;
using Raven.Tests.Core.Utils.Entities;
using Tests.Infrastructure;
using Xunit;
using Xunit.Abstractions;

namespace SlowTests.Cluster
{
    public class ClusterTransactionTests : ReplicationTestBase
    {
        public ClusterTransactionTests(ITestOutputHelper output) : base(output)
        {
        }

        protected override RavenServer GetNewServer(ServerCreationOptions options = null, [CallerMemberName]string caller = null)
        {
            options ??= new ServerCreationOptions();
            options.CustomSettings ??= new Dictionary<string, string>();

            if(options.CustomSettings.ContainsKey(RavenConfiguration.GetKey(x => x.Cluster.OperationTimeout)) == false)
                options.CustomSettings[RavenConfiguration.GetKey(x => x.Cluster.OperationTimeout)] = "60";
            
            if(options.CustomSettings.ContainsKey(RavenConfiguration.GetKey(x => x.Cluster.StabilizationTime)) == false)
                options.CustomSettings[RavenConfiguration.GetKey(x => x.Cluster.StabilizationTime)] = "10";

            if(options.CustomSettings.ContainsKey(RavenConfiguration.GetKey(x => x.Cluster.TcpConnectionTimeout)) == false)
                options.CustomSettings[RavenConfiguration.GetKey(x => x.Cluster.TcpConnectionTimeout)] = "30000";

            return base.GetNewServer(options, caller);
        }

        [Fact]
        public async Task ThrowOnTooLargeClusterTransactionRequest()
        {
            var (_, leader) = await CreateRaftCluster(3, customSettings: new Dictionary<string, string>
            {
                [RavenConfiguration.GetKey(x => x.Cluster.MaxSizeOfSingleRaftCommand)] = "1"
            });
            using (var leaderStore = GetDocumentStore(new Options
                   {
                       Server = leader,
                       ReplicationFactor = 3,
                       
                   }))
            {
                var user1 = new User()
                {
                    Name = "Karmel"
                };
                var user3 = new User()
                {
                    Name = "Indych"
                };

                using (var session = leaderStore.OpenAsyncSession(new SessionOptions
                       {
                           TransactionMode = TransactionMode.ClusterWide
                       }))
                {
                    session.Advanced.ClusterTransaction.CreateCompareExchangeValue("usernames/ayende", new byte[256 * 1024]);
                    await session.StoreAsync(user3, "foo/bar");
                    var ex = await Assert.ThrowsAsync<RavenException>(() => session.SaveChangesAsync());
                    Assert.Contains("The command 'ClusterTransactionCommand' size of 1.5 MBytes exceed the max allowed size", ex.Message);
                }

                using (var session = leaderStore.OpenAsyncSession(new SessionOptions
                       {
                           TransactionMode = TransactionMode.ClusterWide
                       }))
                {
                    session.Advanced.ClusterTransaction.CreateCompareExchangeValue("usernames/ayende", user1);
                    await session.StoreAsync(user3, "foo/bar");
                    await session.SaveChangesAsync();

                    var user = (await session.Advanced.ClusterTransaction.GetCompareExchangeValueAsync<User>("usernames/ayende")).Value;
                    Assert.Equal(user1.Name, user.Name);
                    user = await session.LoadAsync<User>("foo/bar");
                    Assert.Equal(user3.Name, user.Name);
                }
            }
        }

        [Fact]
        public async Task CanCreateClusterTransactionRequest()
        {
            var (_, leader) = await CreateRaftCluster(3);
            using (var leaderStore = GetDocumentStore(new Options
            {
                Server = leader,
                ReplicationFactor = 3
            }))
            {
                var user1 = new User()
                {
                    Name = "Karmel"
                };
                var user3 = new User()
                {
                    Name = "Indych"
                };

                using (var session = leaderStore.OpenAsyncSession(new SessionOptions
                {
                    TransactionMode = TransactionMode.ClusterWide
                }))
                {
                    session.Advanced.ClusterTransaction.CreateCompareExchangeValue("usernames/ayende", user1);
                    await session.StoreAsync(user3, "foo/bar");
                    await session.SaveChangesAsync();

                    var user = (await session.Advanced.ClusterTransaction.GetCompareExchangeValueAsync<User>("usernames/ayende")).Value;
                    Assert.Equal(user1.Name, user.Name);
                    user = await session.LoadAsync<User>("foo/bar");
                    Assert.Equal(user3.Name, user.Name);
                }
            }
        }

        [RavenTheory(RavenTestCategory.ClusterTransactions)]
        [RavenData(DatabaseMode = RavenDatabaseMode.All)]
        public async Task ServeSeveralClusterTransactionRequests(Options options)
        {
            using (var store = GetDocumentStore(options))
            using (var session = store.OpenAsyncSession(new SessionOptions
            {
                TransactionMode = TransactionMode.ClusterWide
            }))
            {
                var user1 = new User()
                {
                    Name = "Karmel"
                };

                session.Advanced.ClusterTransaction.CreateCompareExchangeValue("usernames/karmel", user1);
                await session.SaveChangesAsync();

                var result = (await session.Advanced.ClusterTransaction.GetCompareExchangeValueAsync<User>("usernames/karmel"));
                Assert.Equal(user1.Name, result.Value.Name);

                await session.StoreAsync(user1, "users/1");
                await session.SaveChangesAsync();

                var user = await session.LoadAsync<User>("users/1");
                Assert.Equal(user1.Name, user.Name);
            }
        }

        private Random _random = new Random();

        private string RandomString(int length)
        {
            const string chars = "abcdefghijklmnopqrstuvwxyzABCDEFGHIJKLMNOPQRSTUVWXYZ1234567890 ";
            var str = new char[length];
            for (int i = 0; i < length; i++)
            {
                str[i] = chars[_random.Next(chars.Length)];
            }
            return new string(str);
        }

        [Theory]
        [InlineData(1)]
        [InlineData(3)]
        [InlineData(5)]
        public async Task CanPreformSeveralClusterTransactions(int numberOfNodes)
        {
            var numOfSessions = 10;
            var docsPerSession = 2;
            var (_, leader) = await CreateRaftCluster(numberOfNodes);
            using (var store = GetDocumentStore(new Options
            {
                Server = leader,
                ReplicationFactor = numberOfNodes
            }))
            {
                for (int j = 0; j < numOfSessions; j++)
                {
                    var trys = 5;
                    do
                    {
                        try
                        {
                            using (var session = store.OpenAsyncSession(new SessionOptions
                            {
                                TransactionMode = TransactionMode.ClusterWide
                            }))
                            {
                                for (int i = 0; i < docsPerSession; i++)
                                {
                                    var user = new User
                                    {
                                        LastName = RandomString(2048),
                                        Age = i
                                    };
                                    using (var cts = new CancellationTokenSource(TimeSpan.FromMinutes(numberOfNodes)))
                                    {
                                        await session.StoreAsync(user, "users/" + (docsPerSession * j + i + 1), cts.Token);
                                    }
                                }

                                if (numberOfNodes > 1)
                                    await ActionWithLeader(async l =>
                                    {
                                        using var cts = new CancellationTokenSource(TimeSpan.FromSeconds(10));
                                        var waitForLeaderChangeTask = l.ServerStore.Engine.WaitForLeaderChange(cts.Token);
                                        l.ServerStore.Engine.CurrentLeader?.StepDown();
                                        await waitForLeaderChangeTask;
                                    });

                                using (var cts = new CancellationTokenSource(TimeSpan.FromMinutes(numberOfNodes)))
                                {
                                    await session.SaveChangesAsync(cts.Token);
                                }

                                trys = 5;
                            }
                        }
                        catch (Exception e) when (e is ConcurrencyException)
                        {
                            trys--;
                        }
                    } while (trys < 5 && trys > 0);

                    Assert.True(trys > 0, $"Couldn't save a document after 5 retries.");
                }

                using (var session = store.OpenAsyncSession())
                {
                    using (var cts = new CancellationTokenSource(TimeSpan.FromMinutes(numberOfNodes)))
                    {
                        var res = await session.Query<User>().Customize(q => q.WaitForNonStaleResults()).ToListAsync(cts.Token);
                        Assert.Equal(numOfSessions * docsPerSession, res.Count);
                    }
                }
            }
        }

        [Theory]
        [InlineData(1)]
        [InlineData(5)]
        [InlineData(10)]
        public async Task ClusterTransactionWaitForIndexes(int docs)
        {
            var (_, leader) = await CreateRaftCluster(3);
            using (var leaderStore = GetDocumentStore(new Options
            {
                Server = leader,
                ReplicationFactor = 3
            }))
            {
                new UserByName().Execute(leaderStore);

                var users = new List<User>();
                using (var session = leaderStore.OpenAsyncSession(new SessionOptions
                {
                    TransactionMode = TransactionMode.ClusterWide
                }))
                {
                    session.Advanced.WaitForIndexesAfterSaveChanges();
                    for (int i = 0; i < docs; i++)
                    {
                        var user = new User
                        {
                            Name = "Karmel" + i
                        };
                        users.Add(user);
                        await session.StoreAsync(user, "users/" + i);
                    }
                    await session.SaveChangesAsync();

                    var stored = await session.LoadAsync<User>(users.Select(u => u.Id));
                    Assert.Equal(stored.Select(u => u.Value.Name), users.Select(u => u.Name));
                }
            }
        }

        [Fact]
        public async Task CanImportExportAndBackupWithClusterTransactions()
        {
            var file = GetTempFileName();

            var (_, leader) = await CreateRaftCluster(3, watcherCluster: true);
            var user1 = new User() {Name = "Karmel"};
            var user2 = new User() {Name = "Oren"};
            var user3 = new User() {Name = "Indych"};

            var toDispose = Servers.First(s => s != leader);
            var removedTag = toDispose.ServerStore.NodeTag;

            var members = new List<string> {"A", "B", "C"};
            members.Remove(removedTag);

            Cluster.SuspendObserver(leader);

            using (var store = GetDocumentStore(new Options {Server = leader, ModifyDatabaseRecord = r => r.Topology = new DatabaseTopology {Members = members}}))
            {
                // we kill one server so we would not clean the pending cluster transactions.
                await DisposeAndRemoveServer(toDispose);

                using (var session = store.OpenAsyncSession(new SessionOptions {
                    TransactionMode = TransactionMode.ClusterWide,
                    DisableAtomicDocumentWritesInClusterWideTransaction = true
                }))
                {
                    session.Advanced.ClusterTransaction.CreateCompareExchangeValue("usernames/karmel", user1);
                    await session.StoreAsync(user1, "foo/bar");
                    await session.StoreAsync(new User(), "foo/bar2");
                    await session.SaveChangesAsync();
                    session.Advanced.Clear();

                    session.Advanced.ClusterTransaction.CreateCompareExchangeValue("usernames/ayende", user2);
                    await session.StoreAsync(user2, "foo/bar");
                    await session.StoreAsync(new User(), "foo/bar3");
                    await session.SaveChangesAsync();
                    session.Advanced.Clear();

                    session.Advanced.SetTransactionMode(TransactionMode.SingleNode);
                    await session.StoreAsync(user3, "foo/bar");
                    await session.SaveChangesAsync();
                    session.Advanced.Clear();

                    session.Advanced.SetTransactionMode(TransactionMode.ClusterWide);
                    var user = (await session.Advanced.ClusterTransaction.GetCompareExchangeValueAsync<User>("usernames/ayende")).Value;
                    Assert.Equal(user2.Name, user.Name);
                    user = await session.LoadAsync<User>("foo/bar");
                    Assert.Equal(user3.Name, user.Name);
                }

                var operation = await store.Smuggler.ExportAsync(new DatabaseSmugglerExportOptions(), file);
                await operation.WaitForCompletionAsync(TimeSpan.FromMinutes(1));
            }

            using (var store = GetDocumentStore(new Options {Server = leader, ModifyDatabaseRecord = r => r.Topology = new DatabaseTopology {Members = members}}))
            {
                using (var session = store.OpenAsyncSession())
                {
                    await session.StoreAsync(user1, "foo/bar");
                    await session.SaveChangesAsync();
                    session.Advanced.Evict(user1);

                    var operation = await store.Smuggler.ImportAsync(new DatabaseSmugglerImportOptions(), file);

                    // change the primary node
                    var record = await store.Maintenance.Server.SendAsync(new GetDatabaseRecordOperation(store.Database));
                    record.Topology.Members.Reverse();
                    await store.Maintenance.Server.SendAsync(new ReorderDatabaseMembersOperation(store.Database, record.Topology.Members));
                    await store.GetRequestExecutor().UpdateTopologyAsync(new RequestExecutor.UpdateTopologyParameters(new ServerNode
                    {
                        Url = store.Urls[0],
                        Database = store.Database
                    }));

                    await operation.WaitForCompletionAsync(TimeSpan.FromMinutes(1));

                    Assert.True(WaitForDocument<User>(store, "foo/bar", u => user3.Name == u.Name));
                }
            }
        }

        public class UniqueUser
        {
            public string Id { get; set; }
            public string Email { get; set; }
            public string Name { get; set; }
        }

        [Fact]
        public async Task TestSessionSequance()
        {
            var user1 = new User()
            {
                Name = "Karmel"
            };
            var user2 = new User()
            {
                Name = "Indych"
            };

            using (var store = GetDocumentStore())
            using (var session = store.OpenAsyncSession(new SessionOptions
            {
                TransactionMode = TransactionMode.ClusterWide,
                DisableAtomicDocumentWritesInClusterWideTransaction = true
            }))
            {
                session.Advanced.ClusterTransaction.CreateCompareExchangeValue("usernames/ayende", user1);
                await session.StoreAsync(user1, "users/1");
                await session.SaveChangesAsync();

                var value = await session.Advanced.ClusterTransaction.GetCompareExchangeValueAsync<User>("usernames/ayende");
                value.Value = user2;
                await session.StoreAsync(user2, "users/2");
                user1.Age = 10;
                await session.StoreAsync(user1, "users/1");
                await session.SaveChangesAsync();
            }
        }

        [Fact]
        public async Task ResolveInFavorOfClusterTransaction()
        {
            var user1 = new User()
            {
                Name = "Karmel"
            };
            var user2 = new User()
            {
                Name = "Indych"
            };
            using (var store1 = GetDocumentStore())
            using (var store2 = GetDocumentStore())
            {
                using (var session = store2.OpenAsyncSession())
                {
                    await session.StoreAsync(user2, "users/1");
                    await session.SaveChangesAsync();
                }

                using (var session = store1.OpenAsyncSession(new SessionOptions
                {
                    TransactionMode = TransactionMode.ClusterWide
                }))
                {
                    await session.StoreAsync(user1, "users/1");
                    await session.SaveChangesAsync();
                }

                await SetupReplicationAsync(store1, store2);
                Assert.True(WaitForDocument<User>(store2, "users/1", (u) => u.Name == "Karmel"));
            }
        }

        [Fact]
        public async Task TestCleanUpClusterState()
        {
            DoNotReuseServer();
            var user1 = new User()
            {
                Name = "Karmel"
            };
            var user2 = new User()
            {
                Name = "Indych"
            };
            var user3 = new User()
            {
                Name = "Indych"
            };

            using (var store = GetDocumentStore())
            {
                using (var session = store.OpenAsyncSession(new SessionOptions
                {
                    TransactionMode = TransactionMode.ClusterWide,
                    DisableAtomicDocumentWritesInClusterWideTransaction = true
                }))
                {
                    session.Advanced.ClusterTransaction.CreateCompareExchangeValue("usernames/ayende", user1);
                    await session.StoreAsync(user1);
                    await session.StoreAsync(user2);
                    await session.StoreAsync(user3);
                    await session.SaveChangesAsync();
                }

                using (var session = store.OpenAsyncSession(new SessionOptions
                {
                    TransactionMode = TransactionMode.ClusterWide,
                    DisableAtomicDocumentWritesInClusterWideTransaction = true
                }))
                {
                    session.Advanced.ClusterTransaction.DeleteCompareExchangeValue("usernames/ayende", store.GetLastTransactionIndex(store.Database) ?? 0);
                    await session.StoreAsync(user1);
                    await session.StoreAsync(user2);
                    await session.StoreAsync(user3);
                    await session.SaveChangesAsync();
                }

                using (var session = store.OpenAsyncSession(new SessionOptions
                {
                    TransactionMode = TransactionMode.ClusterWide,
                    DisableAtomicDocumentWritesInClusterWideTransaction = true
                }))
                {
                    session.Advanced.ClusterTransaction.CreateCompareExchangeValue("usernames/ayende", user1);
                    await session.StoreAsync(user1);
                    await session.StoreAsync(user2);
                    await session.StoreAsync(user3);
                    await session.SaveChangesAsync();
                }

                var val = await WaitForValueAsync(() =>
                {
                    using (Server.ServerStore.Engine.ContextPool.AllocateOperationContext(out ClusterOperationContext ctx))
                    using (ctx.OpenReadTransaction())
                    {
                        return ClusterTransactionCommand.ReadFirstClusterTransaction(ctx, store.Database)?.PreviousCount ?? 0;
                    }
                }, 0);

                Assert.Equal(0, val);
            }
        }

        [Fact]
        public async Task TestConcurrentClusterSessions()
        {
            var user1 = new User()
            {
                Name = "Karmel"
            };
            var user3 = new User()
            {
                Name = "Indych"
            };
            var mre1 = new AsyncManualResetEvent();
            var mre2 = new AsyncManualResetEvent();
            using (var store = GetDocumentStore())
            {
                var task1 = Task.Run(async () =>
                {
                    using (var session = store.OpenAsyncSession(new SessionOptions
                    {
                        TransactionMode = TransactionMode.ClusterWide
                    }))
                    {
                        mre1.Set();
                        await mre2.WaitAsync();
                        session.Advanced.ClusterTransaction.CreateCompareExchangeValue("usernames/ayende", user1);
                        await session.StoreAsync(user1, "users/1");
                        await session.SaveChangesAsync();
                    }
                });

                var task2 = Task.Run(async () =>
                {
                    using (var session = store.OpenAsyncSession(new SessionOptions
                    {
                        TransactionMode = TransactionMode.ClusterWide
                    }))
                    {
                        mre2.Set();
                        await mre1.WaitAsync();
                        session.Advanced.ClusterTransaction.CreateCompareExchangeValue("usernames/karmel", user3);
                        await session.StoreAsync(user3, "users/3");
                        await session.SaveChangesAsync();
                    }
                });

                await Task.WhenAll(task1, task2);

                using (var session = store.OpenAsyncSession())
                {
                    var user = await session.LoadAsync<User>("users/1");
                    Assert.Equal(user1.Name, user.Name);
                }
            }
        }

        [Fact]
        public async Task TestSessionMixture()
        {
            var user1 = new User()
            {
                Name = "Karmel"
            };
            var user3 = new User()
            {
                Name = "Indych"
            };

            using (var store = GetDocumentStore())
            {
                string changeVector = null;
                using (var session = store.OpenAsyncSession(new SessionOptions
                {
                    TransactionMode = TransactionMode.ClusterWide
                }))
                {
                    session.Advanced.ClusterTransaction.CreateCompareExchangeValue("usernames/ayende", user1);
                    await session.StoreAsync(user1);
                    await session.SaveChangesAsync();
                    changeVector = session.Advanced.GetChangeVectorFor(user1);
                }
                using (var session = store.OpenAsyncSession())
                {
                    session.Advanced.UseOptimisticConcurrency = true;
                    await session.StoreAsync(user1, changeVector);
                    await session.SaveChangesAsync();
                }
            }
        }

        [Fact]
        public async Task CreateUniqueUser()
        {
            var (_, leader) = await CreateRaftCluster(3);

            using (var store = GetDocumentStore(new Options
            {
                Server = leader,
                ReplicationFactor = 3
            }))
            {
                var email = "grisha@ayende.com";
                var userId = $"users/{email}";

                var task1 = Task.Run(async () => await AddUser(store, email, userId));
                var task2 = Task.Run(async () => await AddUser(store, email, userId));
                var task3 = Task.Run(async () =>
                {
                    using (var session = store.OpenAsyncSession(new SessionOptions{NoTracking = true}))
                    {
                        while (true)
                        {
                            var user = await session.LoadAsync<UniqueUser>(userId);
                            if (user != null)
                                break;

                            await Task.Delay(500);
                            if (task1.IsCompleted && task2.IsCompleted)
                                break;
                        }

                        var userNew = await session.LoadAsync<UniqueUser>(userId);
                        Assert.NotNull(userNew);
                    }
                });

                await Task.WhenAll(task1, task2, task3);

                Assert.True(task1.IsCompletedSuccessfully);
                Assert.True(task2.IsCompletedSuccessfully);
                Assert.True(task3.IsCompletedSuccessfully);
                Assert.Equal(1, task1.Result + task2.Result);
            }
        }

        private static async Task<int> AddUser(IDocumentStore leaderStore, string email, string userId)
        {
            try
            {
                using (var session = leaderStore.OpenAsyncSession(new SessionOptions
                { TransactionMode = TransactionMode.ClusterWide }))
                {
                    var user = new UniqueUser
                    {
                        Name = "Grisha",
                        Email = email
                    };
                    await session.StoreAsync(user, userId);
                    session.Advanced.ClusterTransaction.CreateCompareExchangeValue(email, userId);

                    await session.SaveChangesAsync();
                }
            }
            catch (ConcurrencyException)
            {
                return 1;
            }

            return 0;
        }

        [Fact]
        public async Task SessionCompareExchangeCommands()
        {
            using (var store = GetDocumentStore())
            using (var session = store.OpenAsyncSession(new SessionOptions
            {
                TransactionMode = TransactionMode.ClusterWide
            }))
            {
                var user1 = new User()
                {
                    Name = "Karmel"
                };
                var user3 = new User()
                {
                    Name = "Indych"
                };

                session.Advanced.ClusterTransaction.CreateCompareExchangeValue("usernames/ayende", user1);
                session.Advanced.ClusterTransaction.CreateCompareExchangeValue("usernames/karmel", user1);
                session.Advanced.ClusterTransaction.CreateCompareExchangeValue("usernames/grisha", user1);
                await session.SaveChangesAsync();

                var user = (await session.Advanced.ClusterTransaction.GetCompareExchangeValueAsync<User>("usernames/ayende"));
                Assert.Equal(user1.Name, user.Value.Name);

                var values = await session.Advanced.ClusterTransaction.GetCompareExchangeValuesAsync<User>(new[] { "usernames/ayende", "usernames/karmel", "usernames/grisha" });
                Assert.Equal(3, values.Count);
                Assert.Equal(user.Index, values["usernames/ayende"].Index);
            }
        }

        [Fact]
        public async Task ClusterTxWithCounters()
        {
            using (var storeA = GetDocumentStore())
            {
                using (var session = storeA.OpenAsyncSession(new SessionOptions
                {
                    TransactionMode = TransactionMode.ClusterWide
                }))
                {
                    await session.StoreAsync(new User { Name = "Aviv1" }, "users/1");
                    await session.SaveChangesAsync();
                }

                using (var session = storeA.OpenAsyncSession())
                {
                    session.CountersFor("users/1").Increment("likes", 10);
                    await session.SaveChangesAsync();
                }

                using (var session = storeA.OpenAsyncSession())
                {
                    var user = await session.LoadAsync<User>("users/1");
                    var flags = session.Advanced.GetMetadataFor(user)[Constants.Documents.Metadata.Flags];
                    var list = session.Advanced.GetCountersFor(user);
                    Assert.Equal((DocumentFlags.HasCounters).ToString(), flags);
                    Assert.Equal(1, list.Count);
                }
            }
        }

        [Fact]
        public void ThrowOnClusterTransactionWithCounters()
        {
            using (var store = GetDocumentStore())
            {
                using (var session = store.OpenSession())
                {
                    session.Store(new User { Name = "Aviv1" }, "users/1-A");
                    session.CountersFor("users/1-A").Increment("likes", 100);
                    session.SaveChanges();
                }

                using (var session = store.OpenSession(new SessionOptions
                {
                    TransactionMode = TransactionMode.ClusterWide
                }))
                {
                    var user = session.Load<User>("users/1-A");
                    user.Name = "karmel";
                    var e = Assert.Throws<RavenException>(() => session.SaveChanges());
                    Assert.Equal(typeof(NotSupportedException), e.InnerException.GetType());
                }
            }
        }

        [Fact]
        public void ThrowOnClusterTransactionWithAttachments()
        {
            using (var store = GetDocumentStore())
            {
                using (var session = store.OpenSession())
                {
                    var user = new User
                    {
                        Name = "Aviv1"
                    };
                    session.Store(user, "users/1-A");
                    using (var ms = new MemoryStream(new byte[] { 1, 2, 3, 4, 5 }))
                    {
                        session.Advanced.Attachments.Store(user, "dummy", ms);
                        session.SaveChanges();
                    }
                }

                using (var session = store.OpenSession(new SessionOptions
                {
                    TransactionMode = TransactionMode.ClusterWide
                }))
                {
                    var user = session.Load<User>("users/1-A");
                    user.Name = "karmel";
                    var e = Assert.Throws<RavenException>(() => session.SaveChanges());
                    Assert.Equal(typeof(NotSupportedException), e.InnerException.GetType());
                }
            }
        }

        [Fact]
        public void ThrowOnClusterTransactionWithTimeSeries()
        {
            using (var store = GetDocumentStore())
            {
                using (var session = store.OpenSession())
                {
                    session.Store(new User { Name = "Aviv1" }, "users/1-A");
                    session.TimeSeriesFor("users/1-A", "Heartrate").Append(RavenTestHelper.UtcToday, new[] { 55d }, "watches/apple");
                    session.SaveChanges();
                }

                using (var session = store.OpenSession(new SessionOptions
                {
                    TransactionMode = TransactionMode.ClusterWide
                }))
                {
                    var user = session.Load<User>("users/1-A");
                    user.Name = "karmel";
                    var e = Assert.Throws<RavenException>(() => session.SaveChanges());
                    Assert.Equal(typeof(NotSupportedException), e.InnerException.GetType());
                }
            }
        }

        [Fact]
        public async Task ModifyDocumentWithRevision()
        {
            using (var store = GetDocumentStore())
            {
                var configuration = new RevisionsConfiguration { Default = new RevisionsCollectionConfiguration { Disabled = false } };
                await store.Maintenance.SendAsync(new ConfigureRevisionsOperation(configuration));

                using (var session = store.OpenAsyncSession(new SessionOptions
                {
                    TransactionMode = TransactionMode.ClusterWide,
                    DisableAtomicDocumentWritesInClusterWideTransaction = true
                }))
                {
                    await session.StoreAsync(new User { Name = "Aviv1" }, "users/1");
                    await session.SaveChangesAsync();
                }

                using (var session = store.OpenAsyncSession(new SessionOptions
                {
                    TransactionMode = TransactionMode.ClusterWide,
                    DisableAtomicDocumentWritesInClusterWideTransaction = true
                }))
                {
                    await session.StoreAsync(new User { Name = "Aviv2" }, "users/1");
                    await session.SaveChangesAsync();

                    var list = await session.Advanced.Revisions.GetForAsync<User>("users/1");
                    Assert.Equal(2, list.Count);
                }

                using (var session = store.OpenAsyncSession(new SessionOptions
                {
                    TransactionMode = TransactionMode.ClusterWide,
                    DisableAtomicDocumentWritesInClusterWideTransaction = true
                }))
                {
                    session.Delete("users/1");
                    await session.SaveChangesAsync();

                    var list = await session.Advanced.Revisions.GetForAsync<User>("users/1");
                    Assert.Equal(3, list.Count);
                }

                using (var session = store.OpenAsyncSession())
                {
                    await session.StoreAsync(new User { Name = "Aviv2" }, "users/1");
                    await session.SaveChangesAsync();

                    var list = await session.Advanced.Revisions.GetForAsync<User>("users/1");
                    Assert.Equal(4, list.Count);
                }
            }
        }

        [Fact]
        public async Task PutDocumentInDifferentCollectionWithRevision()
        {
            using (var store = GetDocumentStore())
            {
                using (var session = store.OpenAsyncSession(new SessionOptions
                {
                    TransactionMode = TransactionMode.ClusterWide,
                    DisableAtomicDocumentWritesInClusterWideTransaction = true
                }))
                {
                    await session.StoreAsync(new User { Name = "Aviv1" }, "users/1");
                    await session.SaveChangesAsync();
                }

                using (var session = store.OpenAsyncSession(new SessionOptions
                {
                    TransactionMode = TransactionMode.ClusterWide,
                    DisableAtomicDocumentWritesInClusterWideTransaction = true
                }))
                {
                    await session.StoreAsync(new Employee { FirstName = "Aviv2" }, "users/1");
                    await session.SaveChangesAsync();
                }
            }
        }

        [Fact]
        public async Task PutDocumentInDifferentCollection()
        {
            using (var store = GetDocumentStore())
            {
                var configuration = new RevisionsConfiguration { Default = new RevisionsCollectionConfiguration { Disabled = false } };
                await store.Maintenance.SendAsync(new ConfigureRevisionsOperation(configuration));

                using (var session = store.OpenAsyncSession(new SessionOptions
                {
                    TransactionMode = TransactionMode.ClusterWide,
                    DisableAtomicDocumentWritesInClusterWideTransaction = true
                }))
                {
                    await session.StoreAsync(new User { Name = "Aviv1" }, "users/1");
                    await session.SaveChangesAsync();
                }

                using (var session = store.OpenAsyncSession(new SessionOptions
                {
                    TransactionMode = TransactionMode.ClusterWide,
                    DisableAtomicDocumentWritesInClusterWideTransaction = true
                }))
                {
                    await session.StoreAsync(new Employee { FirstName = "Aviv2" }, "users/1");
                    await session.SaveChangesAsync();

                    var list = await session.Advanced.Revisions.GetForAsync<User>("users/1");
                    Assert.Equal(2, list.Count);
                }
            }
        }

        /// <summary>
        /// This is a comprehensive test. The general flow of the test is as following:
        /// - Create cluster with 5 nodes with a database on _all_ of them and enable revisions.
        /// - Bring one node down, he will later be used to verify the correct behavior (our SUT).
        /// - Perform a cluster transaction which involves a document.
        /// - Bring all nodes down except of the original leader.
        /// - Bring the SUT node back up and wait for the document to replicate.
        /// - Bring another node up in order to have a majority.
        /// - Wait for the raft index on the SUT to catch-up and verify that we still have one document with one revision.
        /// </summary>
        /// <returns></returns>
        [Fact]
        public async Task ClusterTransactionRequestWithRevisions()
        {
            var (_, leader) = await CreateRaftCluster(5, shouldRunInMemory: false, leaderIndex: 0);
            using (var leaderStore = GetDocumentStore(new Options
            {
                DeleteDatabaseOnDispose = false,
                Server = leader,
                ReplicationFactor = 5,
                ModifyDocumentStore = (store) => store.Conventions.DisableTopologyUpdates = true
            }))
            {
                var user1 = new User()
                {
                    Name = "Karmel"
                };
                var user3 = new User()
                {
                    Name = "Indych"
                };
                var index = await RevisionsHelper.SetupRevisions(leader.ServerStore, leaderStore.Database, configuration => configuration.Collections["Users"].PurgeOnDelete = false);
                await Cluster.WaitForRaftIndexToBeAppliedInClusterAsync(index, TimeSpan.FromSeconds(15));

                // bring our SUT node down, but we still have a cluster and can execute cluster transaction.
                var server = Servers[1];
                var result1 = await DisposeServerAndWaitForFinishOfDisposalAsync(server);

                using (var session = leaderStore.OpenAsyncSession(new SessionOptions { TransactionMode = TransactionMode.ClusterWide }))
                {
                    Assert.Equal(1, session.Advanced.RequestExecutor.TopologyNodes.Count);
                    Assert.Equal(leader.WebUrl, session.Advanced.RequestExecutor.Url);
                    session.Advanced.ClusterTransaction.CreateCompareExchangeValue("usernames/ayende", user1);
                    await session.StoreAsync(user3, "foo/bar");
                    await session.StoreAsync(user1, "foo/bar/2");
                    await session.SaveChangesAsync();

                    var user = (await session.Advanced.ClusterTransaction.GetCompareExchangeValueAsync<User>("usernames/ayende")).Value;
                    Assert.Equal(user1.Name, user.Name);
                    user = await session.LoadAsync<User>("foo/bar");
                    Assert.Equal(user3.Name, user.Name);

                    var list = await session.Advanced.Revisions.GetForAsync<User>(user.Id);
                    Assert.Equal(1, list.Count);
                    var changeVector = session.Advanced.GetChangeVectorFor(user);
                    Assert.NotNull(await session.Advanced.Revisions.GetAsync<User>(changeVector));
                }

                using (var session = leaderStore.OpenAsyncSession(new SessionOptions {TransactionMode = TransactionMode.ClusterWide}))
                {
                    session.Delete("foo/bar/2");
                    session.Advanced.WaitForIndexesAfterSaveChanges();
                    await session.SaveChangesAsync();
                }

                // bring more nodes down, so only one node is left
                var task2 = DisposeServerAndWaitForFinishOfDisposalAsync(Servers[2]);
                var task3 = DisposeServerAndWaitForFinishOfDisposalAsync(Servers[3]);
                var task4 = DisposeServerAndWaitForFinishOfDisposalAsync(Servers[4]);
                await Task.WhenAll(task2, task3, task4);

                var result2 = task2.Result;

                using (var session = leaderStore.OpenAsyncSession())
                {
                    Assert.Equal(leader.WebUrl, session.Advanced.RequestExecutor.Url);
                    await session.StoreAsync(user1, "foo/bar");

                    await session.SaveChangesAsync();

                    var list = await session.Advanced.Revisions.GetForAsync<User>(user1.Id);
                    Assert.Equal(2, list.Count);
                }

                long lastRaftIndex;
                using (leader.ServerStore.Engine.ContextPool.AllocateOperationContext(out ClusterOperationContext ctx))
                using (ctx.OpenReadTransaction())
                {
                    lastRaftIndex = leader.ServerStore.Engine.GetLastCommitIndex(ctx);
                }

                // revive the SUT node
                var revived = Servers[1] = GetNewServer(new ServerCreationOptions
                {
                    CustomSettings = new Dictionary<string, string>
                    {
                        [RavenConfiguration.GetKey(x => x.Core.ServerUrls)] = result1.Url,
                        [RavenConfiguration.GetKey(x => x.Cluster.ElectionTimeout)] = "400"
                    },
                    RunInMemory = false,
                    DeletePrevious = false,
                    DataDirectory = result1.DataDirectory,
                    RegisterForDisposal = false
                });
                using (var revivedStore = new DocumentStore()
                {
                    Urls = new[] { revived.WebUrl },
                    Database = leaderStore.Database,
                    Conventions = new DocumentConventions
                    {
                        DisableTopologyUpdates = true
                    }
                }.Initialize())
                {
                    // let the document with the revision to replicate
                    Assert.True(WaitForDocument(revivedStore, "foo/bar"));
                    using (var session = revivedStore.OpenAsyncSession())
                    {
                        session.Advanced.MaxNumberOfRequestsPerSession = int.MaxValue;

                        var user = await session.LoadAsync<User>("foo/bar");
                        var changeVector = session.Advanced.GetChangeVectorFor(user);
                        Assert.NotNull(await session.Advanced.Revisions.GetAsync<User>(changeVector));
                        var count = await WaitForValueAsync((async () =>
                        {
                            var list = await session.Advanced.Revisions.GetForAsync<User>("foo/bar");
                            return list.Count;
                        }), 2);
                        Assert.Equal(2, count);

                        // revive another node so we should have a functional cluster now
                        Servers[2] = GetNewServer(new ServerCreationOptions
                        {
                            CustomSettings = new Dictionary<string, string>
                            {
                                [RavenConfiguration.GetKey(x => x.Core.ServerUrls)] = result2.Url,
                                [RavenConfiguration.GetKey(x => x.Cluster.ElectionTimeout)] = "400"
                            },
                            RunInMemory = false,
                            DeletePrevious = false,
                            DataDirectory = result2.DataDirectory
                        });

                        // wait for the log to apply on the SUT node
                        using (var cts = new CancellationTokenSource(TimeSpan.FromSeconds(15)))
                        {
                            await leader.ServerStore.Engine.WaitForLeaveState(RachisState.Candidate, cts.Token);
                        }
                        var database = await Servers[1].ServerStore.DatabasesLandlord.TryGetOrCreateResourceStore(leaderStore.Database);
                        await database.RachisLogIndexNotifications.WaitForIndexNotification(lastRaftIndex, TimeSpan.FromSeconds(15));

                        count = await WaitForValueAsync((async () =>
                        {
                            var list = await session.Advanced.Revisions.GetForAsync<User>("foo/bar");
                            return list.Count;
                        }), 2);
                        Assert.Equal(2, count);
                    }
                }
            }
        }

        [Fact]
        public async Task ThrowOnUnsupportedOperations()
        {
            using (var store = GetDocumentStore())
            using (var session = store.OpenAsyncSession(new SessionOptions
            {
                TransactionMode = TransactionMode.ClusterWide
            }))
            {
                session.Advanced.Attachments.Store("asd", "test", new MemoryStream(new byte[] { 1, 2, 3, 4 }));
                await Assert.ThrowsAsync<NotSupportedException>(async () => await session.SaveChangesAsync());
            }
        }

        [Fact]
        public async Task ThrowOnOptimisticConcurrency()
        {
            using (var store = GetDocumentStore())
            {
                using (var session = store.OpenAsyncSession(new SessionOptions
                {
                    TransactionMode = TransactionMode.ClusterWide
                }))
                {
                    session.Advanced.UseOptimisticConcurrency = true;
                    await Assert.ThrowsAsync<NotSupportedException>(async () => await session.SaveChangesAsync());
                }
            }
        }

        [Fact]
        public async Task ThrowOnOptimisticConcurrencyForSingleDocument()
        {
            using (var store = GetDocumentStore())
            {
                using (var session = store.OpenAsyncSession(new SessionOptions
                {
                    TransactionMode = TransactionMode.ClusterWide
                }))
                {
                    await session.StoreAsync(new User { Name = "Some Other Name" }, changeVector: string.Empty, "user/1");
                    await Assert.ThrowsAsync<NotSupportedException>(async () => await session.SaveChangesAsync());
                }
            }
        }

        [Fact]
        public async Task ThrowOnInvalidTransactionMode()
        {
            var user1 = new User()
            {
                Name = "Karmel"
            };
            using (var store = GetDocumentStore())
            {
                using (var session = store.OpenAsyncSession())
                {
                    Assert.Throws<InvalidOperationException>(() => session.Advanced.ClusterTransaction.CreateCompareExchangeValue("usernames/ayende", user1));
                    Assert.Throws<InvalidOperationException>(() => session.Advanced.ClusterTransaction.DeleteCompareExchangeValue("usernames/ayende", 0));
                    await Assert.ThrowsAsync<InvalidOperationException>(async () => await session.Advanced.ClusterTransaction.GetCompareExchangeValueAsync<User>("usernames/ayende"));
                }

                using (var session = store.OpenAsyncSession(new SessionOptions
                {
                    TransactionMode = TransactionMode.ClusterWide
                }))
                {
                    session.Advanced.ClusterTransaction.CreateCompareExchangeValue("usernames/ayende", user1);
                    session.Advanced.SetTransactionMode(TransactionMode.SingleNode);
                    await Assert.ThrowsAsync<InvalidOperationException>(async () => await session.SaveChangesAsync());
                    session.Advanced.SetTransactionMode(TransactionMode.ClusterWide);
                    await session.SaveChangesAsync();

                    var u = await session.Advanced.ClusterTransaction.GetCompareExchangeValueAsync<User>("usernames/ayende");
                    Assert.Equal(user1.Name, u.Value.Name);
                }
            }
        }

        [Fact]
        public async Task CanAddNullValueToCompareExchange()
        {
            using (var store = GetDocumentStore())
            {
                using (var session = store.OpenAsyncSession(new SessionOptions
                {
                    TransactionMode = TransactionMode.ClusterWide
                }))
                {
                    const string id = "test/user";
                    session.Advanced.ClusterTransaction.CreateCompareExchangeValue<string>(id, null);
                    await session.SaveChangesAsync();

                    var compareExchangeValue = await session.Advanced.ClusterTransaction.GetCompareExchangeValueAsync<string>(id);
                    Assert.Equal(null, compareExchangeValue.Value);
                }
            }
        }

        [Fact]
        public async Task CanGetListCompareExchange()
        {
            using (var store = GetDocumentStore())
            {
                using (var session = store.OpenAsyncSession(new SessionOptions
                {
                    TransactionMode = TransactionMode.ClusterWide
                }))
                {
                    var value = new List<string> { "1", null, "2" };
                    const string id = "test/user";
                    session.Advanced.ClusterTransaction.CreateCompareExchangeValue(id, value);
                    await session.SaveChangesAsync();

                    var compareExchangeValue = await session.Advanced.ClusterTransaction.GetCompareExchangeValueAsync<List<string>>(id);
                    Assert.True(value.SequenceEqual(compareExchangeValue.Value));
                }
            }
        }

        [Fact]
        public async Task ClusterTransactionConflict()
        {
            using (var store1 = GetDocumentStore())
            using (var store2 = GetDocumentStore())
            {
                using (var session = store1.OpenAsyncSession(new SessionOptions
                {
                    TransactionMode = TransactionMode.ClusterWide
                }))
                {
                    await session.StoreAsync(new User()
                    {
                        Name = "Karmel"
                    }, "users/1");
                    await session.SaveChangesAsync();
                }

                using (var session = store2.OpenAsyncSession(new SessionOptions
                {
                    TransactionMode = TransactionMode.ClusterWide
                }))
                {
                    await session.StoreAsync(new User()
                    {
                        Name = "Grisha"
                    }, "users/1");
                    await session.SaveChangesAsync();
                }

                await Task.WhenAll(SetupReplicationAsync(store1, store2),SetupReplicationAsync(store2, store1));

                await EnsureReplicatingAsync(store1, store2);
                await EnsureReplicatingAsync(store2, store1);

                using (var session = store1.OpenAsyncSession(new SessionOptions
                {
                    TransactionMode = TransactionMode.ClusterWide
                }))
                {
                    var u = await session.LoadAsync<User>("users/1");
                    Assert.Equal("Grisha",u.Name);
                }

                using (var session = store2.OpenAsyncSession(new SessionOptions
                {
                    TransactionMode = TransactionMode.ClusterWide
                }))
                {
                    var u = await session.LoadAsync<User>("users/1");
                    Assert.Equal("Grisha",u.Name);
                }

                var t1 = EnsureNoReplicationLoop(Server, store1.Database);
                var t2 = EnsureNoReplicationLoop(Server, store2.Database);

                await Task.WhenAll(t1, t2);
                await t1;
                await t2;
            }
        }

        [Theory]
        [InlineData("")]
        [InlineData(" ")]
        [InlineData(@"
")]
        public async Task ClusterWideTransaction_WhenStoreDocWithEmptyStringId_ShouldThrowInformativeError(string id)
        {
            var e = await Assert.ThrowsAnyAsync<RavenException>(async () =>
            {
                using var store = GetDocumentStore();
                using var session = store.OpenAsyncSession(new SessionOptions
                {
                    TransactionMode = TransactionMode.ClusterWide
                });
                
                var entity = new User {Id = id};
                await session.StoreAsync(entity);
                await session.SaveChangesAsync();
                WaitForUserToContinueTheTest(store);
            });
            Assert.True(ContainsRachisException(e));

            static bool ContainsRachisException(Exception e)
            {
                while (true)
                {
                    if (e.ToString().Contains(nameof(RachisApplyException)))
                        return true;
                    if (e is AggregateException ae) 
                        return ae.InnerExceptions.Any(ex => ContainsRachisException(ex));

                    if (e.InnerException == null) 
                        return false;
                    e = e.InnerException;
                }
            }
        }

        [Fact]
        public async Task ClusterTransactionShouldBeRedirectedFromPromotableNode()
        {
            var (nodes, leader) = await CreateRaftCluster(3, watcherCluster: true);
            var database = GetDatabaseName();
            var dbCreation = await CreateDatabaseInClusterInner(new DatabaseRecord(database), 2, leader.WebUrl, null);

            using (var storeB = new DocumentStore
            {
                Database = database,
                Urls = new[] { dbCreation.Servers[0].WebUrl }
            }.Initialize())
            {
                await StoreInRegularMode(storeB, 3);

                var result = await leader.ServerStore.SendToLeaderAsync(new DeleteDatabaseCommand(database, Guid.NewGuid().ToString())
                {
                    HardDelete = true,
                    FromNodes = new[] { dbCreation.Servers[0].ServerStore.NodeTag },
                });

                await Cluster.WaitForRaftIndexToBeAppliedInClusterAsync(result.Index, TimeSpan.FromSeconds(10));

                await WaitAndAssertForValueAsync(async () =>
                {
                    var record = await storeB.Maintenance.Server.SendAsync(new GetDatabaseRecordOperation(database));
                    return record.DeletionInProgress.Count;
                }, 0);

                var breakRepl = await BreakReplication(dbCreation.Servers[1].ServerStore, database);

                await storeB.Maintenance.Server.SendAsync(new AddDatabaseNodeOperation(database, dbCreation.Servers[0].ServerStore.NodeTag));

                await StoreInTransactionMode(storeB, 1);

                breakRepl.Mend();

                var val = await WaitForValueAsync(async () => await GetMembersCount(storeB, database), 2, 20000);
                Assert.Equal(2, val);

                await WaitAndAssertForValueAsync(async () =>
                {
                    var record = await storeB.Maintenance.Server.SendAsync(new GetDatabaseRecordOperation(database));
                    return record.DeletionInProgress.Count;
                }, 0);
            }
        }

        [Fact]
        public async Task BlockWorkingWithAtomicGuardBySession()
        {
            using (var store = GetDocumentStore())
            {
                using (var session = store.OpenAsyncSession(new SessionOptions { TransactionMode = TransactionMode.ClusterWide }))
                {
                    var doc = new User { Name = "Grisha" };
                    await session.StoreAsync(doc, "users/1");
                    await session.SaveChangesAsync();
                }

                using (var session = store.OpenAsyncSession(new SessionOptions { TransactionMode = TransactionMode.ClusterWide }))
                {
                    var ex = await Assert.ThrowsAsync<InvalidOperationException>(() => session.Advanced.ClusterTransaction.GetCompareExchangeValueAsync<string>(ClusterWideTransactionHelper.GetAtomicGuardKey("users/1")));
                    Assert.Contains($"'rvn-atomic/users/1' is an atomic guard and you cannot load it via the session", ex.Message);
                }

                using (var session = store.OpenAsyncSession(new SessionOptions { TransactionMode = TransactionMode.ClusterWide }))
                {
                    session.Advanced.ClusterTransaction.CreateCompareExchangeValue(ClusterWideTransactionHelper.GetAtomicGuardKey("users/2"), "foo");
                    var ex = await Assert.ThrowsAsync<CompareExchangeInvalidKeyException>(() => session.SaveChangesAsync());
                    Assert.Contains($"You cannot manipulate the atomic guard 'rvn-atomic/users/2' via the cluster-wide session", ex.Message);
                }
            }
        }

        private class AtomicGuard
        {
#pragma warning disable CS0649
            public string Id;
#pragma warning restore CS0649
            public bool Locked;
        }

        [Fact]
        public async Task CanModifyingAtomicGuardViaOperations()
        {
            using (var store = GetDocumentStore())
            {
                const string docId = "users/1";
                using (var session = store.OpenAsyncSession(new SessionOptions { TransactionMode = TransactionMode.ClusterWide }))
                {
                    var doc = new User { Name = "Grisha" };
                    await session.StoreAsync(doc, docId);
                    await session.SaveChangesAsync();
                }

                var compareExchangeKey = $"rvn-atomic/{docId}";
                var val = await store.Operations.SendAsync(new GetCompareExchangeValueOperation<AtomicGuard>(compareExchangeKey));
                val.Value.Locked = true;
                await store.Operations.SendAsync(new PutCompareExchangeValueOperation<AtomicGuard>(val.Key, val.Value, val.Index));

                val = await store.Operations.SendAsync(new GetCompareExchangeValueOperation<AtomicGuard>(compareExchangeKey));
                Assert.Equal(true, val.Value.Locked);
            }
        }

        private class UserByName : AbstractIndexCreationTask<User>
        {
            public UserByName()
            {
                Map = users => from user in users
                               select new
                               {
                                   user.Name
                               };
            }
        }

        class TestObj
        {
            public string Id { get; set; }
            public string Prop { get; set; }
            public string ExternalId { get; set; }
        }

        private class TestIndex : AbstractIndexCreationTask<TestObj>
        {
            public override string IndexName { get; }

            public TestIndex(string name)
            {
                IndexName = name;
                Map = testObjs => from user in testObjs
                    select new { user.Prop };
            }
        }

        private class TestCommand : TransactionOperationsMerger.MergedTransactionCommand
        {
            private readonly ManualResetEvent _manualResetEvent;
            private readonly TimeSpan _timeout;

            public TestCommand(ManualResetEvent manualResetEvent, TimeSpan timeout)
            {
                if (Debugger.IsAttached)
                    timeout *= 10;
                
                _manualResetEvent = manualResetEvent;
                _timeout = timeout;
            }

            protected override long ExecuteCmd(DocumentsOperationContext context)
            {
                _manualResetEvent.WaitOne(_timeout);
                return 1;
            }

            public override TransactionOperationsMerger.IReplayableCommandDto<TransactionOperationsMerger.MergedTransactionCommand> ToDto<TTransaction>(
                TransactionOperationContext<TTransaction> context)
            {
                throw new NotImplementedException();
            }
        }
        
        [RavenTheory(RavenTestCategory.ClusterTransactions)]
        [RavenData(true, DatabaseMode = RavenDatabaseMode.All)]
        [RavenData(false, DatabaseMode = RavenDatabaseMode.All)]
        public async Task ClusterTransaction_WhenLoadReturnEmptyAndCompareExchangeExit_ShouldStillThrowConcurrency(Options options, bool clusterTrxBefore)
        {
            const string id = "testObjs/1";
            using var store = GetDocumentStore(options);

            if (clusterTrxBefore)
            {
                using var session = store.OpenAsyncSession(new SessionOptions { TransactionMode = TransactionMode.ClusterWide });
                await session.StoreAsync(new TestObj());
                await session.SaveChangesAsync();
            }

            using var mre = new ManualResetEvent(false);
            try
            {
                var database = await GetDatabase(store.Database);
                var longCommandTask = database.TxMerger.Enqueue(new TestCommand(mre, TimeSpan.FromSeconds(15)));

                var halfProcessedTask = Task.Run(async () =>
                {
                    using var session = store.OpenAsyncSession(new SessionOptions
                    {
                        TransactionMode = TransactionMode.ClusterWide
                    });
                    {
                        await session.StoreAsync(new TestObj{Prop = "new 1"}, id);
                        await session.SaveChangesAsync();
                    }
                });
                using var session = store.OpenAsyncSession(new SessionOptions { TransactionMode = TransactionMode.ClusterWide });

                await Task.Delay(10);
                //The origin issue occured if a cluster operation was made between the compare exchange operation to database operation
                await store.ExecuteIndexAsync(new TestIndex("TestIndex"));
                await store.ExecuteIndexAsync(new TestIndex("TestIndex2"));
                
                //Load can potentially also load the atomic guard and use it to generate the document change vector
                Assert.Null(await session.LoadAsync<TestObj>(id));
                await session.StoreAsync(new TestObj{ Prop = "new 2", Id = id });
                Task shouldFailTask = session.SaveChangesAsync();
                await Assert.ThrowsAnyAsync<ClusterTransactionConcurrencyException>( () => shouldFailTask);

                mre.Set();
                await longCommandTask;
                await halfProcessedTask;
            }
            finally
            {
                mre.Set();
            }
        }
        
        [RavenTheory(RavenTestCategory.ClusterTransactions)]
        [RavenData(DatabaseMode = RavenDatabaseMode.All)]
        public async Task LoadAndIncludeClusterTrxCmpxchg_WhenCmpxchgIsFromLastClusterTransactionWithNoDatabaseCommand_ShouldGetCmpxchg(Options options)
        {
            const string cmpxchgId = "cmpxchg/0";
            const string id = "testObjs/1";

            using var store = GetDocumentStore(options);

            using (var session = store.OpenAsyncSession(new SessionOptions { TransactionMode = TransactionMode.ClusterWide, DisableAtomicDocumentWritesInClusterWideTransaction = true}))
            {
                await session.StoreAsync(new TestObj{Prop = cmpxchgId }, id);
                await session.SaveChangesAsync();
            }
            
            using (var session = store.OpenAsyncSession(new SessionOptions { TransactionMode = TransactionMode.ClusterWide, DisableAtomicDocumentWritesInClusterWideTransaction = true}))
            {
                session.Advanced.ClusterTransaction.CreateCompareExchangeValue(cmpxchgId, "some-value");
                await session.SaveChangesAsync();
            }
            
            using (var session = store.OpenAsyncSession(new SessionOptions { TransactionMode = TransactionMode.ClusterWide, DisableAtomicDocumentWritesInClusterWideTransaction = true}))
            {
                await session.LoadAsync<TestObj>(id, i => i.IncludeCompareExchangeValue(x => x.Prop));
                var numberOfRequests = session.Advanced.NumberOfRequests;
                var value = await session.Advanced.ClusterTransaction.GetCompareExchangeValueAsync<string>(cmpxchgId);
                Assert.NotNull(value);
                Assert.Equal(numberOfRequests, session.Advanced.NumberOfRequests);
            }
        }
        
        [RavenTheory(RavenTestCategory.ClusterTransactions)]
        [RavenData(DatabaseMode = RavenDatabaseMode.All)]
        public async Task QueryAndIncludeClusterTrxCmpxchg_WhenDocumentsCommandsDidntFinished_ShouldNotReturnNull(Options options)
        {
            const string id1 = "testObjs/0";
            const string id2 = "testObjs/1";
            using var store = GetDocumentStore(options);
            await store.ExecuteIndexAsync(new TestIndex("TestIndex"));


            using (var session = store.OpenAsyncSession())
            {
                await session.StoreAsync(new TestObj(), id1);
                await session.SaveChangesAsync();
            }
            Indexes.WaitForIndexing(store);
            
            using var mre = new ManualResetEvent(false);
            try
            {
                var database = await GetDatabase(store.Database);
                var testCommandTask = database.TxMerger.Enqueue(new TestCommand(mre, TimeSpan.FromSeconds(15)));
                
                var halfProcessedTask = Task.Run(async () =>
                {
                    using var session = store.OpenAsyncSession(new SessionOptions { TransactionMode = TransactionMode.ClusterWide });
                    await session.StoreAsync(new TestObj { Prop = $"new 1", Id = id2 });
                    session.Advanced.ClusterTransaction.CreateCompareExchangeValue(id1, "somevalue");
                    await session.SaveChangesAsync();
                });
                
                using var session = store.OpenAsyncSession(new SessionOptions { TransactionMode = TransactionMode.ClusterWide });
                
                await Task.Delay(10);
                //The origin issue occured if a cluster operation was made between the compare exchange operation to database operation
                await store.ExecuteIndexAsync(new TestIndex("TestIndex2"));
                await store.ExecuteIndexAsync(new TestIndex("TestIndex3"));

                _ = await session.Advanced
                    .AsyncRawQuery<TestObj>(
                        @"
declare function incl(c) {
    includes.cmpxchg($ce);
    return c;
}
from index 'TestIndex' as c
select incl(c)"
                    )
                    .AddParameter("ce", id1)
                    .ToListAsync();
                
                var numberOfRequests = session.Advanced.NumberOfRequests;
                Assert.NotNull(await session.Advanced.ClusterTransaction.GetCompareExchangeValueAsync<object>(id1));
                Assert.Equal(numberOfRequests, session.Advanced.NumberOfRequests);
                
                mre.Set();
                await testCommandTask;
                await halfProcessedTask;
            }
            finally
            {
                mre.Set();
            }
        }
        
        [RavenTheory(RavenTestCategory.ClusterTransactions)]
        [RavenData(DatabaseMode = RavenDatabaseMode.All)]
        public async Task QueryIndexAndIncludeClusterTrxCmpxchg_WhenModifiedButDocumentsCommandsDidntFinished_ShouldNotBeNull(Options options)
        {
            const string id1 = "testObjs/0";
            const string id2 = "testObjs/1";
            using var store = GetDocumentStore(options);
            await store.ExecuteIndexAsync(new TestIndex("TestIndex"));


            using (var session = store.OpenAsyncSession(new SessionOptions { TransactionMode = TransactionMode.ClusterWide, DisableAtomicDocumentWritesInClusterWideTransaction = true}))
            {
                await session.StoreAsync(new TestObj(), id1);
                session.Advanced.ClusterTransaction.CreateCompareExchangeValue(id1, "some-value");
                await session.SaveChangesAsync();
            }
            Indexes.WaitForIndexing(store);
            
            using var mre = new ManualResetEvent(false);
            try
            {
                var database = await GetDatabase(store.Database);
                var testCommandTask = database.TxMerger.Enqueue(new TestCommand(mre, TimeSpan.FromSeconds(15)));

                var halfProcessedTask = Task.Run(async () =>
                {
                    using var session = store.OpenAsyncSession(new SessionOptions { TransactionMode = TransactionMode.ClusterWide, DisableAtomicDocumentWritesInClusterWideTransaction = true });
                    var cmpxchg = await session.Advanced.ClusterTransaction.GetCompareExchangeValueAsync<string>(id1);
                    cmpxchg.Value = "changed-value";
                    await session.StoreAsync(new TestObj { Prop = $"new 1", Id = id2 });
                    await session.SaveChangesAsync();
                });
                
                using var session = store.OpenAsyncSession(new SessionOptions { TransactionMode = TransactionMode.ClusterWide, DisableAtomicDocumentWritesInClusterWideTransaction = true });
                
                await Task.Delay(10);
                //The origin issue occured if a cluster operation was made between the compare exchange operation to database operation
                await store.ExecuteIndexAsync(new TestIndex("TestIndex2"));
                await store.ExecuteIndexAsync(new TestIndex("TestIndex3"));

                _ = await session.Advanced
                    .AsyncRawQuery<TestObj>(
                        @"
declare function incl(c) {
    includes.cmpxchg($ce);
    return c;
}
from index 'TestIndex' as c
select incl(c)"
                    )
                    .AddParameter("ce", id1)
                    .ToListAsync();
                
                var numberOfRequests = session.Advanced.NumberOfRequests;
                var compareExchangeValueAsync = await session.Advanced.ClusterTransaction.GetCompareExchangeValueAsync<object>(id1);
                Assert.NotNull(compareExchangeValueAsync);
                Assert.Equal(numberOfRequests, session.Advanced.NumberOfRequests);
                
                mre.Set();
                await testCommandTask;
                await halfProcessedTask;
            }
            finally
            {
                mre.Set();
            }
        }

        [RavenTheory(RavenTestCategory.ClusterTransactions)]
        [RavenData(DatabaseMode = RavenDatabaseMode.All)]
        public async Task QueryAndIncludeClusterTrxCmpxchg_WhenModifiedButDocumentsCommandsDidntFinished_ShouldNotBeNull(Options options)
        {
            const string id1 = "testObjs/0";
            const string id2 = "testObjs/1";
            using var store = GetDocumentStore(options);

            using (var session = store.OpenAsyncSession(new SessionOptions { TransactionMode = TransactionMode.ClusterWide, DisableAtomicDocumentWritesInClusterWideTransaction = true}))
            {
                await session.StoreAsync(new TestObj(), id1);
                session.Advanced.ClusterTransaction.CreateCompareExchangeValue(id1, "some-value");
                await session.SaveChangesAsync();
            }
            
            using var mre = new ManualResetEvent(false);
            try
            {
                var database = await GetDatabase(store.Database);
                var testCommandTask = database.TxMerger.Enqueue(new TestCommand(mre, TimeSpan.FromSeconds(15)));

                var halfProcessedTask = Task.Run(async () =>
                {
                    using var session = store.OpenAsyncSession(new SessionOptions { TransactionMode = TransactionMode.ClusterWide, DisableAtomicDocumentWritesInClusterWideTransaction = true });
                    var cmpxchg = await session.Advanced.ClusterTransaction.GetCompareExchangeValueAsync<string>(id1);
                    cmpxchg.Value = "changed-value";
                    await session.StoreAsync(new TestObj { Prop = $"new 1", Id = id2 });
                    await session.SaveChangesAsync();
                });
                
                using var session = store.OpenAsyncSession(new SessionOptions { TransactionMode = TransactionMode.ClusterWide, DisableAtomicDocumentWritesInClusterWideTransaction = true });
                
                await Task.Delay(10);
                //The origin issue occured if a cluster operation was made between the compare exchange operation to database operation
                await store.ExecuteIndexAsync(new TestIndex("TestIndex"));
                await store.ExecuteIndexAsync(new TestIndex("TestIndex2"));

                _ = await session.Advanced
                    .AsyncRawQuery<TestObj>(
                        @"
declare function incl(c) {
    includes.cmpxchg($ce);
    return c;
}
from TestObjs as c
select incl(c)"
                    )
                    .AddParameter("ce", id1)
                    .ToListAsync();
                
                var numberOfRequests = session.Advanced.NumberOfRequests;
                var compareExchangeValueAsync = await session.Advanced.ClusterTransaction.GetCompareExchangeValueAsync<object>(id1);
                Assert.NotNull(compareExchangeValueAsync);
                Assert.Equal(numberOfRequests, session.Advanced.NumberOfRequests);
                
                mre.Set();
                await testCommandTask;
                await halfProcessedTask;
            }
            finally
            {
                mre.Set();
            }
        }
        
        [RavenTheory(RavenTestCategory.ClusterTransactions)]
        [RavenData(DatabaseMode = RavenDatabaseMode.All)]
        public async Task QueryIncludeCmpxchg_WhenEmpty_ShouldNotCreateAdditionalRequest(Options options)
        {
            const string notExistCmpxchgId = "not-exist";
            using var store = GetDocumentStore(options); 
            await store.ExecuteIndexAsync(new TestIndex("TestIndex"));

            using (var session = store.OpenAsyncSession(new SessionOptions { TransactionMode = TransactionMode.ClusterWide }))
            {
                await session.StoreAsync(new TestObj());
                await session.SaveChangesAsync();
            }
            Indexes.WaitForIndexing(store);
            
            using (var session = store.OpenAsyncSession(new SessionOptions { TransactionMode = TransactionMode.ClusterWide }))
            {
                _ = await session.Advanced
                    .AsyncRawQuery<TestObj>(
                        @"
declare function incl(c) {
    includes.cmpxchg($ce);
    return c;
}
from index 'TestIndex' as c
select incl(c)"
                    )
                    .AddParameter("ce", notExistCmpxchgId)
                    .ToListAsync();
                
                var numberOfRequests = session.Advanced.NumberOfRequests;
                var cmpxchg = await session.Advanced.ClusterTransaction.GetCompareExchangeValueAsync<object>(notExistCmpxchgId);
                Assert.Null(cmpxchg);
                Assert.Equal(numberOfRequests, session.Advanced.NumberOfRequests);
            }
        }
        
        [RavenTheory(RavenTestCategory.CompareExchange | RavenTestCategory.ClusterTransactions)]
        [RavenData(DatabaseMode = RavenDatabaseMode.All)]
        public void LoadIncludeCmpxchg_WhenGet_ShouldNotTriggerAdditionalRequest(Options options)
        {
            const string docId = "testObjs/0";
            const string cmpxchgId = "cmpxchg/0";

            using (var store = GetDocumentStore(options))
            {                
                using (var session = store.OpenSession())
                {
                    session.Store(new TestObj{Prop = cmpxchgId}, docId);
                    session.SaveChanges();
                    
                    var test = store.Operations.Send(
                        new PutCompareExchangeValueOperation<string>(cmpxchgId, "some content", 0));
                    Assert.Equal(true, test.Successful);
                }

                using (var session = store.OpenSession(new SessionOptions { TransactionMode = TransactionMode.ClusterWide }))
                {
                    _ = session.Load<TestObj>(docId,
                        includes => includes.IncludeCompareExchangeValue(x => x.Prop));
                    
                    Assert.Equal(1, session.Advanced.NumberOfRequests);
                    _ = session.Advanced.ClusterTransaction.GetCompareExchangeValue<string>(cmpxchgId);
                    Assert.Equal(1, session.Advanced.NumberOfRequests);
                }
            }
        }
        
        [RavenTheory(RavenTestCategory.ClusterTransactions | RavenTestCategory.ClientApi)]
        [RavenData(DatabaseMode = RavenDatabaseMode.All)]
        public async Task LoadClusterWideSession_WhenDeletedWithoutClusterWideSessionAndClusterWideTransactionInProcess_ShouldNotReturnAtomicGuard(Options options)
        {
            using var store = GetDocumentStore(options);
            using var store2 = new DocumentStore{Urls = store.Urls, Database = store.Database}.Initialize();

            const string id = "foo/bar";
            using (var session = store.OpenAsyncSession(new SessionOptions
                   {
                       TransactionMode = TransactionMode.ClusterWide
                   }))
            {
                await session.StoreAsync(new TestObj(), id);
                await session.SaveChangesAsync();
            }

            using (var session = store.OpenAsyncSession())
            {
                session.Delete(id);
                await session.SaveChangesAsync();
            }

            using var mre = new ManualResetEvent(false);
            try
            {
                var database = await GetDatabase(store.Database);
                var testCommandTask = database.TxMerger.Enqueue(new TestCommand(mre, TimeSpan.FromSeconds(15)));

                var halfProcessedTask = Task.Run(async () =>
                {
                    using var session = store.OpenAsyncSession(new SessionOptions
                    {
                        TransactionMode = TransactionMode.ClusterWide
                    });
                    {
                        Assert.Null(await session.LoadAsync<TestObj>(id));
                        await session.StoreAsync(new TestObj(), id);

                        await session.SaveChangesAsync();
                    }
                });
                using var session2 = store2.OpenAsyncSession(new SessionOptions
                {
                    TransactionMode = TransactionMode.ClusterWide
                });

                await Task.Delay(10);
                //The origin issue occured if a cluster operation was made between the compare exchange operation to database operation
                await store.ExecuteIndexAsync(new TestIndex("TestIndex1"));
                await store.ExecuteIndexAsync(new TestIndex("TestIndex2"));

                Assert.Null(await session2.LoadAsync<TestObj>(id));
                await session2.StoreAsync(new TestObj(), id);
                var shouldFailTask = session2.SaveChangesAsync();
                await Assert.ThrowsAnyAsync<ClusterTransactionConcurrencyException>(() => shouldFailTask);

                mre.Set();
                await testCommandTask;
                await halfProcessedTask;
            }
            finally
            {
                mre.Set();
            }
        }

        [RavenTheory(RavenTestCategory.ClusterTransactions)]
        [RavenData(true, DatabaseMode = RavenDatabaseMode.All)]
        [RavenData(false, DatabaseMode = RavenDatabaseMode.All)]
        public async Task DatabaseRequest_WhenHasLastClusterTransaction_ShouldWaitForIndex(Options options, bool withClusterTrxBefore)
        {
            var cmpXchgKey = "cmpXchgKey";

            var server = GetNewServer();
            options.Server = server;
            using var store = GetDocumentStore(options);
            using var store2 = new DocumentStore{Urls = store.Urls, Database = store.Database}.Initialize();

            if (withClusterTrxBefore)
            {
                using var session = store2.OpenAsyncSession(new SessionOptions { TransactionMode = TransactionMode.ClusterWide });
                await session.StoreAsync(new TestObj());
                await session.SaveChangesAsync();
            }
            
            using  (var session = store2.OpenAsyncSession(new SessionOptions { TransactionMode = TransactionMode.ClusterWide }))
            {
                session.Advanced.ClusterTransaction.CreateCompareExchangeValue(cmpXchgKey, "some-value");
                await session.SaveChangesAsync();
            }

            var database = await GetDatabase(server, store.Database);
            var index = database.ClusterWideTransactionIndexWaiter.LastIndex + 5;
            
            store.SetLastTransactionIndex(store.Database, index);

            var tcs = new CancellationTokenSource();
            var shouldBeCompletedWhenReachIndexTask = Task.Run(async () =>
            {
                using var session = store.OpenAsyncSession();
                await session.LoadAsync<TestObj>("testObjs/0", tcs.Token);
            });
            
            while(true)
            {
                using var session = store2.OpenAsyncSession(new SessionOptions{TransactionMode = TransactionMode.ClusterWide});
                
                var cmpXchg = await session.Advanced.ClusterTransaction.GetCompareExchangeValueAsync<string>(cmpXchgKey);
                cmpXchg.Value = Guid.NewGuid().ToString();
                await session.SaveChangesAsync();
                if(cmpXchg?.Index >= index)
                    break;
                await Task.Delay(100);
                Assert.False(shouldBeCompletedWhenReachIndexTask.IsCompleted);
            }

            tcs.CancelAfter(TimeSpan.FromSeconds(10));
            await shouldBeCompletedWhenReachIndexTask;
        }

        [RavenTheory(RavenTestCategory.ClusterTransactions)]
        [RavenData(true, DatabaseMode = RavenDatabaseMode.All)]
        [RavenData(false, DatabaseMode = RavenDatabaseMode.All)]
        public async Task DatabaseRequest_WhenLastClusterTrxWasDeleteCmpxchg_ShouldNotHang(Options options, bool withClusterTrxBefore)
        {
            var cmpXchgKey = "cmpXchgKey";

            var server = GetNewServer(new ServerCreationOptions
            {
                CustomSettings = new Dictionary<string, string>()
                {
                    [RavenConfiguration.GetKey(x => x.Cluster.SupervisorSamplePeriod)] = "50",
                    [RavenConfiguration.GetKey(x => x.Cluster.StabilizationTime)] = "1",
                }
            });

            server.Configuration.Cluster.CompareExchangeTombstonesCleanupInterval = new TimeSetting(100, TimeUnit.Milliseconds);
            
            using var store = GetDocumentStore(new Options{Server = server});
            using var store2 = new DocumentStore
            {
                Database = store.Database,
                Urls = store.Urls
            }.Initialize();

            if (withClusterTrxBefore)
            {
                using var session = store.OpenAsyncSession(new SessionOptions { TransactionMode = TransactionMode.ClusterWide });
                await session.StoreAsync(new TestObj(), "TestObjs/0");
                await session.SaveChangesAsync();
            }
            
            using (var session = store.OpenAsyncSession(new SessionOptions { TransactionMode = TransactionMode.ClusterWide }))
            {
                session.Advanced.ClusterTransaction.CreateCompareExchangeValue(cmpXchgKey, "some-value");
                await session.SaveChangesAsync();
            }

            var mre = new ManualResetEvent(false);
            try
            {
                var database = await GetDatabase(server, store.Database);
                var testCommandTask = database.TxMerger.Enqueue(new TestCommand(mre, TimeSpan.FromSeconds(15)));

                var executeClusterTransactionHangTask = Task.Run(async () =>
                {
                    //This task should hang the DocumentDatabase.ExecuteClusterTransaction
                    using (var session = store2.OpenAsyncSession(new SessionOptions { TransactionMode = TransactionMode.ClusterWide }))
                    {
                        await session.StoreAsync(new TestObj(), "TestObjs/1");
                        await session.SaveChangesAsync();
                    }
                });

                await Task.Delay(1);
                await store.ExecuteIndexAsync(new TestIndex("TestIndex1"));
                await store.ExecuteIndexAsync(new TestIndex("TestIndex2"));
                
                using  (var session = store.OpenAsyncSession(new SessionOptions { TransactionMode = TransactionMode.ClusterWide }))
                {
                    var cmpXchg = await session.Advanced.ClusterTransaction.GetCompareExchangeValueAsync<string>(cmpXchgKey);
                    session.Advanced.ClusterTransaction.DeleteCompareExchangeValue(cmpXchg);
                    await session.SaveChangesAsync();
                }
            
                await Task.Delay(1000);
                
                mre.Set();
                await testCommandTask;
                await executeClusterTransactionHangTask;
            }
            finally
            {
                mre.Set();
            }
            
            using  (var session = store.OpenAsyncSession(new SessionOptions { TransactionMode = TransactionMode.ClusterWide }))
            {
                var tokenSource = new CancellationTokenSource();
                tokenSource.CancelAfter(TimeSpan.FromSeconds(15));
                
                //For the test it can be any database request and should not hang
                await session.Advanced.ClusterTransaction.GetCompareExchangeValueAsync<string>(cmpXchgKey, tokenSource.Token);
            }
        }
        
        [RavenTheory(RavenTestCategory.ClusterTransactions | RavenTestCategory.ClientApi)]
        [RavenData(DatabaseMode = RavenDatabaseMode.All)]
        public async Task DatabaseRequest_WhenWaitForIndexAndClusterTransactionHasError_ShouldThrow(Options options)
        {
            const string id = "testObjs/0";
            
            using var store = GetDocumentStore(options);
            var database = await GetDatabase(store.Database);

            store.SetLastTransactionIndex(store.Database, long.MaxValue);
            
            using var session = store.OpenAsyncSession();
            var t = session.LoadAsync<TestObj>(id);
            database.ClusterWideTransactionIndexWaiter.NotifyListenersAboutError(new TestException());
            var exception = await Assert.ThrowsAnyAsync<RavenException>(() => t);
            const string expected = "System.AggregateException: One or more errors occurred. (Exception of type 'SlowTests.Cluster.ClusterTransactionTests+TestException' was thrown.)";
            Assert.StartsWith(expected, exception.Message);
        }

        private class TestException : Exception 
        {
            
        }
        
    }
}
