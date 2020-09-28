using System;
using System.Collections.Generic;
using System.IO;
using System.Linq;
using System.Runtime.CompilerServices;
using System.Threading;
using System.Threading.Tasks;
using FastTests.Server.Replication;
using FastTests.Utils;
using Raven.Client;
using Raven.Client.Documents;
using Raven.Client.Documents.Conventions;
using Raven.Client.Documents.Indexes;
using Raven.Client.Documents.Operations.CompareExchange;
using Raven.Client.Documents.Operations.Revisions;
using Raven.Client.Documents.Session;
using Raven.Client.Documents.Smuggler;
using Raven.Client.Exceptions;
using Raven.Client.ServerWide;
using Raven.Server;
using Raven.Server.Config;
using Raven.Server.Documents;
using Raven.Server.Documents.Replication;
using Raven.Server.Rachis;
using Raven.Server.ServerWide.Commands;
using Raven.Server.ServerWide.Context;
using Raven.Server.Utils;
using Raven.Tests.Core.Utils.Entities;
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
            if (options == null)
            {
                options = new ServerCreationOptions();
            }

            if (options.CustomSettings == null)
                options.CustomSettings = new Dictionary<string, string>();

            options.CustomSettings[RavenConfiguration.GetKey(x => x.Cluster.OperationTimeout)] = "60";
            options.CustomSettings[RavenConfiguration.GetKey(x => x.Cluster.StabilizationTime)] = "10";
            options.CustomSettings[RavenConfiguration.GetKey(x => x.Cluster.TcpConnectionTimeout)] = "30000";

            return base.GetNewServer(options, caller);
        }

        [Fact]
        public async Task CanCreateClusterTransactionRequest()
        {
            var leader = await CreateRaftClusterAndGetLeader(3);
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

        [Fact]
        public async Task CanCreateClusterTransactionRequest2()
        {
            DebuggerAttachedTimeout.DisableLongTimespan = true;
            var leader = await CreateRaftClusterAndGetLeader(2);
            using (var leaderStore = GetDocumentStore(new Options
            {
                Server = leader,
                ReplicationFactor = 2
            }))
            {
                var count = 0;
                var parallelism = Environment.ProcessorCount * 5;

                for (var i = 0; i < 10; i++)
                {
                    var tasks = new List<Task>();
                    for (var j = 0; j < parallelism; j++)
                    {
                        tasks.Add(Task.Run(async () =>
                        {
                            using (var session = leaderStore.OpenSession(new SessionOptions
                            {
                                TransactionMode = TransactionMode.ClusterWide
                            }))
                            {
                                session.Advanced.ClusterTransaction.CreateCompareExchangeValue($"usernames/{Interlocked.Increment(ref count)}", new User());
                                session.SaveChanges();
                            }

                            await ActionWithLeader((l) =>
                              {
                                  l.ServerStore.Engine.CurrentLeader?.StepDown();
                                  return Task.CompletedTask;
                              });
                        }));
                    }

                    await Task.WhenAll(tasks.ToArray());
                    using (var session = leaderStore.OpenSession(new SessionOptions
                    {
                        TransactionMode = TransactionMode.ClusterWide
                    }))
                    {
                        var results = session.Advanced.ClusterTransaction.GetCompareExchangeValues<User>(
                            Enumerable.Range(i * parallelism, parallelism).Select(x =>
                                $"usernames/{Interlocked.Increment(ref count)}").ToArray<string>());
                        Assert.Equal(parallelism, results.Count);
                    }
                }
            }
        }

        [Fact]
        public async Task ServeSeveralClusterTransactionRequests()
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
            var leader = await CreateRaftClusterAndGetLeader(numberOfNodes);
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
                                    await ActionWithLeader(l =>
                                    {
                                        l.ServerStore.Engine.CurrentLeader?.StepDown();
                                        return Task.CompletedTask;
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
            var leader = await CreateRaftClusterAndGetLeader(3);
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

            var leader = await CreateRaftClusterAndGetLeader(3);
            var user1 = new User()
            {
                Name = "Karmel"
            };
            var user2 = new User()
            {
                Name = "Oren"
            };
            var user3 = new User()
            {
                Name = "Indych"
            };

            using (var store = GetDocumentStore(new Options { Server = leader, ReplicationFactor = 2 }))
            {
                // we kill one server so we would not clean the pending cluster transactions.
                await DisposeAndRemoveServer(Servers.First(s => s != leader));

                using (var session = store.OpenAsyncSession(new SessionOptions
                {
                    TransactionMode = TransactionMode.ClusterWide
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

            using (var store = GetDocumentStore(new Options { Server = leader, ReplicationFactor = 2 }))
            {
                using (var session = store.OpenAsyncSession())
                {
                    await session.StoreAsync(user1, "foo/bar");
                    await session.SaveChangesAsync();
                    session.Advanced.Evict(user1);

                    var operation = await store.Smuggler.ImportAsync(new DatabaseSmugglerImportOptions(), file);
                    await operation.WaitForCompletionAsync(TimeSpan.FromMinutes(1));
                    var user = await session.LoadAsync<User>("foo/bar");
                    session.Advanced.Evict(user);
                    Assert.Equal(user3.Name, user.Name);
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
                TransactionMode = TransactionMode.ClusterWide
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
        public async Task ResolveInFavorOfLocalClusterTransaction()
        {
            var user1 = new User()
            {
                Name = "Source"
            };
            var user2 = new User()
            {
                Name = "Dest"
            };
            using (var store1 = GetDocumentStore())
            using (var store2 = GetDocumentStore())
            {
                using (var session = store2.OpenAsyncSession())
                {
                    session.Advanced.SetTransactionMode(TransactionMode.ClusterWide);
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

                // 1. at first we will resolve to the local, since both are form cluster transaction
                var resolvedToLocal = await WaitForValueAsync(async () =>
                {
                    using (var session = store2.OpenAsyncSession())
                    {
                        var user = await session.LoadAsync<User>("users/1");
                        if (user == null)
                            return false;

                        if (user.Name != "Dest")
                            return false;
                        var changeVector = session.Advanced.GetChangeVectorFor(user);
                        var entries = changeVector.ToChangeVector();
                        return entries.Length == 2;
                    }
                }, true);
                Assert.True(resolvedToLocal);

                // 2. after the resolution the document is stripped from the cluster transaction flag
                using (var session = store1.OpenAsyncSession(new SessionOptions
                {
                    TransactionMode = TransactionMode.ClusterWide
                }))
                {
                    user1.Name = "Source 2";
                    await session.StoreAsync(user1, "users/1");
                    await session.SaveChangesAsync();
                }

                // 3. so in the next conflict we will be overwriting it.
                Assert.True(WaitForDocument<User>(store2, "users/1", (u) => u.Name == "Source 2"));
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
                    TransactionMode = TransactionMode.ClusterWide
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
                    TransactionMode = TransactionMode.ClusterWide
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
                    TransactionMode = TransactionMode.ClusterWide
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
            var mre1 = new ManualResetEvent(false);
            var mre2 = new ManualResetEvent(false);
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
                        mre2.WaitOne();
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
                        mre1.WaitOne();
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
            var leader = await CreateRaftClusterAndGetLeader(3);

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
                    session.TimeSeriesFor("users/1-A", "Heartrate").Append(DateTime.Today, new[] { 55d }, "watches/apple");
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
                    TransactionMode = TransactionMode.ClusterWide
                }))
                {
                    await session.StoreAsync(new User { Name = "Aviv1" }, "users/1");
                    await session.SaveChangesAsync();
                }

                using (var session = store.OpenAsyncSession(new SessionOptions
                {
                    TransactionMode = TransactionMode.ClusterWide
                }))
                {
                    await session.StoreAsync(new User { Name = "Aviv2" }, "users/1");
                    await session.SaveChangesAsync();

                    var list = await session.Advanced.Revisions.GetForAsync<User>("users/1");
                    Assert.Equal(2, list.Count);
                }

                using (var session = store.OpenAsyncSession(new SessionOptions
                {
                    TransactionMode = TransactionMode.ClusterWide
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
                    TransactionMode = TransactionMode.ClusterWide
                }))
                {
                    await session.StoreAsync(new User { Name = "Aviv1" }, "users/1");
                    await session.SaveChangesAsync();
                }

                using (var session = store.OpenAsyncSession(new SessionOptions
                {
                    TransactionMode = TransactionMode.ClusterWide
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
                    TransactionMode = TransactionMode.ClusterWide
                }))
                {
                    await session.StoreAsync(new User { Name = "Aviv1" }, "users/1");
                    await session.SaveChangesAsync();
                }

                using (var session = store.OpenAsyncSession(new SessionOptions
                {
                    TransactionMode = TransactionMode.ClusterWide
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
            var leader = await CreateRaftClusterAndGetLeader(5, shouldRunInMemory: false, leaderIndex: 0);
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
                await WaitForRaftIndexToBeAppliedInCluster(index, TimeSpan.FromSeconds(15));

                // bring our SUT node down, but we still have a cluster and can execute cluster transaction.
                var server = Servers[1];
                var result1 = await DisposeServerAndWaitForFinishOfDisposalAsync(server);

                using (var session = leaderStore.OpenAsyncSession(new SessionOptions
                {
                    TransactionMode = TransactionMode.ClusterWide
                }))
                {
                    Assert.Equal(1, session.Advanced.RequestExecutor.TopologyNodes.Count);
                    Assert.Equal(leader.WebUrl, session.Advanced.RequestExecutor.Url);
                    session.Advanced.ClusterTransaction.CreateCompareExchangeValue("usernames/ayende", user1);
                    await session.StoreAsync(user3, "foo/bar");
                    await session.StoreAsync(user3, "foo/bar/2");
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
    }
}
