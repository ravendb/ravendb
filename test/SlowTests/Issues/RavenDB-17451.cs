using System;
using System.Collections.Concurrent;
using System.Collections.Generic;
using System.Linq;
using System.Threading.Tasks;
using FastTests.Server.Replication;
using Raven.Client.Documents.Subscriptions;
using Raven.Server.Config;
using Raven.Tests.Core.Utils.Entities;
using SlowTests.Core.AdminConsole;
using Xunit;
using Xunit.Abstractions;

namespace SlowTests.Issues
{
    public class RavenDB_17451 : ReplicationTestBase
    {
        public RavenDB_17451(ITestOutputHelper output) : base(output)
        {
        }

        [Theory]
        [InlineData(true)]
        [InlineData(false)]
        public async Task CanDisableTcpCompressionViaConfiguration_ReplicationTest(bool disableOnSrc)
        {
            var srcServer = GetNewServer(new ServerCreationOptions
            {
                RunInMemory = false
            });
            var dstServer = GetNewServer(new ServerCreationOptions
            {
                RunInMemory = false
            });
            using (var srcStore = GetDocumentStore(new Options
            {
                Server = srcServer
            }))
            using (var dstStore1 = GetDocumentStore(new Options
            {
                Server = dstServer
            }))
            using (var dstStore2 = GetDocumentStore(new Options
            {
                Server = dstServer
            }))
            {
                const string docId = "users/1";
                using (var session = srcStore.OpenSession())
                {
                    session.Store(new User { Name = "ayende" }, docId);
                    session.SaveChanges();
                }

                await SetupReplicationAsync(srcStore, dstStore1);

                Assert.True(WaitForDocument<User>(dstStore1, docId, u => u.Name == "ayende"));

                var srcDb = await srcServer.ServerStore.DatabasesLandlord.TryGetOrCreateResourceStore(srcStore.Database);

                var tcpConnections = srcDb.RunningTcpConnections.ToList();

                Assert.Equal(1, tcpConnections.Count);

                var repToDst1TcpInfo = tcpConnections[0];

                // assert compressed tcp connection to dst1
                Assert.True(repToDst1TcpInfo.Stream is Sparrow.Utils.ReadWriteCompressedStream);

                var serverToDisableOn = disableOnSrc ? srcServer : dstServer;
                var configuration = serverToDisableOn.Configuration;
                Assert.False(configuration.Server.DisableTcpCompression);

                // modify configuration
                AdminJsConsoleTests.ExecuteScript(serverToDisableOn, database: null, "server.Configuration.Server.DisableTcpCompression = true;");
                Assert.True(configuration.Server.DisableTcpCompression);

                await SetupReplicationAsync(srcStore, dstStore2);

                Assert.True(WaitForDocument<User>(dstStore2, docId, u => u.Name == "ayende"));

                tcpConnections = srcDb.RunningTcpConnections.ToList();
                Assert.Equal(2, tcpConnections.Count);

                var repToDst2TcpInfo = tcpConnections.Single(i => i.Id > repToDst1TcpInfo.Id);

                // assert non-compressed tcp connection to dst2
                Assert.False(repToDst2TcpInfo.Stream is Sparrow.Utils.ReadWriteCompressedStream);
            }
        }

        [Fact]
        public async Task CanDisableTcpCompressionViaConfiguration_SubscriptionsTest()
        {
            var server = GetNewServer(new ServerCreationOptions
            {
                RunInMemory = false,
                CustomSettings = new Dictionary<string, string>()
                {
                    [RavenConfiguration.GetKey(x => x.Server.DisableTcpCompression)] = "true"
                }
            });

            Assert.True(server.Configuration.Server.DisableTcpCompression);

            using (var store = GetDocumentStore(new Options
            {
                Server = server
            }))
            {
                using (var session = store.OpenSession())
                {
                    for (int i = 0; i < 10; i++)
                    {
                        session.Store(new User());
                    }

                    session.SaveChanges();
                }

                var subscriptionCreationParams = new SubscriptionCreationOptions
                {
                    Query = "from Users"
                };
                var subsId = await store.Subscriptions.CreateAsync(subscriptionCreationParams);
                using (var subscription = store.Subscriptions.GetSubscriptionWorker<User>(new SubscriptionWorkerOptions(subsId)
                {
                    TimeToWaitBeforeConnectionRetry = TimeSpan.FromSeconds(5)
                }))
                {
                    var list = new BlockingCollection<User>();
                    GC.KeepAlive(subscription.Run(u =>
                    {
                        foreach (var item in u.Items)
                        {
                            list.Add(item.Result);
                        }
                    }));
                    User user;
                    for (var i = 0; i < 10; i++)
                    {
                        Assert.True(list.TryTake(out user, 1000));
                    }

                    Assert.False(list.TryTake(out user, 50));

                    var db = await server.ServerStore.DatabasesLandlord.TryGetOrCreateResourceStore(store.Database);

                    var tcpConnections = db.RunningTcpConnections.ToList();

                    Assert.Equal(1, tcpConnections.Count);

                    // assert non-compressed tcp connection
                    Assert.False(tcpConnections[0].Stream is Sparrow.Utils.ReadWriteCompressedStream);
                }
            }
        }


        [Fact]
        public async Task CanDisableTcpCompressionOnTheClientViaStoreConventions()
        {
            var server = GetNewServer(new ServerCreationOptions
            {
                RunInMemory = false
            });

            using (var store1 = GetDocumentStore(new Options
            {
                Server = server
            }))
            using (var store2 = GetDocumentStore(new Options
            {
                Server = server,
                ModifyDocumentStore = s => s.Conventions.DisableTcpCompression = true
            }))
            {
                Assert.False(server.Configuration.Server.DisableTcpCompression);

                foreach (var store in new [] { store1, store2 })
                {
                    var compressionDisabled = store == store2;

                    using (var session = store.OpenSession())
                    {
                        for (int i = 0; i < 10; i++)
                        {
                            session.Store(new User());
                        }

                        session.SaveChanges();
                    }

                    var subscriptionCreationParams = new SubscriptionCreationOptions()
                    {
                        Query = "from Users"
                    };
                    var subsId = await store.Subscriptions.CreateAsync(subscriptionCreationParams);
                    using (var subscription = store.Subscriptions.GetSubscriptionWorker<User>(new SubscriptionWorkerOptions(subsId)
                    {
                        TimeToWaitBeforeConnectionRetry = TimeSpan.FromSeconds(5)
                    }))
                    {
                        var list = new BlockingCollection<User>();
                        GC.KeepAlive(subscription.Run(u =>
                        {
                            foreach (var item in u.Items)
                            {
                                list.Add(item.Result);
                            }
                        }));
                        User user;
                        for (var i = 0; i < 10; i++)
                        {
                            Assert.True(list.TryTake(out user, 1000));
                        }
                        Assert.False(list.TryTake(out user, 50));

                        var db = await server.ServerStore.DatabasesLandlord.TryGetOrCreateResourceStore(store.Database);
                        var tcpConnections = db.RunningTcpConnections.ToList();
                        Assert.Equal(1, tcpConnections.Count);

                        if (compressionDisabled)
                            Assert.False(tcpConnections[0].Stream is Sparrow.Utils.ReadWriteCompressedStream);
                        else
                            Assert.True(tcpConnections[0].Stream is Sparrow.Utils.ReadWriteCompressedStream);
                    }
                }
            }
        }
    }
}
