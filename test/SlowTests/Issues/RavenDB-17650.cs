using System;
using System.Collections.Generic;
using System.Threading;
using System.Threading.Tasks;
using Raven.Client.Documents.Subscriptions;
using Raven.Client.Exceptions;
using Raven.Client.Exceptions.Database;
using Raven.Client.Exceptions.Documents.Subscriptions;
using Raven.Client.ServerWide.Operations;
using Raven.Server.Config;
using Tests.Infrastructure;
using Xunit;
using Xunit.Abstractions;

namespace SlowTests.Issues
{
    public class RavenDB_17650 : ClusterTestBase
    {
        public RavenDB_17650(ITestOutputHelper output) : base(output)
        {
        }

        [RavenFact(RavenTestCategory.Subscriptions)]
        public async Task Should_Retry_When_DatabaseDisabledException_Was_Thrown()
        {
            using var store = GetDocumentStore(new Options()
            {
                ReplicationFactor = 1,
                RunInMemory = false
            });

            string id = "User/33-A";
            using (var session = store.OpenAsyncSession())
            {
                await session.StoreAsync(new User { Id = id, Name = "1" });
                await session.StoreAsync(new User { Name = "2" });
                await session.SaveChangesAsync();
            }

            await store.Subscriptions
                .CreateAsync(new SubscriptionCreationOptions<User>
                {
                    Name = "BackgroundSubscriptionWorker"
                });

            using var worker = store.Subscriptions.GetSubscriptionWorker<User>(new SubscriptionWorkerOptions("BackgroundSubscriptionWorker"));

            // disable database
            var disableSucceeded = store.Maintenance.Server.Send(new ToggleDatabasesStateOperation(store.Database, true));
            Assert.True(disableSucceeded.Success);
            Assert.True(disableSucceeded.Disabled);

            var cts = new CancellationTokenSource();
            var failMre = new ManualResetEvent(false);
            worker.OnSubscriptionConnectionRetry += e =>
            {
                if (e is DatabaseDisabledException)
                {
                    failMre.Set();
                }
            };
            var successMre = new ManualResetEvent(false);
            var _ = worker.Run(batch =>
            {
                successMre.Set();
            }, cts.Token);

            //enable database
            Assert.True(failMre.WaitOne(TimeSpan.FromSeconds(15)), "Subscription didn't fail as expected.");
            var enableSucceeded = store.Maintenance.Server.Send(new ToggleDatabasesStateOperation(store.Database, false));
            Assert.False(enableSucceeded.Disabled);
            Assert.True(enableSucceeded.Success);
            Assert.True(successMre.WaitOne(TimeSpan.FromSeconds(15)), "Subscription didn't success as expected.");
        }

        [RavenFact(RavenTestCategory.Subscriptions)]
        public async Task Should_Retry_When_AllTopologyNodesDownException_Was_Thrown()
        {
            var (nodes, leader) = await CreateRaftCluster(numberOfNodes: 2, shouldRunInMemory: false);
            using var store = GetDocumentStore(new Options()
            {
                ReplicationFactor = 2,
                RunInMemory = false,
                Server = leader
            });
            string id = "User/33-A";
            using (var session = store.OpenAsyncSession())
            {
                await session.StoreAsync(new User { Id = id, Name = "1" });
                await session.StoreAsync(new User { Name = "2" });
                await session.SaveChangesAsync();
            }

            await store.Subscriptions
                .CreateAsync(new SubscriptionCreationOptions<User>
                {
                    Name = "BackgroundSubscriptionWorker"
                });

            using var worker = store.Subscriptions.GetSubscriptionWorker<User>(new SubscriptionWorkerOptions("BackgroundSubscriptionWorker"));

            // dispose nodes
            var result0 = await DisposeServerAndWaitForFinishOfDisposalAsync(nodes[0]);
            var result1 = await DisposeServerAndWaitForFinishOfDisposalAsync(nodes[1]);

            var cts = new CancellationTokenSource();
            var failMre = new ManualResetEvent(false);
            worker.OnSubscriptionConnectionRetry += e =>
            {
                if (e is AllTopologyNodesDownException)
                {
                    failMre.Set();
                }
            };
            var successMre = new ManualResetEvent(false);
            var _ = worker.Run(batch =>
            {
                successMre.Set();
            }, cts.Token);

            //revive node
            Assert.True(failMre.WaitOne(TimeSpan.FromSeconds(15)), "Subscription didn't fail as expected.");
            ReviveNode(result0.DataDirectory, result0.Url);
            ReviveNode(result1.DataDirectory, result1.Url);
            Assert.True(successMre.WaitOne(TimeSpan.FromSeconds(15)), "Subscription didn't success as expected.");
        }

        private void ReviveNode(string nodeDataDirectory, string nodeUrl)
        {
            var cs = new Dictionary<string, string>(DefaultClusterSettings);
            cs[RavenConfiguration.GetKey(x => x.Core.ServerUrls)] = nodeUrl;
            var revivedServer = GetNewServer(new ServerCreationOptions
            {
                DeletePrevious = false,
                RunInMemory = false,
                DataDirectory = nodeDataDirectory,
                CustomSettings = cs
            });
        }

        [RavenFact(RavenTestCategory.Subscriptions)]
        public async Task Should_Throw_DatabaseDisabledException_When_MaxErroneousPeriod_Was_Passed()
        {
            using var store = GetDocumentStore(new Options()
            {
                ReplicationFactor = 1,
                RunInMemory = false
            });

            string id = "User/33-A";
            using (var session = store.OpenAsyncSession())
            {
                await session.StoreAsync(new User { Id = id, Name = "1" });
                await session.StoreAsync(new User { Name = "2" });
                await session.SaveChangesAsync();
            }

            await store.Subscriptions
                .CreateAsync(new SubscriptionCreationOptions<User>
                {
                    Name = "BackgroundSubscriptionWorker"
                });

            using var worker = store.Subscriptions.GetSubscriptionWorker<User>(new SubscriptionWorkerOptions("BackgroundSubscriptionWorker")
            {
                MaxErroneousPeriod = TimeSpan.Zero
            });

            // disable database
            var disableSucceeded = store.Maintenance.Server.Send(new ToggleDatabasesStateOperation(store.Database, true));
            Assert.True(disableSucceeded.Success);
            Assert.True(disableSucceeded.Disabled);

            var cts = new CancellationTokenSource();

            var aggregateException = await Assert.ThrowsAsync<AggregateException>(() => worker.Run(batch => { }, cts.Token));
            var actualExceptionWasThrown = false;
            var subscriptionInvalidStateExceptionWasThrown = false;
            foreach (var e in aggregateException.InnerExceptions)
            {
                if (e is SubscriptionInvalidStateException)
                {
                    subscriptionInvalidStateExceptionWasThrown = true;
                }
                if (e is DatabaseDisabledException)
                {
                    actualExceptionWasThrown = true;
                }

                if (subscriptionInvalidStateExceptionWasThrown && actualExceptionWasThrown)
                    break;
            }
            Assert.True(subscriptionInvalidStateExceptionWasThrown && actualExceptionWasThrown);
        }

        [RavenFact(RavenTestCategory.Subscriptions)]
        public async Task Should_Throw_AllTopologyNodesDownException_When_MaxErroneousPeriod_Was_Passed()
        {
            var (nodes, leader) = await CreateRaftCluster(numberOfNodes: 2, shouldRunInMemory: false);
            using var store = GetDocumentStore(new Options()
            {
                ReplicationFactor = 2,
                RunInMemory = false,
                Server = leader
            });
            string id = "User/33-A";
            using (var session = store.OpenAsyncSession())
            {
                await session.StoreAsync(new User { Id = id, Name = "1" });
                await session.StoreAsync(new User { Name = "2" });
                await session.SaveChangesAsync();
            }

            await store.Subscriptions
                .CreateAsync(new SubscriptionCreationOptions<User>
                {
                    Name = "BackgroundSubscriptionWorker"
                });

            using var worker = store.Subscriptions.GetSubscriptionWorker<User>(new SubscriptionWorkerOptions("BackgroundSubscriptionWorker")
            {
                MaxErroneousPeriod = TimeSpan.Zero
            });

            // dispose nodes
            var result0 = await DisposeServerAndWaitForFinishOfDisposalAsync(nodes[0]);
            var result1 = await DisposeServerAndWaitForFinishOfDisposalAsync(nodes[1]);

            var cts = new CancellationTokenSource();

            var aggregateException = await Assert.ThrowsAsync<AggregateException>(() => worker.Run(batch => { }, cts.Token));
            var actualExceptionWasThrown = false;
            var subscriptionInvalidStateExceptionWasThrown = false;
            foreach (var e in aggregateException.InnerExceptions)
            {
                if (e is SubscriptionInvalidStateException)
                {
                    subscriptionInvalidStateExceptionWasThrown = true;
                }
                if (e is AllTopologyNodesDownException)
                {
                    actualExceptionWasThrown = true;
                }

                if (subscriptionInvalidStateExceptionWasThrown && actualExceptionWasThrown)
                    break;
            }
            Assert.True(subscriptionInvalidStateExceptionWasThrown && actualExceptionWasThrown);
        }

        private class User
        {
            public string Id { get; set; }
            public string Name { get; set; }

        }
    }
}
