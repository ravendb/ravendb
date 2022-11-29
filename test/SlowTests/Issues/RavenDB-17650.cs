using System;
using System.Collections.Generic;
using System.Linq;
using System.Text;
using System.Threading;
using System.Threading.Tasks;
using Amazon.S3.Model;
using FastTests;
using Microsoft.Azure.Documents.Spatial;
using Raven.Client.Documents.Subscriptions;
using Raven.Client.ServerWide.Operations;
using Raven.Server.Config;
using SlowTests.MailingList;
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

        [Fact]
        public async Task Should_Retry_When_DatabaseDisabledException_Was_Thrown()
        {
            var (nodes, leader) = await CreateRaftCluster(numberOfNodes: 3, shouldRunInMemory: false/*, watcherCluster: true*/);
            using var store = GetDocumentStore(new Options()
            {
                ReplicationFactor = 3,
                Server = leader,
                RunInMemory = false
            });
            string id = "User/33-A";
            using (var session = store.OpenAsyncSession())
            {
                await session.StoreAsync(new User { Id = id, Name = "1" });
                await session.StoreAsync(new User { Name = "2" });
                await session.SaveChangesAsync();
            }
            WaitForUserToContinueTheTest(store);

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
            var mre = new ManualResetEvent(false);
            var t = worker.Run(async batch =>
            {
                User user = null;
                using (var session = batch.OpenAsyncSession())
                {
                    user = await session.LoadAsync<User>(id);
                }
                
                if (user != null)
                {
                    mre.Set();
                }
            }, cts.Token);

            //enable database
            await Task.Delay(2000);
            var enableSucceeded = store.Maintenance.Server.Send(new ToggleDatabasesStateOperation(store.Database, false));
            Assert.False(enableSucceeded.Disabled);
            Assert.True(enableSucceeded.Success);
            WaitForUserToContinueTheTest(store);
            Assert.True(mre.WaitOne(TimeSpan.FromSeconds(15)), "User didn't loaded.");
        }

        [Fact]
        public async Task Should_Retry_When_AllNodesTopologyDownException_Was_Thrown()
        {
            var (nodes, leader) = await CreateRaftCluster(numberOfNodes: 3, shouldRunInMemory: false/*, watcherCluster: true*/);
            using var store = GetDocumentStore(new Options()
            {
                ReplicationFactor = 3,
                Server = leader,
                RunInMemory = false
            });
            string id = "User/33-A";
            using (var session = store.OpenAsyncSession())
            {
                await session.StoreAsync(new User { Id = id, Name = "1" });
                await session.StoreAsync(new User { Name = "2" });
                await session.SaveChangesAsync();
            }
            WaitForUserToContinueTheTest(store);

            await store.Subscriptions
                .CreateAsync(new SubscriptionCreationOptions<User>
                {
                    Name = "BackgroundSubscriptionWorker"
                });

            using var worker = store.Subscriptions.GetSubscriptionWorker<User>(new SubscriptionWorkerOptions("BackgroundSubscriptionWorker"));

            // dispose nodes
            var results = new List<(string DataDirectory, string Url, string NodeTag)>();
            foreach (var node in nodes)
            {
                var result = await DisposeServerAndWaitForFinishOfDisposalAsync(node);
                results.Add(result);
            }

            var cts = new CancellationTokenSource();
            var mre = new ManualResetEvent(false);
            var t = worker.Run(async batch =>
            {
                User user = null;
                using (var session = batch.OpenAsyncSession())
                {
                    user = await session.LoadAsync<User>(id);
                }

                if (user != null)
                {
                    mre.Set();
                }
            }, cts.Token);

            //revive nodes
            await Task.Delay(2000);
            foreach (var result in results)
            {
                var cs = new Dictionary<string, string>(DefaultClusterSettings);
                cs[RavenConfiguration.GetKey(x => x.Core.ServerUrls)] = result.Url;
                var revivedServer = GetNewServer(new ServerCreationOptions
                {
                    DeletePrevious = false,
                    RunInMemory = false,
                    DataDirectory = result.DataDirectory,
                    CustomSettings = cs
                });
            }
            
            Assert.True(mre.WaitOne(TimeSpan.FromSeconds(15)), "User didn't loaded.");
        }

        private class User
        {
            public string Id { get; set; }
            public string Name { get; set; }

        }
    }
}
