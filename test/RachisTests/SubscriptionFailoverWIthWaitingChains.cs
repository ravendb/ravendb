using System;
using System.Collections.Generic;
using System.Diagnostics;
using System.IO;
using System.Linq;
using System.Threading;
using System.Threading.Tasks;
using Raven.Client.Documents;
using Raven.Client.Documents.Operations.OngoingTasks;
using Raven.Client.Documents.Subscriptions;
using Raven.Tests.Core.Utils.Entities;
using Sparrow.Platform;
using Sparrow.Server;
using Tests.Infrastructure;
using Xunit;
using Xunit.Abstractions;

namespace RachisTests
{
    public class SubscriptionFailoverWithWaitingChains : ClusterTestBase
    {
        public SubscriptionFailoverWithWaitingChains(ITestOutputHelper output) : base(output)
        {
        }

        [RavenTheory(RavenTestCategory.Cluster | RavenTestCategory.Subscriptions)]
        [RavenData(DatabaseMode = RavenDatabaseMode.All)]
        public async Task MakeSureSubscriptionProcessedAfterDisposingTheResponsibleNodes(Options options)
        {
            const int clusterSize = 5;
            var cluster = await CreateRaftCluster(clusterSize, shouldRunInMemory: false);

            options.Server = cluster.Leader;
            options.ReplicationFactor = clusterSize;
            options.ModifyDocumentStore = s => s.Conventions.ReadBalanceBehavior = Raven.Client.Http.ReadBalanceBehavior.RoundRobin;
            options.DeleteDatabaseOnDispose = false;

            using (var store = GetDocumentStore(options))
            {
                var namesList = new List<string> { "E", "G", "R" };
                var subsId = store.Subscriptions.Create<User>();
                using var subsWorker = store.Subscriptions.GetSubscriptionWorker<User>(new Raven.Client.Documents.Subscriptions.SubscriptionWorkerOptions(subsId)
                {
                    TimeToWaitBeforeConnectionRetry = TimeSpan.FromMilliseconds(16)
                });

                HashSet<string> redirects = new HashSet<string>();
                var mre = new ManualResetEvent(false);
                var processedItems = new List<string>();
                subsWorker.AfterAcknowledgment += batch =>
                {
                    foreach (var item in batch.Items)
                        processedItems.Add(item.Result.Name);

                    mre.Set();
                    return Task.CompletedTask;
                };
                subsWorker.OnSubscriptionConnectionRetry += ex =>
                {
                    redirects.Add(subsWorker.CurrentNodeTag);
                };

                _ = subsWorker.Run(x => { });

                List<string> toggledNodes = new List<string>();
                var toggleCount = Math.Round(clusterSize * 0.51);
                string previousResponsibleNode = string.Empty;
                for (int i = 0; i < toggleCount; i++)
                {
                    string currentResponsibleNode = string.Empty;
                    await ActionWithLeader(async l =>
                    {
                        currentResponsibleNode = await GetResponsibleNodeAndCompareWithPrevious(store, l.ServerStore.NodeTag, previousResponsibleNode, subsId);
                    });

                    toggledNodes.Add(currentResponsibleNode);
                    previousResponsibleNode = currentResponsibleNode;

                    var node = cluster.Nodes.FirstOrDefault(x => x.ServerStore.NodeTag == currentResponsibleNode);
                    Assert.NotNull(node);

                    if (i != 0)
                        mre.Reset();

                    using (var session = store.OpenSession())
                    {
                        session.Store(new User
                        {
                            Name = namesList[i]
                        });
                        session.SaveChanges();
                    }

                    Assert.True(mre.WaitOne(TimeSpan.FromSeconds(15)), "no ack");

                    var res = await DisposeServerAndWaitForFinishOfDisposalAsync(node);
                    Assert.Equal(currentResponsibleNode, res.NodeTag);
                }

                Assert.True(redirects.Count >= toggleCount, $"redirects count : {redirects.Count}, leaderNodeTag: {cluster.Leader.ServerStore.NodeTag}, missing: {string.Join(", ", cluster.Nodes.Select(x => x.ServerStore.NodeTag).Except(redirects))}, offline: {string.Join(", ", toggledNodes)}");
                Assert.Equal(namesList.Count, processedItems.Count);
                for (int i = 0; i < namesList.Count; i++)
                {
                    Assert.Equal(namesList[i], processedItems[i]);
                }
            }
        }

        private static async Task<string> GetResponsibleNodeAndCompareWithPrevious(DocumentStore store, string forNode, string previousResponsibleNode, string subsId)
        {
            string currentResponsibleNode = string.Empty;
            var responsibleTime = Debugger.IsAttached || PlatformDetails.Is32Bits ? 300_000 : 30_000;
            var op = new GetOngoingTaskInfoOperation(subsId, OngoingTaskType.Subscription);

            var sp = Stopwatch.StartNew();
            while (previousResponsibleNode == currentResponsibleNode || string.IsNullOrEmpty(currentResponsibleNode))
            {
                var res = await store.Maintenance.ForNode(forNode).SendAsync(op);
                currentResponsibleNode = res.ResponsibleNode.NodeTag;

                await Task.Delay(1000);

                Assert.True(sp.ElapsedMilliseconds < responsibleTime, $"Could not get the subscription ResponsibleNode in responsible time '{responsibleTime}'");
            }

            return currentResponsibleNode;
        }

        [RavenTheory(RavenTestCategory.Subscriptions)]
        [RavenData(DatabaseMode = RavenDatabaseMode.All)]
        public async Task SubscriptionShouldReconnectOnExceptionInTcpListener(Options options)
        {
            options.ModifyDocumentStore = s => s.Conventions.ReadBalanceBehavior = Raven.Client.Http.ReadBalanceBehavior.RoundRobin;

            using var server = GetNewServer(new ServerCreationOptions
            {
                RunInMemory = false,
                RegisterForDisposal = false,
            });

            options.Server = server;

            using (var store = GetDocumentStore(options))
            {
                var mre = new AsyncManualResetEvent();
                using (var session = store.OpenSession())
                {
                    session.Store(new User());
                    session.SaveChanges();
                }

                var subsId = await store.Subscriptions.CreateAsync<User>();
                using var subsWorker = store.Subscriptions.GetSubscriptionWorker<User>(new SubscriptionWorkerOptions(subsId)
                {
                    TimeToWaitBeforeConnectionRetry = TimeSpan.FromMilliseconds(16)
                });
                subsWorker.OnSubscriptionConnectionRetry += ex =>
                {
                    Assert.NotNull(ex);
                    
                    if (ex is AggregateException ae)
                        Assert.True(ae.InnerExceptions.Count(e => e.GetType() == typeof(IOException) || e.GetType() == typeof(EndOfStreamException)) > 0);
                    else
                        Assert.True(ex.GetType() == typeof(IOException) || ex.GetType() == typeof(EndOfStreamException));
                    mre.Set();
                };
                server.ForTestingPurposesOnly().ThrowExceptionInListenToNewTcpConnection = true;
                try
                {
                    var task = subsWorker.Run(x => { });
                    Assert.True(await mre.WaitAsync(TimeSpan.FromSeconds(30)));
                }
                finally
                {
                    server.ForTestingPurposesOnly().ThrowExceptionInListenToNewTcpConnection = false;
                }
            }
        }
    }
}
