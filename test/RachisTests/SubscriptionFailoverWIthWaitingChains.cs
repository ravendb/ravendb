using System;
using System.Collections.Generic;
using System.Diagnostics;
using System.IO;
using System.Linq;
using System.Reflection;
using System.Threading;
using System.Threading.Tasks;
using Raven.Client.Documents;
using Raven.Client.Documents.Session;
using Raven.Client.Documents.Subscriptions;
using Raven.Client.Exceptions;
using Raven.Client.Exceptions.Database;
using Raven.Client.Exceptions.Documents.Subscriptions;
using Raven.Client.Extensions;
using Raven.Client.Util;
using Raven.Server;
using Raven.Server.Documents;
using Raven.Server.Documents.Subscriptions;
using Raven.Server.ServerWide.Context;
using Raven.Tests.Core.Utils.Entities;
using Sparrow.Platform;
using Sparrow.Server;
using Tests.Infrastructure;
using Xunit;
using Xunit.Abstractions;
using Xunit.Sdk;

namespace RachisTests
{
    public class SubscriptionFailoverWithWaitingChains : ClusterTestBase
    {
        private class CountdownsArray : IDisposable
        {
            private CountdownEvent[] _array;

            public CountdownsArray(int arraySize, int countdownCount)
            {
                _array = new CountdownEvent[arraySize];
                for (var i = 0; i < arraySize; i++)
                {
                    _array[i] = new CountdownEvent(countdownCount);
                }
            }

            public CountdownEvent[] GetArray()
            {
                return _array.ToArray();
            }

            public void Dispose()
            {
                foreach (var cde in _array)
                {
                    cde.Dispose();
                }
            }
        }

        public SubscriptionFailoverWithWaitingChains(ITestOutputHelper output) : base(output)
        {
        }

        private class TestParams : DataAttribute
        {
            public TestParams(int subscriptionsChainSize, int clusterSize, int dBGroupSize, bool shouldTrapRevivedNodesIntoCandidate)
            {
                SubscriptionsChainSize = subscriptionsChainSize;
                ClusterSize = clusterSize;
                DBGroupSize = dBGroupSize;
                ShouldTrapRevivedNodesIntoCandidate = shouldTrapRevivedNodesIntoCandidate;
            }

            public int SubscriptionsChainSize { get; }
            public int ClusterSize { get; }
            public int DBGroupSize { get; }
            public bool ShouldTrapRevivedNodesIntoCandidate { get; }

            public override IEnumerable<object[]> GetData(MethodInfo testMethod)
            {
                yield return new object[]{
                    SubscriptionsChainSize,
                    ClusterSize,
                    DBGroupSize,
                    ShouldTrapRevivedNodesIntoCandidate};
            }
        }

        [Fact]
        public async Task MakeSureSubscriptionProcessedAfterDisposingTheResponsibleNodes()
        {
            const int clusterSize = 5;
            var cluster = AsyncHelpers.RunSync(() => CreateRaftCluster(clusterSize, shouldRunInMemory: false));

            using (var store = GetDocumentStore(new Options
            {
                Server = cluster.Leader,
                ReplicationFactor = clusterSize,
                ModifyDocumentStore = s => s.Conventions.ReadBalanceBehavior = Raven.Client.Http.ReadBalanceBehavior.RoundRobin,
                DeleteDatabaseOnDispose = false
            }))
            {
                var namesList = new List<string> { "E", "G", "R" };
                var databaseName = store.Database;
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
                        currentResponsibleNode = await GetResponsibleNodeAndCompareWithPrevious(l, databaseName, previousResponsibleNode, subsId);
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

        private static async Task<string> GetResponsibleNodeAndCompareWithPrevious(RavenServer l, string databaseName, string previousResponsibleNode, string subsId)
        {
            string currentResponsibleNode = string.Empty;
            var responsibleTime = Debugger.IsAttached || PlatformDetails.Is32Bits ? 300_000 : 30_000;
            var documentDatabase = await l.ServerStore.DatabasesLandlord.TryGetOrCreateResourceStore(databaseName);

            var sp = Stopwatch.StartNew();
            while (previousResponsibleNode == currentResponsibleNode || string.IsNullOrEmpty(currentResponsibleNode))
            {
                using (documentDatabase.ServerStore.ContextPool.AllocateOperationContext(out TransactionOperationContext context))
                using (context.OpenReadTransaction())
                {
                    currentResponsibleNode = documentDatabase.SubscriptionStorage.GetResponsibleNode(context, subsId);
                }

                await Task.Delay(1000);

                Assert.True(sp.ElapsedMilliseconds < responsibleTime, $"Could not get the subscription ResponsibleNode in responsible time '{responsibleTime}'");
            }

            return currentResponsibleNode;
        }

        [Fact]
        public async Task SubscriptionShouldReconnectOnExceptionInTcpListener()
        {
            using var server = GetNewServer(new ServerCreationOptions
            {
                RunInMemory = false,
                RegisterForDisposal = false,
            });
            using (var store = GetDocumentStore(new Options
            {
                Server = server,
                ModifyDocumentStore = s => s.Conventions.ReadBalanceBehavior = Raven.Client.Http.ReadBalanceBehavior.RoundRobin,
            }))
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

        internal static async Task ContinuouslyGenerateDocsInternal(int DocsBatchSize, DocumentStore store, CancellationToken token)
        {
            try
            {
                var ids = new List<string>();
                using (var session = store.OpenAsyncSession(new SessionOptions
                {
                    TransactionMode = TransactionMode.ClusterWide
                }))
                {
                    for (var k = 0; k < DocsBatchSize; k++)
                    {
                        User entity = new User
                        {
                            Name = "ClusteredJohnny" + k
                        };
                        await session.StoreAsync(entity, token);
                        ids.Add(session.Advanced.GetDocumentId(entity));
                    }
                    await session.SaveChangesAsync(token);
                }

                using (var session = store.OpenAsyncSession())
                {
                    for (var k = 0; k < DocsBatchSize; k++)
                    {
                        await session.StoreAsync(new User
                        {
                            Name = "Johnny" + k
                        }, token);
                    }
                    await session.SaveChangesAsync(token);
                }

                using (var session = store.OpenAsyncSession())
                {
                    for (var k = 0; k < DocsBatchSize; k++)
                    {
                        var user = await session.LoadAsync<User>(ids[k]);
                        user.Age++;
                    }
                    await session.SaveChangesAsync(token);
                }
                await Task.Delay(16, token);
            }
            catch (AllTopologyNodesDownException)
            {
            }
            catch (DatabaseDisabledException)
            {
            }
            catch (DatabaseDoesNotExistException)
            {
            }
            catch (RavenException)
            {
            }
        }

        internal static async Task<SubscriptionStorage.SubscriptionGeneralDataAndStats> GetSubscription(string name, string database, List<RavenServer> nodes, CancellationToken token = default)
        {
            foreach (var curNode in nodes)
            {
                DocumentDatabase db;
                try
                {
                    db = await curNode.ServerStore.DatabasesLandlord.TryGetOrCreateResourceStore(database).WithCancellation(token);
                }
                catch (DatabaseNotRelevantException)
                {
                    continue;
                }

                using (curNode.ServerStore.ContextPool.AllocateOperationContext(out TransactionOperationContext context))
                using (context.OpenReadTransaction())
                {
                    SubscriptionStorage.SubscriptionGeneralDataAndStats subscription = null;
                    try
                    {
                        subscription = db
                            .SubscriptionStorage
                            .GetSubscription(context, id: null, name, history: false);
                    }
                    catch (SubscriptionDoesNotExistException)
                    {
                        // expected
                    }

                    if (subscription == null)
                        continue;

                    return subscription;
                }
            }

            return null;
        }
    }
}
