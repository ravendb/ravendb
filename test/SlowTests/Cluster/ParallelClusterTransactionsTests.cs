using System;
using System.Collections.Generic;
using System.Linq;
using System.Runtime.CompilerServices;
using System.Threading;
using System.Threading.Tasks;
using FastTests.Server.Replication;
using Raven.Client.Documents.Operations.CompareExchange;
using Raven.Client.Documents.Session;
using Raven.Server;
using Raven.Server.Config;
using Raven.Server.ServerWide.Context;
using Raven.Server.Utils;
using Raven.Tests.Core.Utils.Entities;
using Xunit;
using Xunit.Abstractions;

namespace SlowTests.Cluster
{
    public class ParallelClusterTransactionsTests : ReplicationTestBase
    {
        public ParallelClusterTransactionsTests(ITestOutputHelper output) : base(output)
        {
        }

        // the values are lower to make the cluster less stable
        protected override RavenServer GetNewServer(ServerCreationOptions options = null, [CallerMemberName] string caller = null)
        {
            if (options == null)
            {
                options = new ServerCreationOptions();
            }
            if (options.CustomSettings == null)
                options.CustomSettings = new Dictionary<string, string>();

            options.CustomSettings[RavenConfiguration.GetKey(x => x.Cluster.OperationTimeout)] = "10";
            options.CustomSettings[RavenConfiguration.GetKey(x => x.Cluster.StabilizationTime)] = "1";
            options.CustomSettings[RavenConfiguration.GetKey(x => x.Cluster.TcpConnectionTimeout)] = "3000";
            options.CustomSettings[RavenConfiguration.GetKey(x => x.Cluster.ElectionTimeout)] = "50";

            return base.GetNewServer(options, caller);
        }

        [Fact(Skip="RavenDB-22199")]
        public Task ParallelClusterTransactions() => ParallelClusterTransactions(3);


        public async Task ParallelClusterTransactions(int numberOfNodes)
        {
            DebuggerAttachedTimeout.DisableLongTimespan = true;

            var cluster = await CreateRaftCluster(numberOfNodes);
            var db = GetDatabaseName();
            using (GetDocumentStore(new Options
            {
                Server = cluster.Leader,
                ReplicationFactor = numberOfNodes,
                ModifyDatabaseName = _ => db
            }))
            {
                var count = 0;
                var tasks = new List<Task>();
                var random = new Random();

                for (int i = 0; i < 100; i++)
                {
                    var t = Task.Run(async () =>
                    {
                        var nodeNum = random.Next(0, numberOfNodes);
                        using (var store = GetDocumentStore(new Options
                        {
                            Server = cluster.Nodes[nodeNum],
                            CreateDatabase = false
                        }))
                        {
                            for (int j = 0; j < 10; j++)
                            {
                                try
                                {
                                    await store.Operations.ForDatabase(db).SendAsync(new PutCompareExchangeValueOperation<User>($"usernames/{Interlocked.Increment(ref count)}", new User(), 0));

                                    using (var session = store.OpenAsyncSession(db))
                                    {
                                        session.Advanced.SetTransactionMode(TransactionMode.ClusterWide);
                                        session.Advanced.ClusterTransaction.CreateCompareExchangeValue($"usernames/{Interlocked.Increment(ref count)}", new User());
                                        await session.StoreAsync(new User());
                                        await session.SaveChangesAsync();
                                    }
                                }
                                catch
                                {
                                    //
                                }
                            }
                        }
                    });
                    tasks.Add(t);
                }


                var cts = new CancellationTokenSource();
                var checkingTask = Task.Run(async () =>
                {
                    while (cts.IsCancellationRequested == false)
                    {
                        foreach (var n in cluster.Nodes)
                        {
                            var lastNodeNotifiedIndex = n.ServerStore.Cluster.LastNotifiedIndex;
                            using (n.ServerStore.Engine.ContextPool.AllocateOperationContext(out ClusterOperationContext context))
                            using (context.OpenReadTransaction())
                            {
                                var lastCommitIndex = n.ServerStore.Engine.GetLastCommitIndex(context);
                                if (lastNodeNotifiedIndex > lastCommitIndex)
                                {
                                    var logs = Cluster.CollectLogs(context, n);
                                    throw new InvalidOperationException(
                                        $"node {n.ServerStore.NodeTag} notified {lastNodeNotifiedIndex}, committed: {lastCommitIndex}{Environment.NewLine}{logs}");
                                }
                            }
                        }

                        try
                        {
                            await Task.Delay(150, cts.Token);
                        }
                        catch
                        {
                            // ignore
                        }
                    }
                });
                var destablizeTask = Task.Run(async () =>
                {
                    while (cts.IsCancellationRequested == false)
                    {
                        try
                        {
                            await ActionWithLeader(l => l.ServerStore.Engine.CurrentLeader.StepDown(forceElection: false));
                            await Task.Delay(1000, cts.Token);
                        }
                        catch
                        {
                            // ignore
                        }

                    }
                });

                foreach (var task in tasks)
                {
                    try
                    {
                        await task;
                    }
                    catch
                    {
                        // ignore
                    }

                }

                cts.Cancel();
                await checkingTask;
                await destablizeTask;

                var maxTerm = cluster.Nodes.Select(x => x.ServerStore.Engine.CurrentTerm).Max();
                long maxTermOld;
                var attempts = 3 * numberOfNodes;
                do
                {
                    maxTermOld = maxTerm;
                    await Task.Delay(TimeSpan.FromSeconds(3) * numberOfNodes);
                    maxTerm = cluster.Nodes.Select(x => x.ServerStore.Engine.CurrentTerm).Max();

                    attempts--; // cluster couldn't stabilize
                } while (maxTerm != maxTermOld && attempts > 0);

                var compareExchangeCount = new HashSet<long>();
                var maxLog = 0L;

                foreach (var n in cluster.Nodes.Where(x => x.ServerStore.Engine.CurrentTerm >= maxTerm))
                {
                    using (n.ServerStore.Engine.ContextPool.AllocateOperationContext(out ClusterOperationContext context))
                    using (context.OpenReadTransaction())
                    {
                        var currentLog = n.ServerStore.Engine.GetLastEntryIndex(context);
                        if (maxLog < currentLog)
                            maxLog = currentLog;
                    }
                }

                foreach (var node in cluster.Nodes)
                {
                    await node.ServerStore.Cluster.WaitForIndexNotification(maxLog, TimeSpan.FromMinutes(1));

                    using (node.ServerStore.ContextPool.AllocateOperationContext(out TransactionOperationContext context))
                    using (context.OpenReadTransaction())
                    {
                        compareExchangeCount.Add(node.ServerStore.Cluster.GetNumberOfCompareExchange(context, db));
                    }
                }

                Assert.Equal(1, compareExchangeCount.Count);
            }
        }
    }
}
