using System;
using System.Collections.Generic;
using System.Text;
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

namespace StressTests.Cluster
{
    public class ClusterStressTests : ReplicationTestBase
    {
        // the values are lower to make the cluster less stable
        protected override RavenServer GetNewServer(ServerCreationOptions options = null)
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

            return base.GetNewServer(options);
        }

        [Fact]
        public async Task ParallelClusterTransactions()
        {
            DebuggerAttachedTimeout.DisableLongTimespan = true;
            var numberOfNodes = 7;
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

                var compareExchangeCount = new HashSet<long>();
                var lastLog = 0L;

                await ActionWithLeader((leader) =>
                {
                    using (leader.ServerStore.ContextPool.AllocateOperationContext(out TransactionOperationContext ctx))
                    using (ctx.OpenReadTransaction())
                    {
                        lastLog = leader.ServerStore.Engine.GetLastEntryIndex(ctx);
                    }
                    return Task.CompletedTask;
                });

                foreach (var node in cluster.Nodes)
                {
                    await node.ServerStore.Cluster.WaitForIndexNotification(lastLog);

                    using (node.ServerStore.ContextPool.AllocateOperationContext(out TransactionOperationContext ctx))
                    using (ctx.OpenReadTransaction())
                    {
                        compareExchangeCount.Add(node.ServerStore.Cluster.GetNumberOfCompareExchange(ctx, db));
                    }
                }

                Assert.Equal(1, compareExchangeCount.Count);
            }
        }
    }
}
