using System;
using System.Collections.Generic;
using System.IO;
using System.Linq;
using System.Net;
using System.Text;
using System.Threading;
using System.Threading.Tasks;
using Amazon.Glacier.Model;
using Esprima.Ast;
using FastTests.Graph;
using FastTests.Issues;
using InterversionTests;
using Jint;
using McMaster.Extensions.CommandLineUtils;
using Nest;
using Raven.Client.Documents;
using Raven.Client.Documents.Commands.Batches;
using Raven.Client.Documents.Conventions;
using Raven.Client.Documents.Session;
using Raven.Client.Exceptions;
using Raven.Client.ServerWide;
using Raven.Client.ServerWide.Commands.Cluster;
using Raven.Client.ServerWide.Operations;
using Raven.Server;
using Raven.Server.Config;
using Raven.Server.Monitoring.Snmp.Objects.Database;
using Raven.Server.ServerWide.Commands;
using Tests.Infrastructure;
using Xunit;
using Xunit.Abstractions;

namespace SlowTests.Issues
{
    public class RavenDB_20628_Interversion : MixedClusterTestBase
    {
        public RavenDB_20628_Interversion(ITestOutputHelper output) : base(output)
        {
        }

        [Fact]
        public async Task ClusterTransaction_Failover_Shouldnt_Throw_ConcurrencyException()
        {
            var (leader, peers, local) = await CreateMixedCluster(new[]
            {
                "5.4.105",
                "5.4.105"
            });

            string databaseName = GetDatabaseName();
            var urls = new List<string>(){ leader.WebUrl }.Concat(peers.Select(l => l.Url)).ToArray();

            var tags = leader.ServerStore.GetClusterTopology().AllNodes.Keys.Where(x => x != leader.ServerStore.NodeTag).ToList();


            using (var store1 = new DocumentStore { Urls = new[]
                   {
                       leader.WebUrl
                   } }.Initialize())
            {
                var record = new DatabaseRecord(databaseName)
                {
                    Topology = new DatabaseTopology
                    {
                        Members = new List<string>
                        {
                            leader.ServerStore.NodeTag,
                        }.Concat(tags).ToList()
                    },
                    Settings =
                    {
                        [RavenConfiguration.GetKey(x => x.Replication.ReplicationMinimalHeartbeat)] = "1",
                        [RavenConfiguration.GetKey(x => x.Replication.RetryReplicateAfter)] = "1",
                        [RavenConfiguration.GetKey(x => x.Core.RunInMemory)] = true.ToString(),
                        [RavenConfiguration.GetKey(x => x.Core.ThrowIfAnyIndexCannotBeOpened)] = true.ToString(),
                        [RavenConfiguration.GetKey(x => x.Indexing.MinNumberOfMapAttemptsAfterWhichBatchWillBeCanceledIfRunningLowOnMemory)] = int.MaxValue.ToString()
                    }
                };
                await CreateDatabase(store1, 3, dbName: databaseName, record);
            }


            using var store = new DocumentStore
            {
                Urls = new [] { leader.WebUrl },
                Database = databaseName
            }.Initialize();

            var killed = false;
            
            var disposeNodeTask = Task.Run(async () =>
            {
                await Task.Delay(700);
                var url = store.GetRequestExecutor(databaseName).TopologyNodes.First().Url;

                Assert.Equal(leader.WebUrl, url);
                await DisposeServerAndWaitForFinishOfDisposalAsync(leader);
                killed = true;
            });
            await ProcessDocument(store, "Docs/1-A");

            await disposeNodeTask;
            Assert.True(killed);

            foreach (var p in peers)
            {
                await KillSlavedServerProcessAsync(p.Process);
            }
        }


        private async Task ProcessDocument(IDocumentStore store, string id)
        {
            using (var session = store.OpenAsyncSession(new SessionOptions { TransactionMode = TransactionMode.ClusterWide }))
            {
                var doc = new Doc { Id = id };
                await session.StoreAsync(doc);
                await session.SaveChangesAsync();
            }

            for (int i = 0; i < 8000; i++)
            {
                if(i%1000 == 0)
                    Console.WriteLine($"ProcessDocument {i}");
                using (var session = store.OpenAsyncSession(new SessionOptions { TransactionMode = TransactionMode.ClusterWide }))
                {
                    var doc = await session.LoadAsync<Doc>(id);
                    doc.Progress = i;
                    await session.SaveChangesAsync();
                }
            }
        }

        public class Doc
        {
            public string Id { get; set; }
            public int Progress { get; set; }
        }
    }
}


