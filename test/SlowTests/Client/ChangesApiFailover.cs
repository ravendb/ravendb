using System;
using System.Collections.Generic;
using System.Diagnostics;
using System.Linq;
using System.Text;
using System.Threading;
using System.Threading.Tasks;
using FastTests;
using Raven.Client.Documents.Session;
using Tests.Infrastructure;
using Tests.Infrastructure.Entities;
using Xunit;
using Xunit.Abstractions;

namespace SlowTests.Client
{
    public class ChangesApiFailover : ClusterTestBase
    {
        public ChangesApiFailover(ITestOutputHelper output) : base(output)
        {
        }

        [RavenTheory(RavenTestCategory.ChangesApi | RavenTestCategory.Cluster)]
        [RavenData(DatabaseMode = RavenDatabaseMode.All)]
        public async Task Can_Subscribe_To_Single_Document_Changes_With_Failover(Options options)
        {
            var cluster = await CreateRaftCluster(3, watcherCluster: true);
            options.Server = cluster.Leader;
            options.ReplicationFactor = 3;

            using (var store = GetDocumentStore(options))
            {
                store.GetRequestExecutor().OnTopologyUpdated += OnTopologyChange;
                using (var changes = store.Changes())
                {
                    var re = store.GetRequestExecutor();
                    re.OnTopologyUpdated += OnTopologyChange;

                    await changes.EnsureConnectedNow();

                    const int numberOfDocuments = 10;
                    var count = 0L;
                    var cde = new CountdownEvent(numberOfDocuments);

                    for (var i = 0; i < numberOfDocuments; i++)
                    {
                        var forDocument = changes
                            .ForDocument($"orders/{i}");

                        forDocument.Subscribe(x =>
                        {
                            Interlocked.Increment(ref count);
                            cde.Signal();
                        });

                        await forDocument.EnsureSubscribedNow();
                    }

                    for (var i = 0; i < numberOfDocuments; i++)
                    {
                        using (var session = store.OpenAsyncSession())
                        {
                            await session.StoreAsync(new Order(), $"orders/{i}");
                            await session.SaveChangesAsync();
                        }

                        if (i == 0)
                        {
                            var s = Servers.First(s => s != cluster.Leader);
                            await DisposeServerAndWaitForFinishOfDisposalAsync(s);
                            using (var cts = new CancellationTokenSource(TimeSpan.FromSeconds(15)))
                            {
                                await Cluster.WaitForNodeToBeRehabAsync(store, s.ServerStore.NodeTag, token: cts.Token);
                            }
                        }
                    }

                    Assert.True(cde.Wait(TimeSpan.FromSeconds(60)), $"Missed {cde.CurrentCount} events.");
                    Assert.Equal(numberOfDocuments, count);

                    re.OnTopologyUpdated -= OnTopologyChange;
                }
            }
        }

        private void OnTopologyChange(object sender, TopologyUpdatedEventArgs args)
        {
            foreach (var node in args.Topology.Nodes)
            {
                Debug.Assert(node.Database.Contains('$') == false, $"{node.Database} must not contain '$' char.");
            }
        }
    }
}
