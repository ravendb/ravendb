using System;
using System.Collections.Generic;
using System.Diagnostics;
using System.Linq;
using System.Threading;
using System.Threading.Tasks;
using FastTests.Server.Replication;
using Raven.Client.Documents;
using Raven.Client.Documents.Operations;
using Raven.Client.ServerWide.Operations;
using Raven.Server;
using Raven.Server.Config;
using Xunit;

namespace SlowTests.Issues
{
    public class RavenDB_7912 : ReplicationTestBase
    {
        [Fact]
        public async Task InstallSnapshotShouldHandleDeletionInProgress()
        {
            var databaseName = nameof(InstallSnapshotShouldHandleDeletionInProgress) + Guid.NewGuid();
            using (var leader = await CreateRaftClusterAndGetLeader(3, shouldRunInMemory: false, leaderIndex: 0))
            {
                using (var cts = new CancellationTokenSource(TimeSpan.FromSeconds(120)))
                {
                    await CreateDatabaseInCluster(databaseName, 3, leader.WebUrl);

                    using (var leaderStore = new DocumentStore
                    {
                        Urls = new[] { leader.WebUrl },
                        Database = databaseName
                    })
                    {
                        leaderStore.Initialize();

                        var stats = await leaderStore.Maintenance.SendAsync(new GetStatisticsOperation(), cts.Token);
                        Assert.NotNull(stats);

                        await DisposeServerAndWaitForFinishOfDisposalAsync(Servers[1], cts.Token);

                        await leaderStore.Maintenance.Server.SendAsync(new DeleteDatabasesOperation(databaseName, hardDelete: true), cts.Token);

                        var url = Servers[1].WebUrl;
                        var dataDir = Servers[1].Configuration.Core.DataDirectory.FullPath.Split('/').Last();

                        Servers[1] = GetNewServer(new Dictionary<string, string>
                            {
                                {RavenConfiguration.GetKey(x => x.Core.PublicServerUrl), url},
                                {RavenConfiguration.GetKey(x => x.Core.ServerUrls), url}
                            },
                            runInMemory: false,
                            deletePrevious: false,
                            partialPath: dataDir);

                        Assert.True(await WaitForDatabaseToBeDeleted(Servers[1], databaseName, TimeSpan.FromSeconds(30), cts.Token));
                    }
                }
            }
        }

        private static async Task<bool> WaitForDatabaseToBeDeleted(RavenServer server, string databaseName, TimeSpan timeout, CancellationToken token)
        {
            using (var store = new DocumentStore
            {
                Urls = new[] { server.WebUrl },
                Database = databaseName,
                Conventions =
                {
                    DisableTopologyUpdates = true
                }
            })
            {
                store.Initialize();

                var pollingInterval = timeout.TotalSeconds < 1 ? timeout : TimeSpan.FromSeconds(1);
                var sw = Stopwatch.StartNew();
                while (true)
                {
                    var delayTask = Task.Delay(pollingInterval, token);
                    var dbTask = store.Maintenance.Server.SendAsync(new GetDatabaseRecordOperation(databaseName), token);
                    var doneTask = await Task.WhenAny(dbTask, delayTask);
                    if (doneTask == delayTask)
                    {
                        if (sw.Elapsed > timeout)
                        {
                            return false;
                        }

                        await Task.Delay(100, token);
                        continue;
                    }

                    var dbRecord = await dbTask;
                    if (dbRecord?.DeletionInProgress == null || dbRecord.DeletionInProgress.Count == 0)
                    {
                        return true;
                    }
                }
            }
        }
    }
}
