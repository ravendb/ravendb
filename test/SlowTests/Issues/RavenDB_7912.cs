using System;
using System.Collections.Generic;
using System.Diagnostics;
using System.Linq;
using System.Threading;
using System.Threading.Tasks;
using FastTests.Server.Replication;
using Raven.Client.Documents;
using Raven.Client.Documents.Operations;
using Raven.Client.Exceptions.Database;
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
                await CreateDatabaseInCluster(databaseName, 3, leader.WebUrl);

                using (var leaderStore = new DocumentStore
                {
                    Urls = new[] { leader.WebUrl },
                    Database = databaseName
                })
                {
                    leaderStore.Initialize();

                    var stats = leaderStore.Admin.Send(new GetStatisticsOperation());
                    Assert.NotNull(stats);

                    DisposeServerAndWaitForFinishOfDisposal(Servers[1]);

                    leaderStore.Admin.Server.Send(new DeleteDatabaseOperation(databaseName, hardDelete: true));

                    var url = Servers[1].WebUrl;
                    var dataDir = Servers[1].Configuration.Core.DataDirectory.FullPath.Split('/').Last();

                    Servers[1] = GetNewServer(new Dictionary<string, string>
                        {
                            {RavenConfiguration.GetKey(x => x.Core.PublicServerUrl), url},
                            {RavenConfiguration.GetKey(x => x.Core.ServerUrl), url}
                        },
                        runInMemory: false,
                        deletePrevious: false,
                        partialPath: dataDir);

                    WaitForDatabaseToBeDeleted(Servers[1], databaseName, TimeSpan.FromSeconds(30));
                }
            }
        }

        private static void WaitForDatabaseToBeDeleted(RavenServer server, string databaseName, TimeSpan timeout)
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

                var sw = Stopwatch.StartNew();
                while (sw.Elapsed < timeout)
                {
                    try
                    {
                        store.Admin.Send(new GetStatisticsOperation());
                    }
                    catch (DatabaseDoesNotExistException)
                    {
                        return;
                    }
                    catch (DatabaseDisabledException)
                    {
                        // continue
                    }

                    Thread.Sleep(100);
                }

                throw new InvalidOperationException($"Database '{databaseName}' was not deleted after snapshot installation.");
            }
        }
    }
}
