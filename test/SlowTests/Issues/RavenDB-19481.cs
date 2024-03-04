using System;
using System.Collections.Concurrent;
using System.Collections.Generic;
using System.IO;
using System.Linq;
using System.Text;
using System.Threading;
using System.Threading.Tasks;
using FastTests;
using FastTests.Graph;
using FastTests.Utils;
using Nest;
using NetTopologySuite.Utilities;
using Raven.Client.Documents.Operations;
using Raven.Client.Documents.Operations.Backups;
using Raven.Client.Documents.Session;
using Raven.Server.ServerWide.Context;
using Tests.Infrastructure;
using Xunit;
using Xunit.Abstractions;
using static Raven.Server.Smuggler.Documents.CounterItem;
using Assert = Xunit.Assert;

namespace SlowTests.Issues
{
    public class RavenDB_19481 : RavenTestBase
    {
        public RavenDB_19481(ITestOutputHelper output) : base(output)
        {
        }


        [RavenFact(RavenTestCategory.Smuggler)]
        public async Task ShouldntReapplyClusterTransactionTwiceInRestore()
        {
            DoNotReuseServer();

            var backupPath = NewDataPath(suffix: "BackupFolder");
            using (var store = GetDocumentStore())
            {
                const string id = "users/1";

                await RevisionsHelper.SetupRevisions(Server.ServerStore, store.Database);

                using (var session = store.OpenAsyncSession(new SessionOptions
                {
                    TransactionMode = TransactionMode.ClusterWide
                }))
                {
                    await session.StoreAsync(new User
                    {
                        Name = "Grisha"
                    }, id);
                    await session.SaveChangesAsync();
                }

                var stats = await store.Maintenance.SendAsync(new GetStatisticsOperation());
                Assert.Equal(1, stats.CountOfRevisionDocuments);


                var config = Backup.CreateBackupConfiguration(backupPath);
                var backupTaskId = await Backup.UpdateConfigAndRunBackupAsync(Server, config, store);
                await Backup.RunBackupAndReturnStatusAsync(Server, backupTaskId, store, isFullBackup: false, expectedEtag: 4);

                var databaseName = $"restored_database-{Guid.NewGuid()}";

                var clusterTransactions = new Dictionary<string, long>();
                Server.ServerStore.ForTestingPurposesOnly().BeforeExecuteClusterTransactionBatch = (dbName, batch) =>
                {
                    if (dbName == databaseName)
                    {
                        lock (clusterTransactions) 
                        {
                            foreach (var clusterTx in batch)
                            {
                                var raftRequestId = clusterTx.Options.TaskId;

                                if (clusterTransactions.ContainsKey(clusterTx.Options.TaskId) == false)
                                    clusterTransactions.Add(raftRequestId, 1);
                                else
                                    clusterTransactions[raftRequestId]++;
                            }
                        }
                    }
                };

                using (Backup.RestoreDatabase(store,
                           new RestoreBackupConfiguration
                           {
                               BackupLocation = Directory.GetDirectories(backupPath).First(),
                               DatabaseName = databaseName
                           }))
                {
                }

                foreach (var kvp in clusterTransactions)
                {
                    var timesWasApplied = kvp.Value;
                    Assert.True(timesWasApplied <=1 , $"cluster transaction \"{kvp.Key}\" was reapplied more then once ({timesWasApplied} times)");
                }
            }
        }

    }
}
