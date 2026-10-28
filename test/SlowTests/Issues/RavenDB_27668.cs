using System;
using System.Collections.Generic;
using System.IO;
using System.Linq;
using System.Threading;
using System.Threading.Tasks;
using FastTests;
using Raven.Client.Documents;
using Raven.Client.Documents.Operations.Backups;
using Raven.Client.Documents.Operations.ConnectionStrings;
using Raven.Client.Documents.Operations.ETL;
using Raven.Client.Documents.Operations.ETL.OLAP;
using Raven.Client.Documents.Operations.OngoingTasks;
using Raven.Client.Util;
using Raven.Server.Documents;
using Raven.Server.Documents.ETL;
using Raven.Server.ServerWide.Commands.ETL;
using Tests.Infrastructure;
using Tests.Infrastructure.Entities;
using Xunit;

namespace SlowTests.Issues
{
    public class RavenDB_27668 : RavenTestBase
    {
        private const string EtlName = "olap-test";
        private const string TransformationName = "Orders";

        // daily, about 12 hours from now - the next scheduled run won't happen during the test
        private static string FarFromNowFrequency => $"0 {DateTime.UtcNow.AddHours(12).Hour} * * *";

        private const string Script = @"
loadToOrders(noPartition(), {
    Company: this.Company,
    Freight: this.Freight
});";

        private static readonly TimeSpan Timeout = TimeSpan.FromSeconds(30);

        public RavenDB_27668(ITestOutputHelper output) : base(output)
        {
        }

        public enum RestartBy
        {
            TogglingTaskState,
            UpdatingTask
        }

        [RavenTheory(RavenTestCategory.Etl)]
        [InlineData(RestartBy.TogglingTaskState)]
        [InlineData(RestartBy.UpdatingTask)]
        public async Task RestartedOlapEtlProcessShouldNotExportBeforeTheNextScheduledRun(RestartBy restartBy)
        {
            using (var store = GetDocumentStore())
            {
                var path = NewDataPath(forceCreateDir: true);
                var database = await GetDocumentDatabaseInstanceFor(store);
                var (taskId, configuration) = await SetupOlapEtlAndWaitForInitialExport(store, database, path);

                var filesAfterInitialExport = CountFiles(path);

                await ModifyOrder(store);
                await RestartEtlProcess(store, database, taskId, configuration, restartBy);

                // the first pass of a restarted process used to export the pending changes right away
                Assert.Equal(filesAfterInitialExport, await WaitForValueAsync(() => CountFiles(path), filesAfterInitialExport + 1, timeout: 3000));
            }
        }

        [RavenFact(RavenTestCategory.Etl)]
        public async Task RestartedOlapEtlProcessShouldExportRightAwayIfItMissedAScheduledRun()
        {
            using (var store = GetDocumentStore())
            {
                var path = NewDataPath(forceCreateDir: true);
                var database = await GetDocumentDatabaseInstanceFor(store);
                var (taskId, configuration) = await SetupOlapEtlAndWaitForInitialExport(store, database, path);

                var filesAfterInitialExport = CountFiles(path);

                // the daily run was missed while the process wasn't running
                await UpdateEtlState(database, lastBatchTime: DateTime.UtcNow.AddDays(-2));

                await ModifyOrder(store);
                await RestartEtlProcess(store, database, taskId, configuration, RestartBy.TogglingTaskState);

                Assert.Equal(filesAfterInitialExport + 1, await WaitForValueAsync(() => CountFiles(path), filesAfterInitialExport + 1));
            }
        }

        [RavenFact(RavenTestCategory.Etl)]
        public async Task SavingTheProgressAfterEveryBatchShouldKeepTheStoredLastBatchTime()
        {
            using (var store = GetDocumentStore())
            {
                var path = NewDataPath(forceCreateDir: true);
                var database = await GetDocumentDatabaseInstanceFor(store);
                var (taskId, configuration) = await SetupOlapEtlAndWaitForInitialExport(store, database, path);

                // a missed scheduled run makes the restarted process export the modified order right away
                await UpdateEtlState(database, lastBatchTime: DateTime.UtcNow.AddDays(-2));
                var storedLastBatchTime = EtlProcess.GetProcessState(database, EtlName, TransformationName).LastBatchTime;

                // called after the progress of every batch was saved, and before the last batch time is saved
                var lastBatchTimeAfterSavingTheProgress = new TaskCompletionSource<DateTime?>(TaskCreationOptions.RunContinuationsAsynchronously);
                database.ForTestingPurposesOnly().BeforeSavingOlapEtlLastBatchTime = () =>
                    lastBatchTimeAfterSavingTheProgress.TrySetResult(EtlProcess.GetProcessState(database, EtlName, TransformationName).LastBatchTime);

                await ModifyOrder(store);
                await RestartEtlProcess(store, database, taskId, configuration, RestartBy.TogglingTaskState);

                Assert.True(await lastBatchTimeAfterSavingTheProgress.Task.WaitWithoutExceptionAsync(Timeout));
                Assert.Equal(storedLastBatchTime, await lastBatchTimeAfterSavingTheProgress.Task);
            }
        }

        [RavenFact(RavenTestCategory.Etl)]
        public async Task SavingTheProgressAfterEveryBatchShouldNotOverwriteANewerLastBatchTime()
        {
            using (var store = GetDocumentStore())
            {
                var path = NewDataPath(forceCreateDir: true);
                var database = await GetDocumentDatabaseInstanceFor(store);
                var (taskId, configuration) = await SetupOlapEtlAndWaitForInitialExport(store, database, path);

                // a missed scheduled run makes the restarted process export the modified order right away
                await UpdateEtlState(database, lastBatchTime: DateTime.UtcNow.AddDays(-2));

                // the batch has already read the state, store a newer last batch time like another shard would
                var newerLastBatchTime = DateTime.UtcNow.AddDays(-1);
                var newerLastBatchTimeStored = 0;
                database.ForTestingPurposesOnly().OnEtlItemExtracted = (_, _, _) =>
                {
                    if (Interlocked.Exchange(ref newerLastBatchTimeStored, 1) == 0)
                        UpdateEtlState(database, newerLastBatchTime).GetAwaiter().GetResult();
                };

                // called after the progress of every batch was saved, and before the last batch time is saved
                var lastBatchTimeAfterSavingTheProgress = new TaskCompletionSource<DateTime?>(TaskCreationOptions.RunContinuationsAsynchronously);
                database.ForTestingPurposesOnly().BeforeSavingOlapEtlLastBatchTime = () =>
                    lastBatchTimeAfterSavingTheProgress.TrySetResult(EtlProcess.GetProcessState(database, EtlName, TransformationName).LastBatchTime);

                await ModifyOrder(store);
                await RestartEtlProcess(store, database, taskId, configuration, RestartBy.TogglingTaskState);

                Assert.True(await lastBatchTimeAfterSavingTheProgress.Task.WaitWithoutExceptionAsync(Timeout));
                Assert.Equal(newerLastBatchTime, await lastBatchTimeAfterSavingTheProgress.Task);
            }
        }

        [RavenFact(RavenTestCategory.Etl)]
        public async Task SavingTheLastBatchTimeShouldBeRetriedOnTimeout()
        {
            using (var store = GetDocumentStore())
            {
                var path = NewDataPath(forceCreateDir: true);
                var database = await GetDocumentDatabaseInstanceFor(store);
                var (taskId, configuration) = await SetupOlapEtlAndWaitForInitialExport(store, database, path);

                var saveAttempts = 0;
                database.ForTestingPurposesOnly().BeforeSavingOlapEtlLastBatchTime = () =>
                {
                    if (Interlocked.Increment(ref saveAttempts) < 3)
                        throw new TimeoutException("Simulating a failure to save the last batch time");
                };

                // a missed scheduled run makes the restarted process export the modified order right away
                var missedLastBatchTime = DateTime.UtcNow.AddDays(-2);
                await UpdateEtlState(database, missedLastBatchTime);
                await ModifyOrder(store);
                await RestartEtlProcess(store, database, taskId, configuration, RestartBy.TogglingTaskState);

                Assert.True(await WaitForValueAsync(() => EtlProcess.GetProcessState(database, EtlName, TransformationName).LastBatchTime > missedLastBatchTime, true));
                Assert.Equal(3, Volatile.Read(ref saveAttempts));

                var process = database.EtlLoader.Processes.Single();
                Assert.Null(database.TaskErrorsStorage.ReadLatestProcessErrorOfTask(process.TaskCategory, process.Name));

                var filesAfterSave = CountFiles(path);

                await ModifyOrder(store);

                // the saved last batch time schedules the next run
                Assert.Equal(filesAfterSave, await WaitForValueAsync(() => CountFiles(path), filesAfterSave + 1, timeout: 3000));
                Assert.Equal(3, Volatile.Read(ref saveAttempts));
            }
        }

        private async Task<(long TaskId, OlapEtlConfiguration Configuration)> SetupOlapEtlAndWaitForInitialExport(DocumentStore store, DocumentDatabase database, string path)
        {
            using (var session = store.OpenAsyncSession())
            {
                for (var i = 1; i <= 3; i++)
                    await session.StoreAsync(new Order { Company = $"companies/{i}", Freight = i }, $"orders/{i}");

                await session.SaveChangesAsync();
            }

            var etlDone = Etl.WaitForEtlToComplete(store);

            var configuration = new OlapEtlConfiguration
            {
                Name = EtlName,
                ConnectionStringName = "local",
                RunFrequency = FarFromNowFrequency,
                Transforms = { new Transformation { Name = TransformationName, Collections = new List<string> { "Orders" }, Script = Script } }
            };

            var result = Etl.AddEtl(store, configuration, new OlapConnectionString
            {
                Name = "local",
                LocalSettings = new LocalSettings { FolderPath = path }
            });

            // a new task exports right away
            Assert.True(await etlDone.WaitAsync(Timeout));
            Assert.True(CountFiles(path) > 0);

            // the last batch time is stored after the batch completion is reported
            Assert.True(await WaitForValueAsync(() => EtlProcess.GetProcessState(database, EtlName, TransformationName).LastBatchTime != null, true));

            return (result.TaskId, configuration);
        }

        private static async Task ModifyOrder(DocumentStore store)
        {
            using (var session = store.OpenAsyncSession())
            {
                var order = await session.LoadAsync<Order>("orders/1");
                order.Freight++;
                await session.SaveChangesAsync();
            }
        }

        private async Task RestartEtlProcess(DocumentStore store, DocumentDatabase database, long taskId, OlapEtlConfiguration configuration, RestartBy restartBy)
        {
            var oldProcess = database.EtlLoader.Processes.Single();

            switch (restartBy)
            {
                case RestartBy.TogglingTaskState:
                    await store.Maintenance.SendAsync(new ToggleOngoingTaskStateOperation(taskId, OngoingTaskType.OlapEtl, disable: true));
                    await store.Maintenance.SendAsync(new ToggleOngoingTaskStateOperation(taskId, OngoingTaskType.OlapEtl, disable: false));
                    break;
                case RestartBy.UpdatingTask:
                    // the process is recreated only if the configuration has changed
                    configuration.RunFrequency = $"30 {DateTime.UtcNow.AddHours(12).Hour} * * *";
                    await store.Maintenance.SendAsync(new UpdateEtlOperation<OlapConnectionString>(taskId, configuration));
                    break;
                default:
                    throw new ArgumentOutOfRangeException(nameof(restartBy), restartBy, null);
            }

            Assert.True(await WaitForValueAsync(() => database.EtlLoader.Processes.Any(p => p != oldProcess), true));
        }

        private static async Task UpdateEtlState(DocumentDatabase database, DateTime? lastBatchTime)
        {
            var state = EtlProcess.GetProcessState(database, EtlName, TransformationName);
            var serverStore = database.ServerStore;

            var command = new UpdateEtlProcessStateCommand(database.Name, EtlName, TransformationName, state.GetLastProcessedEtag(database.DbBase64Id, serverStore.NodeTag),
                state.ChangeVector, serverStore.NodeTag, serverStore.LicenseManager.HasHighlyAvailableTasks(), database.DbBase64Id, RaftIdGenerator.NewId(),
                state.SkippedTimeSeriesDocs, lastBatchTime);

            var (index, _) = await serverStore.SendToLeaderAsync(command);
            await database.RachisLogIndexNotifications.WaitForIndexNotification(index, serverStore.Engine.OperationTimeout);
        }

        private static int CountFiles(string path) => Directory.GetFiles(path, "*.*", SearchOption.AllDirectories).Length;
    }
}
