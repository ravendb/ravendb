using System;
using System.Collections.Generic;
using System.IO;
using System.Text;
using System.Threading;
using System.Threading.Tasks;
using FastTests;
using NCrontab.Advanced;
using Raven.Client.Documents;
using Raven.Client.Documents.Operations;
using Raven.Client.Documents.Operations.Backups;
using Raven.Client.ServerWide;
using Raven.Client.ServerWide.Operations;
using Raven.Client.ServerWide.Operations.Configuration;
using Raven.Server.Config;
using Raven.Tests.Core.Utils.Entities;
using Voron.Util;
using Xunit;
using Xunit.Abstractions;

namespace SlowTests.Issues
{
    public class RavenDB_14292 : RavenTestBase
    {
        public RavenDB_14292(ITestOutputHelper output) : base(output)
        {
        }

        [Theory]
        [InlineData(1)]
        public async Task ServerWideBackupShouldBackupIdleDatabase(int rounds)
        {
            const string fullBackupFrequency = "*/2 * * * *";
            var backupParser = CrontabSchedule.Parse(fullBackupFrequency);
            const int maxIdleTimeInSec = 10;

            using var server = GetNewServer(new ServerCreationOptions
            {
                CustomSettings = new Dictionary<string, string>
                {
                    [RavenConfiguration.GetKey(x => x.Databases.MaxIdleTime)] = $"{maxIdleTimeInSec}",
                    [RavenConfiguration.GetKey(x => x.Databases.FrequencyToCheckForIdle)] = "3",
                    [RavenConfiguration.GetKey(x => x.Core.RunInMemory)] = "false"
                }
            });
            using var dispose = new DisposableAction(() => server.ServerStore.DatabasesLandlord.SkipShouldContinueDisposeCheck = false);
            server.ServerStore.DatabasesLandlord.SkipShouldContinueDisposeCheck = true;

            var dbName = GetDatabaseName();
            var controlgroupDbName = GetDatabaseName() + "controlgroup";
            var baseBackupPath = NewDataPath(suffix: "BackupFolder");

            using var store = new DocumentStore { Database = dbName, Urls = new[] { server.WebUrl } }.Initialize();
            store.Maintenance.Server.Send(new CreateDatabaseOperation(new DatabaseRecord(dbName)));
            store.Maintenance.Server.Send(new CreateDatabaseOperation(new DatabaseRecord(controlgroupDbName)));
            await using var keepControlGroupAlive = new RepeatableAsyncAction(async token =>
            {
                await store.Maintenance.ForDatabase(controlgroupDbName).SendAsync(new GetStatisticsOperation(), token);
                await Task.Delay(TimeSpan.FromSeconds(maxIdleTimeInSec), token);
            }).Run();

            using (var session = store.OpenAsyncSession())
            {
                await session.StoreAsync(new User { Name = "EGOR" }, "su");
                await session.SaveChangesAsync();
            }
            using (var session = store.OpenAsyncSession(controlgroupDbName))
            {
                await session.StoreAsync(new User { Name = "egor" }, "susu");
                await session.SaveChangesAsync();
            }

            var first = true;
            DateTime backupStartTime = default;
            GetPeriodicBackupStatusOperation periodicBackupStatusOperation = null;
            PeriodicBackupStatus status = null;
            PeriodicBackupStatus controlGroupStatus = null;

            for (int i = 0; i < rounds; i++)
            {
                // let db get idle
                WaitForValue(() => server.ServerStore.IdleDatabases.Count > 0, true, 180 * 1000, 3000);

                Assert.True(1 == server.ServerStore.IdleDatabases.Count, $"IdleDatabasesCount({server.ServerStore.IdleDatabases.Count}), Round({i})");
                Assert.True(server.ServerStore.IdleDatabases.ContainsKey(store.Database), $"Round({i})");

                DateTime lastBackup;
                if (first)
                {
                    var putConfiguration = new ServerWideBackupConfiguration
                    {
                        FullBackupFrequency = fullBackupFrequency,
                        LocalSettings = new LocalSettings { FolderPath = baseBackupPath },
                    };
                    var result = await store.Maintenance.Server.SendAsync(new PutServerWideBackupConfigurationOperation(putConfiguration));
                    backupStartTime = DateTime.UtcNow;

                    var serverWideConfiguration = await store.Maintenance.Server.SendAsync(new GetServerWideBackupConfigurationOperation(result.Name));
                    Assert.NotNull(serverWideConfiguration);
                    periodicBackupStatusOperation = new GetPeriodicBackupStatusOperation(serverWideConfiguration.TaskId);

                    // the configuration is applied to existing databases
                    var periodicBackupConfigurations = (await store.Maintenance.Server.SendAsync(new GetDatabaseRecordOperation(store.Database))).PeriodicBackups;
                    Assert.Equal(1, periodicBackupConfigurations.Count);
                    var backupConfigurations = (await store.Maintenance.Server.SendAsync(new GetDatabaseRecordOperation(controlgroupDbName))).PeriodicBackups;
                    Assert.Equal(1, backupConfigurations.Count);

                    first = false;
                    lastBackup = backupStartTime;
                }
                else
                {
                    Assert.True(status.LastFullBackup.HasValue);
                    lastBackup = status.LastFullBackup.Value;
                }
                var nextBackup = backupParser.GetNextOccurrence(lastBackup);
                await Task.Delay(nextBackup - DateTime.UtcNow);

                status = await AssertWaitForNextBackup(store.Database, status);
                controlGroupStatus = await AssertWaitForNextBackup(controlgroupDbName, controlGroupStatus);
                async Task<PeriodicBackupStatus> AssertWaitForNextBackup(string db, PeriodicBackupStatus prevStatus)
                {
                    PeriodicBackupStatus nextStatus = null;
                    Assert.True(await WaitForValueAsync(async () =>
                    {
                        nextStatus = (await store.Maintenance.ForDatabase(db).SendAsync(periodicBackupStatusOperation)).Status;
                        if (nextStatus == null)
                            return false;
                        Assert.True(nextStatus.Error?.Exception == null, nextStatus.Error?.Exception);
                        Assert.True(nextStatus.LocalBackup?.Exception == null, nextStatus.LocalBackup?.Exception);

                        return prevStatus == null || nextStatus.LastOperationId.HasValue && nextStatus.LastOperationId > prevStatus.LastOperationId;
                    }, true), $"Round {i}");
                    return nextStatus;
                }

                var backupsDir = Directory.GetDirectories(baseBackupPath);
                Assert.Equal(2, backupsDir.Length);

                AssertBackupDirCount(controlgroupDbName, controlGroupStatus);
                AssertBackupDirCount(store.Database, status);
                void AssertBackupDirCount(string db, PeriodicBackupStatus periodicBackupStatus)
                {
                    var backupPath = Path.Combine(baseBackupPath, db);
                    var backupDirs = Directory.GetDirectories(backupPath);
                    Assert.True(i + 1 == backupDirs.Length,
                        $"firstBackupStartTime: {backupStartTime}, checkTime: {DateTime.UtcNow}, i: {i}, " +
                        $"controlGroupBackupNum: {backupDirs.Length}, path: {backupPath}, {PrintBackups(periodicBackupStatus, backupDirs)}");
                }
            }
        }

        private string PrintBackups(PeriodicBackupStatus pb, string[] dirs)
        {
            var sb = new StringBuilder();
            sb.AppendLine($"LocalBackup:{Environment.NewLine}Directory:{pb.LocalBackup.BackupDirectory}, Exception: {pb.LocalBackup.Exception}");
            sb.AppendLine($"Status:{Environment.NewLine}LastFullBackup: {pb.LastFullBackup}, FolderName: {pb.FolderName}, Exception:{pb.Error?.Exception}");
            sb.AppendLine($"Dirs:{Environment.NewLine}{string.Join(", ", dirs)}");
            return sb.ToString();
        }

        public class RepeatableAsyncAction : IAsyncDisposable
        {
            private readonly Func<CancellationToken, Task> _action;
            private readonly CancellationTokenSource _source = new CancellationTokenSource();
            private Task _task;

            public RepeatableAsyncAction(Func<CancellationToken, Task> action) => _action = action;

            public RepeatableAsyncAction Run()
            {
                _task = Task.Run(async () =>
                {
                    try
                    {
                        while (_source.Token.IsCancellationRequested == false)
                        {
                            await _action(_source.Token);
                        }
                    }
                    catch
                    {
                        /* ignored */
                    }
                });

                return this;
            }

            public async ValueTask DisposeAsync()
            {
                _source.Cancel();
                await _task;
            }
        }
    }
}
