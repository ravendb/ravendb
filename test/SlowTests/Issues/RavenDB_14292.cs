using System;
using System.Collections.Generic;
using System.IO;
using System.Linq;
using System.Text;
using System.Threading.Tasks;
using FastTests;
using Raven.Client.Documents.Operations;
using Raven.Client.Documents.Operations.Backups;
using Raven.Client.ServerWide;
using Raven.Client.ServerWide.Operations;
using Raven.Client.ServerWide.Operations.Configuration;
using Raven.Server.Config;
using Raven.Tests.Core.Utils.Entities;
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
            using var server = GetNewServer(new ServerCreationOptions
            {
                CustomSettings = new Dictionary<string, string>
                {
                    [RavenConfiguration.GetKey(x => x.Databases.MaxIdleTime)] = "10",
                    [RavenConfiguration.GetKey(x => x.Databases.FrequencyToCheckForIdle)] = "3",
                    [RavenConfiguration.GetKey(x => x.Core.RunInMemory)] = "false"
                }
            });
            var testDatabaseName = GetDatabaseName();
            try
            {
                var backupPath = NewDataPath(suffix: "BackupFolder");

                server.ServerStore.DatabasesLandlord.SkipShouldContinueDisposeCheck = true;

                using var store = GetDocumentStore(new Options { Server = server, RunInMemory = false });
                var dbName = store.Database;

                store.Maintenance.Server.Send(new CreateDatabaseOperation(new DatabaseRecord(testDatabaseName)));

                using (var session = store.OpenAsyncSession())
                {
                    await session.StoreAsync(new User { Name = "EGOR" }, "su");
                    await session.SaveChangesAsync();
                }
                using (var session = store.OpenAsyncSession(testDatabaseName))
                {
                    await session.StoreAsync(new User { Name = "egor" }, "susu");
                    await session.StoreAsync(new User { Name = "egor2" }, "sususu");
                    await session.SaveChangesAsync();
                }

                var first = true;
                long backupTaskId = 0;
                long opId1 = 0;
                long opId2 = 0;
                DateTime firstBackupStartTime = default;
                for (int i = 0; i < rounds; i++)
                {
                    // let db get idle
                    var now = DateTime.Now;
                    var nextNow = now + TimeSpan.FromSeconds(60);
                    while (now < nextNow && server.ServerStore.IdleDatabases.Count < 1)
                    {
                        await Task.Delay(3000);
                        await store.Maintenance.ForDatabase(testDatabaseName).SendAsync(new GetStatisticsOperation());
                        now = DateTime.Now;
                    }

                    Assert.True(1 == server.ServerStore.IdleDatabases.Count,
                        $"1 == server.ServerStore.IdleDatabases.Count({server.ServerStore.IdleDatabases.Count}), finishedOnTime? {now < nextNow}, now = {now}, nextNow = {nextNow}");
                    Assert.True(server.ServerStore.IdleDatabases.ContainsKey(dbName));

                    if (first)
                    {
                        firstBackupStartTime = DateTime.UtcNow;
                        var putConfiguration = new ServerWideBackupConfiguration
                        {
                            FullBackupFrequency = "*/2 * * * *",
                            LocalSettings = new LocalSettings { FolderPath = backupPath },
                        };

                        var result = await store.Maintenance.Server.SendAsync(new PutServerWideBackupConfigurationOperation(putConfiguration));
                        var serverWideConfiguration = await store.Maintenance.Server.SendAsync(new GetServerWideBackupConfigurationOperation(result.Name));
                        Assert.NotNull(serverWideConfiguration);

                        // the configuration is applied to existing databases
                        var record1 = await store.Maintenance.Server.SendAsync(new GetDatabaseRecordOperation(store.Database));
                        var backups1 = record1.PeriodicBackups;
                        Assert.Equal(1, backups1.Count);
                        backupTaskId = backups1.First().TaskId;

                        first = false;
                    }

                    //Wait for backup occurrence
                    nextNow = DateTime.Now + TimeSpan.FromSeconds(122);
                    while (now < nextNow && server.ServerStore.IdleDatabases.Count > 0)
                    {
                        await Task.Delay(2000);
                        store.Maintenance.ForDatabase(testDatabaseName).Send(new GetStatisticsOperation());
                        now = DateTime.Now;
                    }

                    Assert.True(0 == server.ServerStore.IdleDatabases.Count,
                        $"0 == server.ServerStore.IdleDatabases.Count({server.ServerStore.IdleDatabases.Count}), finishedOnTime? {now < nextNow}, now = {now}, nextNow = {nextNow}");

                    var operation = new GetPeriodicBackupStatusOperation(backupTaskId);
                    PeriodicBackupStatus status1 = null;
                    Assert.True(WaitForValue(() =>
                    {
                        status1 = store.Maintenance.ForDatabase(dbName).Send(operation).Status;

                        if (status1?.LastOperationId != null && status1.LastOperationId.Value > opId1)
                        {
                            opId1 = status1.LastOperationId.Value;
                            return true;
                        }

                        return false;

                    }, true));
                    PeriodicBackupStatus status2 = null;
                    Assert.True(WaitForValue(() =>
                    {
                        status2 = store.Maintenance.ForDatabase(testDatabaseName).Send(operation).Status;

                        if (status2?.LastOperationId != null && status2.LastOperationId.Value > opId2)
                        {
                            opId2 = status2.LastOperationId.Value;
                            return true;
                        }

                        return false;

                    }, true));

                    var backupsDir = Directory.GetDirectories(backupPath);
                    var backupsNum = backupsDir.Length;

                    var testDatabaseBackupPath = Path.Combine(backupPath, testDatabaseName);
                    var testDatabaseBackupDirs = Directory.GetDirectories(testDatabaseBackupPath);
                    var testDatabaseBackupNum = testDatabaseBackupDirs.Length;

                    var dbNameBackupPath = Path.Combine(backupPath, dbName);
                    var dbNameBackupDirs = Directory.GetDirectories(dbNameBackupPath);
                    var dbNameBackupNum = dbNameBackupDirs.Length;

                    Assert.Equal(2, backupsNum);
                    Assert.True(i + 1 == testDatabaseBackupNum,
                        $"firstBackupStartTime: {firstBackupStartTime}, i: {i}, testDatabaseBackupNum: {testDatabaseBackupNum}, path: {testDatabaseBackupPath}, {PrintBackups(status1, testDatabaseBackupDirs)}");
                    Assert.True(i + 1 == dbNameBackupNum,
                        $"firstBackupStartTime: {firstBackupStartTime}, i: {i}, dbNameBackupNum: {dbNameBackupNum}, path: {dbNameBackupPath}, {PrintBackups(status2, dbNameBackupDirs)}");
                }
            }
            finally
            {
                server.ServerStore.DatabasesLandlord.SkipShouldContinueDisposeCheck = false;
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
    }
}
