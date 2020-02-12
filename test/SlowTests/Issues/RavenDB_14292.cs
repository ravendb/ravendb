using System;
using System.Collections.Generic;
using System.IO;
using System.Linq;
using System.Threading;
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

                    if (first)
                    {
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
                    var value = WaitForValue(() =>
                    {
                        var status = store.Maintenance.Send(operation).Status;
                        return status?.LastEtag;
                    }, 1);
                    Assert.Equal(1, value);

                    Assert.True(2 == Directory.GetDirectories(backupPath).Length, $"2 == Directory.GetDirectories(backupPath).Length({Directory.GetDirectories(backupPath).Length})");
                    Assert.True(i + 1 == Directory.GetDirectories(Path.Combine(backupPath, testDatabaseName)).Length, $"i + 1 == Directory.GetDirectories(Path.Combine(backupPath, '{testDatabaseName}')).Length({Directory.GetDirectories(Path.Combine(backupPath, testDatabaseName)).Length})");
                    Assert.True(i + 1 == Directory.GetDirectories(Path.Combine(backupPath, dbName)).Length, $"i + 1 == Directory.GetDirectories(Path.Combine(backupPath, dbName)).Length({Directory.GetDirectories(Path.Combine(backupPath, dbName)).Length})");
                }
            }
            finally
            {
                server.ServerStore.DatabasesLandlord.SkipShouldContinueDisposeCheck = false;
            }
        }
    }
}
