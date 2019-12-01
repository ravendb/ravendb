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

        [Fact]
        public async Task ShouldWork()
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
            var backupPath = NewDataPath(suffix: "BackupFolder");

            using var store = GetDocumentStore(new Options { Server = server, RunInMemory = false });
            var dbName = store.Database;
            store.Maintenance.Server.Send(new CreateDatabaseOperation(new DatabaseRecord("Test")));

            using (var session = store.OpenAsyncSession())
            {
                await session.StoreAsync(new User { Name = "EGOR" }, "su");
                await session.SaveChangesAsync();
            }
            using (var session = store.OpenAsyncSession("Test"))
            {
                await session.StoreAsync(new User { Name = "egor" }, "susu");
                await session.StoreAsync(new User { Name = "egor2" }, "sususu");
                await session.SaveChangesAsync();
            }

            var now = DateTime.Now;
            var nextNow = now + TimeSpan.FromSeconds(60);
            while (now < nextNow && server.ServerStore.IdleDatabases.Count < 1)
            {
                Thread.Sleep(3000);
                var x = await store.Maintenance.ForDatabase("Test").SendAsync(new GetStatisticsOperation());
                now = DateTime.Now;
            }

            Assert.Equal(1, server.ServerStore.IdleDatabases.Count);

            var putConfiguration = new ServerWideBackupConfiguration
            {
                FullBackupFrequency = "*/2 * * * *",
                LocalSettings = new LocalSettings { FolderPath = backupPath },
            };

            var result = await store.Maintenance.Server.SendAsync(new PutServerWideBackupConfigurationOperation(putConfiguration));
            var serverWideConfiguration = await store.Maintenance.Server.SendAsync(new GetServerWideBackupConfigurationOperation(result.Name));
            Assert.NotNull(serverWideConfiguration);
            Assert.Equal(1, server.ServerStore.IdleDatabases.Count);

            // the configuration is applied to existing databases
            var record1 = await store.Maintenance.Server.SendAsync(new GetDatabaseRecordOperation(store.Database));
            var backups1 = record1.PeriodicBackups;
            Assert.Equal(1, backups1.Count);
            var backupTaskId = backups1.First().TaskId;


            nextNow = DateTime.Now + TimeSpan.FromSeconds(122);
            while (now < nextNow && server.ServerStore.IdleDatabases.Count > 0)
            {
                Thread.Sleep(2000);
                store.Maintenance.ForDatabase("Test").Send(new GetStatisticsOperation());
                now = DateTime.Now;
            }

            Assert.Equal(0, server.ServerStore.IdleDatabases.Count);

            var operation = new GetPeriodicBackupStatusOperation(backupTaskId);
            var value = WaitForValue(() =>
            {
                var status = store.Maintenance.Send(operation).Status;
                return status?.LastEtag;
            }, 1);

            Assert.Equal(1, value);
            Assert.Equal(2, Directory.GetDirectories(backupPath).Length);
            Assert.Equal(1, Directory.GetDirectories(Path.Combine(backupPath, "Test")).Length);
            Assert.Equal(1, Directory.GetDirectories(Path.Combine(backupPath, dbName)).Length);
        }
    }
}
