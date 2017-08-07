// -----------------------------------------------------------------------
//  <copyright file="PeriodicBackupTests.cs" company="Hibernating Rhinos LTD">
//      Copyright (c) Hibernating Rhinos LTD. All rights reserved.
//  </copyright>
// -----------------------------------------------------------------------

using System;
using System.IO;
using System.Linq;
using System.Threading;
using System.Threading.Tasks;
using Raven.Client.Documents.Smuggler;
using Raven.Client.ServerWide.Operations;
using Raven.Client.ServerWide.PeriodicBackup;
using Raven.Tests.Core.Utils.Entities;
using Xunit;

namespace FastTests.Server.Documents.PeriodicBackup
{
    public class PeriodicBackupTests : RavenTestBase
    {
        [Fact, Trait("Category", "Smuggler")]
        public async Task CanSetupPeriodicBackupWithVeryLargePeriods()
        {
            var backupPath = NewDataPath(suffix: "BackupFolder");
            using (var store = GetDocumentStore())
            {
                var config = new PeriodicBackupConfiguration
                {
                    LocalSettings = new LocalSettings
                    {
                        FolderPath = backupPath
                    },
                    FullBackupFrequency = "* */1 * * *",
                    IncrementalBackupFrequency = "* */2 * * *"
                };

                await store.Admin.Server.SendAsync(new UpdatePeriodicBackupOperation(config, store.Database));

                var periodicBackupRunner = (await GetDocumentDatabaseInstanceFor(store)).PeriodicBackupRunner;
                var backups = periodicBackupRunner.PeriodicBackups;
                Assert.Equal("* */1 * * *", backups.First().Configuration.FullBackupFrequency);
                Assert.Equal("* */2 * * *", backups.First().Configuration.IncrementalBackupFrequency);
            }
        }

        [Fact(Skip = "RavenDB-7931 Takes too long"), Trait("Category", "Smuggler")]
        public async Task CanBackupToDirectory()
        {
            var backupPath = NewDataPath(suffix: "BackupFolder");
            using (var store = GetDocumentStore())
            {
                using (var session = store.OpenAsyncSession())
                {
                    await session.StoreAsync(new User { Name = "oren" }, "users/1");
                    await session.SaveChangesAsync();
                }

                var config = new PeriodicBackupConfiguration
                {
                    LocalSettings = new LocalSettings
                    {
                        FolderPath = backupPath
                    },
                    IncrementalBackupFrequency = "* * * * *" //every minute
                };
                var operation = new UpdatePeriodicBackupOperation(config, store.Database);
                var result = await store.Admin.Server.SendAsync(operation);
                var periodicBackupTaskId = result.TaskId;

                var getPeriodicBackupStatus = new GetPeriodicBackupStatusOperation(store.Database, periodicBackupTaskId);
                SpinWait.SpinUntil(() => store.Admin.Server.Send(getPeriodicBackupStatus).Status?.LastFullBackup != null, TimeSpan.FromSeconds(60));
            }

            using (var store = GetDocumentStore(dbSuffixIdentifier: "2"))
            {
                await store.Smuggler.ImportIncrementalAsync(new DatabaseSmugglerOptions(),
                    Directory.GetDirectories(backupPath).First());

                using (var session = store.OpenAsyncSession())
                {
                    var user = await session.LoadAsync<User>("users/1");
                    Assert.NotNull(user);
                    Assert.Equal("oren", user.Name);
                }
            }
        }
    }
}