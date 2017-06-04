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
using Raven.Client.Server.Operations;
using Raven.Client.Server.PeriodicBackup;
using Raven.Tests.Core.Utils.Entities;
using Xunit;

namespace FastTests.Server.Documents.PeriodicBackup
{
    public class PeriodicBackupTests : RavenTestBase
    {
        private readonly string _backupPath;

        public PeriodicBackupTests()
        {
            _backupPath = NewDataPath(suffix: "BackupFolder");
        }

        [Fact, Trait("Category", "Smuggler")]
        public async Task CanSetupPeriodicBackupWithVeryLargePeriods()
        {
            using (var store = GetDocumentStore())
            {
                var config = new PeriodicBackupConfiguration
                {
                    LocalSettings = new LocalSettings
                    {
                        FolderPath = _backupPath
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

        [Fact, Trait("Category", "Smuggler")]
        public async Task CanBackupToDirectory()
        {
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
                        FolderPath = _backupPath
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
                    Directory.GetDirectories(_backupPath).First());

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