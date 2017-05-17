// -----------------------------------------------------------------------
//  <copyright file="PeriodicExportTests.cs" company="Hibernating Rhinos LTD">
//      Copyright (c) Hibernating Rhinos LTD. All rights reserved.
//  </copyright>
// -----------------------------------------------------------------------

using System;
using System.IO;
using System.Threading;
using System.Threading.Tasks;

using Xunit;
using System.Linq;
using Raven.Client.Documents.Smuggler;
using Raven.Client.Server.Operations;
using Raven.Client.Server.PeriodicBackup;
using Raven.Tests.Core.Utils.Entities;

namespace FastTests.Server.Documents.PeriodicExport
{
    public class PeriodicExportTests : RavenTestBase
    {
        private readonly string _exportPath;

        public PeriodicExportTests()
        {
            _exportPath = NewDataPath(suffix: "ExportFolder");
        }

        [Fact, Trait("Category", "Smuggler")]
        public async Task CanSetupPeriodicExportWithVeryLargePeriods()
        {
            using (var store = GetDocumentStore())
            {
                var config = new PeriodicBackupConfiguration
                {
                    LocalSettings = new LocalSettings
                    {
                        FolderPath = _exportPath
                    },
                    //TODO: FullBackupFrequency = 
                    //FullBackupIntervalInMilliseconds = (long)TimeSpan.FromDays(50).TotalMilliseconds,
                    //IncrementalIntervalInMilliseconds = (long)TimeSpan.FromDays(50).TotalMilliseconds
                };
                await store.Admin.Server.SendAsync(new ConfigurePeriodicBackupOperation(config, store.Database));

                var periodicExportRunner = (await GetDocumentDatabaseInstanceFor(store)).BundleLoader.PeriodicBackupRunner;
                Assert.Equal(50, periodicExportRunner.IncrementalInterval.TotalDays);
                Assert.Equal(50, periodicExportRunner.FullExportInterval.TotalDays);
            }
        }

        [Fact, Trait("Category", "Smuggler")]
        public async Task CanExportToDirectory()
        {
            using (var store = GetDocumentStore())
            {
                using (var session = store.OpenAsyncSession())
                {
                    await session.StoreAsync(new User { Name = "oren" });
                    var config = new PeriodicBackupConfiguration
                    {
                        LocalSettings = new LocalSettings
                        {
                            FolderPath = _exportPath
                        },
                        //TODO: FullBackupFrequency = 
                        //IncrementalIntervalInMilliseconds = 25
                    };
                    await store.Admin.Server.SendAsync(new ConfigurePeriodicBackupOperation(config, store.Database));
                    await session.SaveChangesAsync();

                }
                var operation = new GetPeriodicBackupStatusOperation();
                SpinWait.SpinUntil(() => store.Admin.Server.Send(operation).Status != null, 10000);

            }

            using (var store = GetDocumentStore(dbSuffixIdentifier: "2"))
            {
                await store.Smuggler.ImportIncrementalAsync(new DatabaseSmugglerOptions(),
                    Directory.GetDirectories(_exportPath).First());

                using (var session = store.OpenAsyncSession())
                {
                    var user = await session.LoadAsync<User>("users/1");
                    Assert.Equal("oren", user.Name);
                }
            }
        }
    }
}