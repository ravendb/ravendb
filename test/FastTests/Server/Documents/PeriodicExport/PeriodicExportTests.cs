// -----------------------------------------------------------------------
//  <copyright file="PeriodicExportTests.cs" company="Hibernating Rhinos LTD">
//      Copyright (c) Hibernating Rhinos LTD. All rights reserved.
//  </copyright>
// -----------------------------------------------------------------------

using System;
using System.Reflection;
using System.Threading;
using System.Threading.Tasks;
using Raven.Abstractions.Data;
using Raven.Abstractions.Extensions;
using Raven.Client.Smuggler;
using Raven.Server.Documents.PeriodicExport;
using Raven.Server.Utils;
using Raven.Tests.Core.Utils.Entities;
using Xunit;

namespace FastTests.Server.Documents.PeriodicExport
{
    public class PeriodicExportTests : RavenTestBase
    {
        private readonly string _exportPath;

        public PeriodicExportTests()
        {
            _exportPath = NewDataPath(suffix: "ExportFolder");
        }

        public override void Dispose()
        {
            IOExtensions.DeleteDirectory(_exportPath);
            base.Dispose();
        }

        [Fact, Trait("Category", "Smuggler")]
        public async Task CanSetupPeriodicExportWithVeryLargePeriods()
        {
            using (var store = await GetDocumentStore())
            {
                using (var session = store.OpenAsyncSession())
                {
                    await session.StoreAsync(new PeriodicExportConfiguration
                    {
                        Active = true,
                        LocalFolderName = _exportPath,
                        FullExportIntervalMilliseconds = (long)TimeSpan.FromDays(50).TotalMilliseconds,
                        IntervalMilliseconds = (long)TimeSpan.FromDays(50).TotalMilliseconds
                    }, Constants.PeriodicExport.ConfigurationDocumentKey);
                    await session.SaveChangesAsync();
                }

                var periodicExportRunner = (await GetDocumentDatabaseInstanceFor(store)).BundleLoader.PeriodicExportRunner;
                Assert.Equal(50, periodicExportRunner.IncrementalInterval.TotalDays);
                Assert.Equal(50, periodicExportRunner.FullExportInterval.TotalDays);
            }
        }

        [Fact, Trait("Category", "Smuggler")]
        public async Task PeriodicExport_should_work_with_long_intervals()
        {
            using (var store = await GetDocumentStore())
            {
                using (var session = store.OpenAsyncSession())
                {
                    await session.StoreAsync(new User { Name = "oren" });
                    await session.StoreAsync(new PeriodicExportConfiguration
                    {
                        Active = true,
                        LocalFolderName = _exportPath,
                        IntervalMilliseconds = 25
                    }, Constants.PeriodicExport.ConfigurationDocumentKey);
                    await session.SaveChangesAsync();
                }

                var periodicExportRunner = (await GetDocumentDatabaseInstanceFor(store)).BundleLoader.PeriodicExportRunner;

                //get by reflection the maxTimerTimeoutInMilliseconds field
                //this field is the maximum interval acceptable in .Net's threading timer
                //if the requested export interval is bigger than this maximum interval, 
                //a timer with maximum interval will be used several times until the interval cumulatively
                //will be equal to requested interval
                typeof(PeriodicExportRunner)
                    .GetField(nameof(PeriodicExportRunner.MaxTimerTimeout), BindingFlags.Instance | BindingFlags.Public)
                    .SetValue(periodicExportRunner, TimeSpan.FromMilliseconds(5));

                SpinWait.SpinUntil(() => store.AsyncDatabaseCommands.GetAsync(Constants.PeriodicExport.StatusDocumentKey).Result != null, 10000);
            }

            using (var store = await GetDocumentStore(dbSuffixIdentifier: "2"))
            {
                await store.Smuggler.ImportIncrementalAsync(new DatabaseSmugglerOptions(), _exportPath);
                using (var session = store.OpenAsyncSession())
                {
                    var user = await session.LoadAsync<User>(1);
                    Assert.Equal("oren", user.Name);
                }
            }
        }

        [Fact, Trait("Category", "Smuggler")]
        public async Task CanExportToDirectory()
        {
            using (var store = await GetDocumentStore())
            {
                using (var session = store.OpenAsyncSession())
                {
                    await session.StoreAsync(new User {Name = "oren"});
                    await session.StoreAsync(new PeriodicExportConfiguration
                    {
                        Active = true,
                        LocalFolderName = _exportPath,
                        IntervalMilliseconds = 25
                    }, Constants.PeriodicExport.ConfigurationDocumentKey);
                    await session.SaveChangesAsync();

                }
                SpinWait.SpinUntil(() => store.AsyncDatabaseCommands.GetAsync(Constants.PeriodicExport.StatusDocumentKey).Result != null, 10000);
            }

            using (var store = await GetDocumentStore(dbSuffixIdentifier: "2"))
            {
                await store.Smuggler.ImportIncrementalAsync(new DatabaseSmugglerOptions(), _exportPath);

                using (var session = store.OpenAsyncSession())
                {
                    var user = await session.LoadAsync<User>(1);
                    Assert.Equal("oren", user.Name);
                }
            }
        }

        [Fact, Trait("Category", "Smuggler")]
        public async Task CanExportToDirectory_MultipleExports()
        {
            using (var store = await GetDocumentStore())
            {
                using (var session = store.OpenAsyncSession())
                {
                    await session.StoreAsync(new User {Name = "oren"});
                    await session.StoreAsync(new PeriodicExportConfiguration
                    {
                        Active = true,
                        LocalFolderName = _exportPath,
                        IntervalMilliseconds = 25
                    }, Constants.PeriodicExport.ConfigurationDocumentKey);

                    await session.SaveChangesAsync();

                }
                SpinWait.SpinUntil(() =>
                {
                    var jsonDocument = store.DatabaseCommands.Get(Constants.PeriodicExport.StatusDocumentKey);
                    if (jsonDocument == null)
                        return false;
                    var periodicExportStatus = jsonDocument.DataAsJson.JsonDeserialization<PeriodicExportStatus>();
                    return periodicExportStatus.LastDocsEtag > 0;
                });

                var statusDocument = await store.AsyncDatabaseCommands.GetAsync(Constants.PeriodicExport.StatusDocumentKey);
                var etagForExports = statusDocument.Etag;
                using (var session = store.OpenAsyncSession())
                {
                    await session.StoreAsync(new User {Name = "ayende"});
                    await session.SaveChangesAsync();
                }
                SpinWait.SpinUntil(() => store.DatabaseCommands.Get(Constants.PeriodicExport.StatusDocumentKey).Etag != etagForExports);
            }

            using (var store = await GetDocumentStore(dbSuffixIdentifier: "2"))
            {
                await store.Smuggler.ImportIncrementalAsync(new DatabaseSmugglerOptions(), _exportPath);

                using (var session = store.OpenAsyncSession())
                {
                    var users = await session.LoadAsync<User>(new ValueType[] {1, 2});
                    Assert.Equal("oren", users[0].Name);
                    Assert.Equal("ayende", users[1].Name);
                }
            }
        }

        [Fact, Trait("Category", "Smuggler")]
        public async Task CanExportToDirectory_MultipleExports_with_long_interval()
        {
            using (var store = await GetDocumentStore())
            {
                using (var session = store.OpenAsyncSession())
                {
                    await session.StoreAsync(new User { Name = "oren" });
                    await session.StoreAsync(new PeriodicExportConfiguration
                    {
                        Active = true,
                        LocalFolderName = _exportPath,
                        IntervalMilliseconds = 25
                    }, Constants.PeriodicExport.ConfigurationDocumentKey);

                    await session.SaveChangesAsync();
                }

                var periodicExportRunner = (await GetDocumentDatabaseInstanceFor(store)).BundleLoader.PeriodicExportRunner;

                //get by reflection the maxTimerTimeoutInMilliseconds field
                //this field is the maximum interval acceptable in .Net's threading timer
                //if the requested export interval is bigger than this maximum interval, 
                //a timer with maximum interval will be used several times until the interval cumulatively
                //will be equal to requested interval
                typeof(PeriodicExportRunner)
                    .GetField(nameof(PeriodicExportRunner.MaxTimerTimeout), BindingFlags.Instance | BindingFlags.Public)
                    .SetValue(periodicExportRunner, TimeSpan.FromMilliseconds(5));

                SpinWait.SpinUntil(() =>
                {
                    var jsonDocument = store.DatabaseCommands.Get(Constants.PeriodicExport.StatusDocumentKey);
                    if (jsonDocument == null)
                        return false;
                    var periodicExportStatus = jsonDocument.DataAsJson.JsonDeserialization<PeriodicExportStatus>();
                    return periodicExportStatus.LastDocsEtag > 0;
                });

                var statusDocument = await store.AsyncDatabaseCommands.GetAsync(Constants.PeriodicExport.StatusDocumentKey);
                var etagForExports = statusDocument.Etag;
                using (var session = store.OpenAsyncSession())
                {
                    await session.StoreAsync(new User {Name = "ayende"});
                    await session.SaveChangesAsync();
                }
                SpinWait.SpinUntil(() => store.DatabaseCommands.Get(Constants.PeriodicExport.StatusDocumentKey).Etag != etagForExports);
            }

            using (var store = await GetDocumentStore(dbSuffixIdentifier: "2"))
            {
                await store.Smuggler.ImportIncrementalAsync(new DatabaseSmugglerOptions(), _exportPath);
                using (var session = store.OpenAsyncSession())
                {
                    var users = await session.LoadAsync<User>(new ValueType[] { 1, 2 });
                    Assert.Equal("oren", users[0].Name);
                    Assert.Equal("ayende", users[1].Name);
                }
            }
        }
    }
}