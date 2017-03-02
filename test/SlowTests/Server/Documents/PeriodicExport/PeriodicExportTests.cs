using System;
using System.IO;
using System.Linq;
using System.Reflection;
using System.Threading;
using System.Threading.Tasks;
using FastTests;
using Raven.Client;
using Raven.Client.Documents.Smuggler;
using Raven.Server.Documents.PeriodicExport;
using Raven.Tests.Core.Utils.Entities;
using Xunit;

namespace SlowTests.Server.Documents.PeriodicExport
{
    public class PeriodicExportTestsSlow : RavenTestBase
    {
        private readonly string _exportPath;

        public PeriodicExportTestsSlow()
        {
            _exportPath = NewDataPath(suffix: "ExportFolder");
        }


        [Fact, Trait("Category", "Smuggler")]
        public async Task CanExportToDirectory_MultipleExports_with_long_interval()
        {
            using (var store = GetDocumentStore())
            {
                using (var session = store.OpenAsyncSession())
                {
                    await session.StoreAsync(new User { Name = "oren" });
                    await session.StoreAsync(new PeriodicExportConfiguration
                    {
                        Active = true,
                        LocalFolderName = _exportPath,
                        IntervalMilliseconds = 25
                    }, Constants.Documents.PeriodicExport.ConfigurationKey);

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

                using (var commands = store.Commands())
                {
                    SpinWait.SpinUntil(() =>
                    {
                        var jsonDocument = commands.Get(Constants.Documents.PeriodicExport.StatusKey);
                        if (jsonDocument == null)
                            return false;
                        var periodicExportStatus = (PeriodicExportStatus)store.Conventions.DeserializeEntityFromBlittable(typeof(PeriodicExportStatus), jsonDocument.BlittableJson);
                        return periodicExportStatus.LastDocsEtag > 0;
                    }, TimeSpan.FromSeconds(10));

                    dynamic statusDocument = await commands.GetAsync(Constants.Documents.PeriodicExport.StatusKey);
                    var etagForExports = statusDocument.Etag;
                    using (var session = store.OpenAsyncSession())
                    {
                        await session.StoreAsync(new User { Name = "ayende" });
                        await session.SaveChangesAsync();
                    }

                    SpinWait.SpinUntil(() =>
                    {
                        dynamic document = commands.Get(Constants.Documents.PeriodicExport.StatusKey);
                        return document.Etag != etagForExports;
                    }, 10000);
                }
            }

            using (var store = GetDocumentStore(dbSuffixIdentifier: "2"))
            {
                await store.Smuggler.ImportIncrementalAsync(new DatabaseSmugglerOptions(),
                    Directory.GetDirectories(_exportPath).First());
                using (var session = store.OpenAsyncSession())
                {
                    var users = await session.LoadAsync<User>(new[] { "users/1", "users/2" });
                    Assert.True(users.Any(x => x.Value.Name == "oren"));
                    Assert.True(users.Any(x => x.Value.Name == "ayende"));
                }
            }
        }

        [Fact, Trait("Category", "Smuggler")]
        public async Task PeriodicExport_should_work_with_long_intervals()
        {
            using (var store = GetDocumentStore())
            {
                using (var session = store.OpenAsyncSession())
                {
                    await session.StoreAsync(new User { Name = "oren" });
                    await session.StoreAsync(new PeriodicExportConfiguration
                    {
                        Active = true,
                        LocalFolderName = _exportPath,
                        IntervalMilliseconds = 25
                    }, Constants.Documents.PeriodicExport.ConfigurationKey);
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

                using (var commands = store.Commands())
                {
                    SpinWait.SpinUntil(() => commands.Get(Constants.Documents.PeriodicExport.StatusKey) != null, 10000);
                }
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


        [Fact, Trait("Category", "Smuggler")]
        public async Task CanExportToDirectory_MultipleExports()
        {
            using (var store = GetDocumentStore())
            {
                using (var session = store.OpenAsyncSession())
                {
                    await session.StoreAsync(new User { Name = "oren" });
                    await session.StoreAsync(new PeriodicExportConfiguration
                    {
                        Active = true,
                        LocalFolderName = _exportPath,
                        IntervalMilliseconds = 25
                    }, Constants.Documents.PeriodicExport.ConfigurationKey);

                    await session.SaveChangesAsync();
                }

                using (var commands = store.Commands())
                {
                    SpinWait.SpinUntil(() =>
                    {
                        var jsonDocument = commands.Get(Constants.Documents.PeriodicExport.StatusKey);
                        if (jsonDocument == null)
                            return false;
                        var periodicExportStatus = (PeriodicExportStatus)store.Conventions.DeserializeEntityFromBlittable(typeof(PeriodicExportStatus), jsonDocument.BlittableJson);
                        return periodicExportStatus.LastDocsEtag > 0;
                    }, TimeSpan.FromSeconds(10));


                    dynamic statusDocument = await commands.GetAsync(Constants.Documents.PeriodicExport.StatusKey);
                    var etagForExports = statusDocument.Etag;
                    using (var session = store.OpenAsyncSession())
                    {
                        await session.StoreAsync(new User { Name = "ayende" });
                        await session.SaveChangesAsync();
                    }

                    SpinWait.SpinUntil(() =>
                    {
                        dynamic document = commands.Get(Constants.Documents.PeriodicExport.StatusKey);
                        return document.Etag != etagForExports;
                    }, 10000);
                }
            }

            using (var store = GetDocumentStore(dbSuffixIdentifier: "2"))
            {
                await store.Smuggler.ImportIncrementalAsync(new DatabaseSmugglerOptions(),
                    Directory.GetDirectories(_exportPath).First());

                using (var session = store.OpenAsyncSession())
                {
                    var users = await session.LoadAsync<User>(new[] { "users/1", "users/2" });
                    Assert.True(users.Any(x => x.Value.Name == "oren"));
                    Assert.True(users.Any(x => x.Value.Name == "ayende"));
                }
            }
        }

    }
}