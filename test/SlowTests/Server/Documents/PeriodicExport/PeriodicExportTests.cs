using System;
using System.IO;
using System.Linq;
using System.Reflection;
using System.Threading;
using System.Threading.Tasks;
using FastTests;
using Raven.Client;
using Raven.Client.Documents.Smuggler;
using Raven.Client.Server.Operations;
using Raven.Client.Server.PeriodicExport;
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
                    var config = new PeriodicExportConfiguration
                    {
                        Active = true,
                        LocalFolderName = _exportPath,
                        IntervalMilliseconds = 25
                    };
                    await store.Admin.Server.SendAsync(new ConfigurePeriodicExportBundleOperation(config, store.DefaultDatabase));                    
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
                    var operation = new GetPeriodicExportStatusOperation(store.DefaultDatabase);
                        //await store.Admin.Server.SendAsync(new ConfigurePeriodicExportBundleOperation(config, store.DefaultDatabase));
                    SpinWait.SpinUntil(() =>
                    {
                        var result = store.Admin.Server.Send(operation);
                        if (result.Status == null)
                            return false;
                        return result.Status.LastDocsEtag > 0;
                    }, TimeSpan.FromSeconds(10));

                    var etagForExports = store.Admin.Server.Send(operation).Status.LastDocsEtag;
                    using (var session = store.OpenAsyncSession())
                    {
                        await session.StoreAsync(new User { Name = "ayende" });
                        await session.SaveChangesAsync();
                    }

                    SpinWait.SpinUntil(() =>
                    {
                        var newLastEtag = store.Admin.Server.Send(operation).Status.LastDocsEtag;
                        return newLastEtag != etagForExports;
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
                    var config = new PeriodicExportConfiguration
                    {
                        Active = true,
                        LocalFolderName = _exportPath,
                        IntervalMilliseconds = 25
                    };
                    var operation = new ConfigurePeriodicExportBundleOperation(config, store.DefaultDatabase);
                    await store.Admin.Server.SendAsync(operation);
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
                var getPeriodicBackupStatus = new GetPeriodicExportStatusOperation(store.DefaultDatabase);
                using (var commands = store.Commands())
                {
                    SpinWait.SpinUntil(() => store.Admin.Server.Send(getPeriodicBackupStatus).Status != null, 10000);
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
                    var config = new PeriodicExportConfiguration
                    {
                        Active = true,
                        LocalFolderName = _exportPath,
                        IntervalMilliseconds = 25
                    };
                    var operation = new ConfigurePeriodicExportBundleOperation(config, store.DefaultDatabase);
                    await store.Admin.Server.SendAsync(operation);
                    await session.SaveChangesAsync();
                }
                var getPeriodicBackupStatus = new GetPeriodicExportStatusOperation(store.DefaultDatabase);
                using (var commands = store.Commands())
                {
                    SpinWait.SpinUntil(() =>
                    {
                        var result = store.Admin.Server.Send(getPeriodicBackupStatus);
                        if (result.Status == null)
                            return false;                        
                        return result.Status.LastDocsEtag > 0;
                    }, TimeSpan.FromSeconds(10));


                    var res = await store.Admin.Server.SendAsync(getPeriodicBackupStatus);
                    var etagForExports = res.Status.LastDocsEtag;
                    using (var session = store.OpenAsyncSession())
                    {
                        await session.StoreAsync(new User { Name = "ayende" });
                        await session.SaveChangesAsync();
                    }

                    SpinWait.SpinUntil(() =>
                    {
                        res = store.Admin.Server.Send(getPeriodicBackupStatus);
                        return res.Status.LastDocsEtag != etagForExports;
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