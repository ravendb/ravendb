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
using Raven.Client.Smuggler;
using Raven.Server.Documents.PeriodicExport;
using Raven.Server.Smuggler;
using Raven.Server.Utils;
using Raven.Tests.Core.Utils.Entities;
using Xunit;

namespace FastTests.Server.Documents.PeriodicExport
{
    public class PeriodicExportTests : RavenTestBase
    {
        [Fact, Trait("Category", "Smuggler")]
        public async Task CanSetupPeriodicExportWithVeryLargePeriods()
        {
            var exportPath = NewDataPath(suffix: "ExportFolder");
            using (var store = await GetDocumentStore())
            {
                using (var session = store.OpenAsyncSession())
                {
                    await session.StoreAsync(new PeriodicExportConfiguration
                    {
                        Active = true,
                        LocalFolderName = exportPath,
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

        [Fact(Skip = "Implement server side import"), Trait("Category", "Smuggler")]
        public async Task PeriodicExport_should_work_with_long_intervals()
        {
            var exportPath = NewDataPath(suffix: "ExportFolder");
            using (var store = await GetDocumentStore())
            {
                using (var session = store.OpenAsyncSession())
                {
                    await session.StoreAsync(new PeriodicExportConfiguration
                    {
                        Active = true,
                        LocalFolderName = exportPath,
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

                using (var session = store.OpenAsyncSession())
                {
                    await session.StoreAsync(new User { Name = "oren" });
                    await session.SaveChangesAsync();
                }
                SpinWait.SpinUntil(() => store.AsyncDatabaseCommands.GetAsync(Constants.PeriodicExport.StatusDocumentKey).Result != null, 10000);
            }

            using (var store = await GetDocumentStore())
            {
                await store.Smuggler.ImportIncrementalAsync(new DatabaseSmugglerOptions(), exportPath);

            /*    var dataDumper = new DatabaseDataImporter(store.SystemDatabase) { Options = { Incremental = true } };
                dataDumper.ImportData(new SmugglerImportOptions<RavenConnectionStringOptions> { FromFile = exportPath }).Wait();
*/
                using (var session = store.OpenSession())
                {
                    Assert.Equal("oren", session.Load<User>(1).Name);
                }
            }
            IOExtensions.DeleteDirectory(exportPath);
        }

        /* [Fact, Trait("Category", "Smuggler")]
         public async Task CanExportToDirectory()
         {
             var exportPath = NewDataPath(suffix: "ExportFolder");
             using (var store = await GetDocumentStore())
             {
                 using (var session = store.OpenAsyncSession())
                 {
                     await session.StoreAsync(new User { Name = "oren" });
                     var periodicExportSetup = new PeriodicExportConfiguration
                     {
                         Active = true,
                         LocalFolderName = exportPath,
                         IntervalMilliseconds = 25
                     };
                     await session.StoreAsync(periodicExportSetup, Constants.PeriodicExport.ConfigurationDocumentKey);
                     await session.SaveChangesAsync();

                 }
                 SpinWait.SpinUntil(() => store.AsyncDatabaseCommands.GetAsync(Constants.PeriodicExport.StatusDocumentKey).Result != null, 10000);
             }

             using (var store = NewDocumentStore())
             {
                 var dataDumper = new DatabaseDataDumper(store.SystemDatabase) { Options = { Incremental = true } };
                 dataDumper.ImportData(new SmugglerImportOptions<RavenConnectionStringOptions> { FromFile = exportPath }).Wait();

                 using (var session = store.OpenSession())
                 {
                     Assert.Equal("oren", session.Load<User>(1).Name);
                 }
             }
             IOExtensions.DeleteDirectory(exportPath);
         }

         [Fact, Trait("Category", "Smuggler")]
         public async Task CanExportToDirectory_MultipleExports()
         {
             var exportPath = NewDataPath(suffix: "ExportFolder");
             using (var store = NewDocumentStore())
             {
                 using (var session = store.OpenSession())
                 {
                     session.StoreAsync(new User { Name = "oren" });
                     var periodicExportSetup = new PeriodicExportConfiguration
                     {
                         Active = true,
                         LocalFolderName = exportPath,
                         IntervalMilliseconds = 25
                     };
                     session.StoreAsync(periodicExportSetup, Constants.PeriodicExport.ConfigurationDocumentKey);

                     session.SaveChanges();

                 }
                 SpinWait.SpinUntil(() =>
                 {
                     var jsonDocument = store.DatabaseCommands.Get(Constants.PeriodicExport.StatusDocumentKey);
                     if (jsonDocument == null)
                         return false;
                     var periodicExportStatus = jsonDocument.DataAsJson.JsonDeserialization<PeriodicExportStatus>();
                     return periodicExportStatus.LastDocsEtag != Etag.Empty && periodicExportStatus.LastDocsEtag != null;
                 });

                 var etagForExports= store.DatabaseCommands.Get(Constants.PeriodicExport.StatusDocumentKey).Etag;
                 using (var session = store.OpenSession())
                 {
                     session.StoreAsync(new User { Name = "ayende" });
                     session.SaveChanges();
                 }
                 SpinWait.SpinUntil(() =>
                      store.DatabaseCommands.Get(Constants.PeriodicExport.StatusDocumentKey).Etag != etagForExports);

             }

             using (var store = NewDocumentStore())
             {
             var exportPath = NewDataPath(suffix: "ExportFolder");
                 var dataDumper = new DatabaseDataDumper(store.SystemDatabase) { Options = {Incremental = true}};
                 dataDumper.ImportData(new SmugglerImportOptions<RavenConnectionStringOptions> { FromFile = exportPath }).Wait();

                 using (var session = store.OpenSession())
                 {
                     Assert.Equal("oren", session.Load<User>(1).Name);
                     Assert.Equal("ayende", session.Load<User>(2).Name);
                 }
             }
             IOExtensions.DeleteDirectory(exportPath);
         }

         [Fact, Trait("Category", "Smuggler")]
         public async Task CanExportToDirectory_MultipleExports_with_long_interval()
         {
             var exportPath = NewDataPath("ExportFolder");
             using (var store = NewDocumentStore())
             {
                 var periodicExportTask = store.DocumentDatabase.StartupTasks.OfType<PeriodicExportTask>().FirstOrDefault();

                 //get by reflection the maxTimerTimeoutInMilliseconds field
                 //this field is the maximum interval acceptable in .Net's threading timer
                 //if the requested export interval is bigger than this maximum interval, 
                 //a timer with maximum interval will be used several times until the interval cumulatively
                 //will be equal to requested interval
                 var maxTimerTimeoutInMillisecondsField = typeof(PeriodicExportTask)
                         .GetField("maxTimerTimeoutInMilliseconds",
                             BindingFlags.Instance | BindingFlags.NonPublic);

                 Assert.NotNull(maxTimerTimeoutInMillisecondsField); //sanity check, can fail here only in case of source code change
                 //that removes this field
                 maxTimerTimeoutInMillisecondsField.SetValue(periodicExportTask, 5);

                 using (var session = store.OpenSession())
                 {
                     session.StoreAsync(new User { Name = "oren" });
                     var periodicExportSetup = new PeriodicExportConfiguration
                     {
                         Active = true,
                         LocalFolderName = exportPath,
                         IntervalMilliseconds = 25
                     };
                     session.StoreAsync(periodicExportSetup, Constants.PeriodicExport.ConfigurationDocumentKey);

                     session.SaveChanges();

                 }
                 SpinWait.SpinUntil(() =>
                 {
                     var jsonDocument = store.DatabaseCommands.Get(Constants.PeriodicExport.StatusDocumentKey);
                     if (jsonDocument == null)
                         return false;
                     var periodicExportStatus = jsonDocument.DataAsJson.JsonDeserialization<PeriodicExportStatus>();
                     return periodicExportStatus.LastDocsEtag != Etag.Empty && periodicExportStatus.LastDocsEtag != null;
                 });

                 var etagForExports = store.DatabaseCommands.Get(Constants.PeriodicExport.StatusDocumentKey).Etag;
                 using (var session = store.OpenSession())
                 {
                     session.StoreAsync(new User { Name = "ayende" });
                     session.SaveChanges();
                 }
                 SpinWait.SpinUntil(() =>
                      store.DatabaseCommands.Get(Constants.PeriodicExport.StatusDocumentKey).Etag != etagForExports);

             }

             using (var store = NewDocumentStore())
             {
                 var dataDumper = new DatabaseDataDumper(store.SystemDatabase) { Options = { Incremental = true } };
                 dataDumper.ImportData(new SmugglerImportOptions<RavenConnectionStringOptions> { FromFile = exportPath }).Wait();

                 using (var session = store.OpenSession())
                 {
                     Assert.Equal("oren", session.Load<User>(1).Name);
                     Assert.Equal("ayende", session.Load<User>(2).Name);
                 }
             }
             IOExtensions.DeleteDirectory(exportPath);
         }*/
    }
}