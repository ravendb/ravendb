// -----------------------------------------------------------------------
//  <copyright file="SmugglerWithDisabledVersioning_RavenDB_3219.cs" company="Hibernating Rhinos LTD">
//      Copyright (c) Hibernating Rhinos LTD. All rights reserved.
//  </copyright>
// -----------------------------------------------------------------------
using System.IO;
using System.Threading.Tasks;
using Raven.Abstractions.Data;
using Raven.Abstractions.Database.Smuggler.FileSystem;
using Raven.Bundles.Versioning.Data;
using Raven.Client.FileSystem.Bundles.Versioning;
using Raven.Smuggler.FileSystem;
using Raven.Smuggler.FileSystem.Files;
using Raven.Smuggler.FileSystem.Remote;
using Raven.Tests.Helpers;
using Xunit;
using VersioningUtil = Raven.Database.FileSystem.Bundles.Versioning.VersioningUtil;

namespace Raven.Tests.FileSystem.Smuggler
{
    public class SmugglerWithDisabledVersioning_RavenDB_3219 : RavenFilesTestBase
    {
        [Fact, Trait("Category", "Smuggler")]
        public async Task CanDisableVersioningDuringImport()
        {
            long fileCount;
            string export = Path.Combine(NewDataPath("export_3219"), "Export");

            using (var store = NewStore())
            {
                for (int i = 0; i < 10; i++)
                {
                    await store.AsyncFilesCommands.UploadAsync("test-" + i, StringToStream("hello"));
                }
                
                var smuggler = new FileSystemSmuggler(new FileSystemSmugglerOptions());
                await smuggler.ExecuteAsync(
                    new RemoteSmugglingSource(new FilesConnectionStringOptions
                    {
                        Url = store.Url,
                        DefaultFileSystem = store.DefaultFileSystem
                    }), 
                    new FileSmugglingDestination(export, false));

                fileCount = (await store.AsyncFilesCommands.GetStatisticsAsync()).FileCount;
            }

            using (var store = NewStore(activeBundles: "Versioning", fileSystemName: "Import", index: 1))
            {
                await store.AsyncFilesCommands.Configuration.SetKeyAsync(VersioningUtil.DefaultConfigurationName, new VersioningConfiguration
                {
                    Id = VersioningUtil.DefaultConfigurationName
                });

                var smuggler = new FileSystemSmuggler(new FileSystemSmugglerOptions()
                                                      {
                                                          ShouldDisableVersioningBundle = true
                                                      });

                await smuggler.ExecuteAsync(
                    new FileSmugglingSource(export),
                    new RemoteSmugglingDestination(
                        new FilesConnectionStringOptions
                        {
                            Url = store.Url, DefaultFileSystem = store.DefaultFileSystem
                        }));

                var fileCountAfterImport = (await store.AsyncFilesCommands.GetStatisticsAsync()).FileCount;

                Assert.Equal(fileCount, fileCountAfterImport);

                // after import versioning should be active
                await store.AsyncFilesCommands.UploadAsync("with-rev", StringToStream("rev"));

                using (var session = store.OpenAsyncSession())
                {
                    var revisions = await session.GetRevisionsForAsync("with-rev", 0, 10);
                    Assert.Equal(1, revisions.Length);
                }
            }
        }

        [Fact, Trait("Category", "Smuggler")]
        public async Task CanDisableVersioningDuringImport_Between()
        {
            using (var storeExport = NewStore())
            using (var storeImport = NewStore(activeBundles: "Versioning", fileSystemName: "Import", index: 1))
            {
                for (int i = 0; i < 10; i++)
                {
                    await storeExport.AsyncFilesCommands.UploadAsync("test-" + i, StringToStream("hello"));
                }

                await storeImport.AsyncFilesCommands.Configuration.SetKeyAsync(VersioningUtil.DefaultConfigurationName, new VersioningConfiguration
                {
                    Id = VersioningUtil.DefaultConfigurationName
                });

                var fileCount = (await storeExport.AsyncFilesCommands.GetStatisticsAsync()).FileCount;

                var smuggler = new FileSystemSmuggler(new FileSystemSmugglerOptions
                {
                    ShouldDisableVersioningBundle = true
                });

                await smuggler.ExecuteAsync(new RemoteSmugglingSource(new FilesConnectionStringOptions
                {
                    Url = storeExport.Url,
                    DefaultFileSystem = storeExport.DefaultFileSystem
                }), 
                new RemoteSmugglingDestination(new FilesConnectionStringOptions
                {
                    Url = storeImport.Url,
                    DefaultFileSystem = storeImport.DefaultFileSystem
                }));

                var fileCountAfterImport = (await storeImport.AsyncFilesCommands.GetStatisticsAsync()).FileCount;

                Assert.Equal(fileCount, fileCountAfterImport);

                // after import versioning should be active
                await storeImport.AsyncFilesCommands.UploadAsync("with-rev", StringToStream("rev"));

                using (var session = storeImport.OpenAsyncSession())
                {
                    var revisions = await session.GetRevisionsForAsync("with-rev", 0, 10);
                    Assert.Equal(1, revisions.Length);
                }
            }
        }
    }
}
